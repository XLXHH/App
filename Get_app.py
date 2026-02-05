import os
import uuid
import queue
import threading
import traceback
import base64
from datetime import date
from pathlib import Path
import time
import streamlit as st
import pandas as pd
import streamlit.components.v1 as components

import Get_Red as core

BASE_DIR = Path(__file__).resolve().parent

st.set_page_config(page_title="Reddit 爬虫网页", layout="wide")

LOG_MAX_LINES = 1500
AUTO_REFRESH_MS = 1200


# -------------------------
# Session state init
# -------------------------
def init_state():
    if "run_id" not in st.session_state:
        st.session_state.run_id = str(uuid.uuid4())
    if "log_q" not in st.session_state:
        st.session_state.log_q = queue.Queue()
    if "logs_by_run" not in st.session_state:
        st.session_state.logs_by_run = {}
    if "running" not in st.session_state:
        st.session_state.running = False
    if "rt" not in st.session_state:
        st.session_state.rt = None
    if "last_output_file" not in st.session_state:
        st.session_state.last_output_file = ""
    if "auto_refresh_enabled" not in st.session_state:
        st.session_state.auto_refresh_enabled = True

    # ✅ 自动下载：确保每个 run_id 只触发一次
    if "auto_download_enabled" not in st.session_state:
        st.session_state.auto_download_enabled = True
    if "auto_download_done_for_run" not in st.session_state:
        st.session_state.auto_download_done_for_run = ""

    if "runtime_state" not in st.session_state:
        st.session_state.runtime_state = {}



init_state()


# -------------------------
# Queue protocol
# -------------------------
# 我们统一往队列里塞两类消息：
# 1) ("log", run_id, "text...")
# 2) ("result", run_id, {"out_path": "...", "status": "done"})
def q_put(log_q: queue.Queue, item):
    try:
        log_q.put_nowait(item)
    except Exception:
        pass


def drain_queue():
    """只在主线程调用：把队列消息分发到 session_state"""
    changed = False
    while True:
        try:
            msg = st.session_state.log_q.get_nowait()
        except queue.Empty:
            break

        if not isinstance(msg, tuple) or len(msg) < 3:
            continue

        mtype, rid, payload = msg[0], msg[1], msg[2]

        if mtype == "log":
            # 运行态状态
            if isinstance(payload, dict) and payload.get("type") == "state":
                st.session_state.runtime_state = payload["state"]
                changed = True
                continue

            # 普通刷屏日志（程序员用）
            st.session_state.logs_by_run.setdefault(rid, []).append(str(payload))

            if len(st.session_state.logs_by_run[rid]) > LOG_MAX_LINES:
                st.session_state.logs_by_run[rid] = st.session_state.logs_by_run[rid][-LOG_MAX_LINES:]
            changed = True

        elif mtype == "result":
            # 只有当结果属于“当前 run_id”时才更新 UI
            if rid == st.session_state.run_id:
                out_path = (payload or {}).get("out_path", "") if isinstance(payload, dict) else ""
                status = (payload or {}).get("status", "") if isinstance(payload, dict) else ""
                if out_path:
                    st.session_state.last_output_file = out_path
                # 任务结束，解除 running
                st.session_state.running = False
                auto_trigger_download_once(out_path)
                # 写一条 UI 日志
                st.session_state.logs_by_run.setdefault(rid, []).append(f"[UI] 任务结束 status={status}")
                changed = True

    return changed


def get_logs(rid: str):
    return st.session_state.logs_by_run.get(rid, [])


def build_keyword_groups_from_table(df: pd.DataFrame, allow_space_keyword: bool) -> dict:
    out = {}
    if df is None or df.empty:
        return out

    for _, row in df.iterrows():
        g = str(row.get("group", "")).strip()
        kws_str = row.get("keywords", "")

        if not g:
            continue
        if kws_str is None:
            continue

        kws_str = str(kws_str)
        if kws_str == "":
            continue

        raw_list = kws_str.replace("，", ",").replace("\n", ",").split(",")

        kws = []
        for k in raw_list:
            s = "" if k is None else str(k)
            # 纯空白（包括空字符串、空格、tab）：
            if s.strip() == "":
                # 默认不允许空格关键词；只有开关开启时才保留为单空格
                if allow_space_keyword:
                    kws.append(" ")
                else:
                    continue
            else:
                kws.append(s.strip())

        if len(kws) == 0:
            continue

        out[g] = kws

    return out



def try_auto_refresh():
    if not (st.session_state.running and st.session_state.auto_refresh_enabled):
        return
    try:
        from streamlit_autorefresh import st_autorefresh  # type: ignore
        st_autorefresh(interval=AUTO_REFRESH_MS, key="log_autorefresh")
        return
    except Exception:
        pass
    try:
        getattr(st, "autorefresh")(interval=AUTO_REFRESH_MS, key="log_autorefresh")
    except Exception:
        pass


def render_log_panel(lines: list[str], height_px: int = 420):
    def esc(s: str) -> str:
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    log_html = "<br>".join(esc(x) for x in lines)
    st.markdown(
        f"""
        <div style="
            height:{height_px}px;
            overflow-y:auto;
            padding:10px;
            border:1px solid #ddd;
            background-color:#fafafa;
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono','Courier New', monospace;
            font-size: 13px;
            line-height: 1.45;
        ">{log_html}</div>
        """,
        unsafe_allow_html=True
    )


# -------------------------
# Control actions
# -------------------------
def start_crawl(cfg: dict):
    if st.session_state.running:
        return

    # 主线程捕获：run_id + queue 引用
    rid = st.session_state.run_id
    q = st.session_state.log_q

    st.session_state.logs_by_run.setdefault(rid, [])
    st.session_state.running = True
    st.session_state.last_output_file = ""

    # ✅ 新任务开始：重置“本次 run 是否已自动下载”
    st.session_state.auto_download_done_for_run = ""

    # logger_func：线程里只碰 queue，不碰 session_state
    def logger_func(msg: str):
        q_put(q, ("log", rid, msg))

    rt = core.CrawlerRuntime(cfg=cfg, log_q=logger_func)
    rt.pause_event.clear()
    rt.stop_event.clear()

    st.session_state.rt = rt

    q_put(q, ("log", rid, f"[UI] 开始任务 run_id={rid}"))
    q_put(q, ("log", rid, f"[UI] 输出目录：{cfg.get('output_dir')}"))

    def runner(local_run_id: str, local_rt, local_q: queue.Queue):
        # ✅ 后台线程：严禁访问 st.session_state
        try:
            out = core.run_crawler(local_rt)
            q_put(local_q, ("result", local_run_id, {"out_path": out or "", "status": "done"}))
        except Exception:
            q_put(local_q, ("log", local_run_id, "[FATAL] 后台线程异常："))
            q_put(local_q, ("log", local_run_id, traceback.format_exc()))
            q_put(local_q, ("result", local_run_id, {"out_path": "", "status": "error"}))

    threading.Thread(target=runner, args=(rid, rt, q), daemon=True).start()
    st.rerun()


def pause():
    rt = st.session_state.rt
    if rt:
        rt.pause_event.set()
        q_put(st.session_state.log_q, ("log", st.session_state.run_id, "[UI] 已点击暂停"))


@st.cache_data
def read_excel_cached(path: str, mtime: float, sheet_name: str):
    return pd.read_excel(path, sheet_name=sheet_name)


def resume():
    rt = st.session_state.rt
    if rt:
        rt.pause_event.clear()
        q_put(st.session_state.log_q, ("log", st.session_state.run_id, "[UI] 已点击继续"))


def stop_and_reset_ui():
    """
    Stop 永远可用：
    - 如果正在跑：给当前任务发 stop_event
    - 不管是否正在跑：都立刻释放 UI、生成新 run_id、rerun
    """
    old_rid = st.session_state.run_id
    q = st.session_state.log_q
    rt = st.session_state.rt

    if rt:
        rt.stop_event.set()
        rt.pause_event.clear()
        q_put(q, ("log", old_rid, "[UI] 已点击停止：已发送 stop_event"))
    else:
        q_put(q, ("log", old_rid, "[UI] 已点击停止：当前无运行任务，执行重置"))

    if rt:
        rt.update_state(status="stopped", end_ts=time.time())

    # ✅ 无条件重置 UI
    st.session_state.running = False
    st.session_state.last_output_file = ""
    st.session_state.rt = None

    # ✅ 新 run_id：隔离旧任务日志（并允许立即开始新任务）
    st.session_state.run_id = str(uuid.uuid4())

    st.rerun()


def auto_trigger_download_once(out_path: str):
    """
    ✅ 任务完成后自动触发浏览器下载（每个 run_id 只触发一次）
    注意：这依赖浏览器策略；文件过大可能会被拦截/失败，此时用手动下载按钮兜底。
    """
    if not st.session_state.auto_download_enabled:
        return
    if st.session_state.auto_download_done_for_run == st.session_state.run_id:
        return
    if not out_path or (not os.path.exists(out_path)):
        return

    # 读文件 -> base64 -> data url -> JS 自动点击
    try:
        with open(out_path, "rb") as f:
            xbytes = f.read()

        b64 = base64.b64encode(xbytes).decode("utf-8")
        filename = os.path.basename(out_path)
        if out_path.lower().endswith(".zip"):
            mime = "application/zip"
        else:
            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

        html = f"""
        <a id="auto_dl" download="{filename}" href="data:{mime};base64,{b64}">download</a>
        <script>
          // 轻微延迟，避免某些浏览器在渲染前点击无效
          setTimeout(function(){{
            var a = document.getElementById('auto_dl');
            if(a) a.click();
          }}, 200);
        </script>
        """
        components.html(html, height=0, width=0)

        # 标记：本次 run 已触发过
        st.session_state.auto_download_done_for_run = st.session_state.run_id
        st.session_state.logs_by_run.setdefault(st.session_state.run_id, []).append("[UI] 已触发自动下载（如被浏览器拦截，请使用手动下载按钮）")
    except Exception as e:
        st.session_state.logs_by_run.setdefault(st.session_state.run_id, []).append(f"[UI][WARN] 自动下载触发失败：{e}")



# ================= UI =================
st.title("Reddit 爬虫功能页")
try_auto_refresh()
drain_queue()
mode = st.selectbox(
    "选择爬取模式",
    options=["链接（Links）", "全站（All）", "指定社群（Subreddits）"],
    index=0,  # 默认选择 "全站"
    key="mode_selector",
    format_func=lambda x: x.split("（")[0],  # 只显示“模式”名称
    help="选择爬取模式：链接模式允许输入 Reddit 链接，其他模式则基于关键词爬取"
)
if mode.startswith("全站"):
    mode = "1"
elif mode.startswith("指定社群"):
    mode = "2"
else:
    mode = "3"
# 通过设置样式来控制选择框在一行中显示并使其美观
st.markdown("""
    <style>
        .streamlit-expanderHeader {
            display: flex;
            justify-content: space-between;
        }
    </style>
""", unsafe_allow_html=True)

if mode == "3":
    left, right = st.columns([1.08, 0.92], gap="large")
    with left:
        # 链接输入框
        link_urls_text = st.text_area("Links（多个链接可换行或用逗号分隔）", value="", height=120,
                                      disabled=st.session_state.running)
        st.markdown("### 日志")
        render_log_panel(get_logs(st.session_state.run_id), height_px=420)

        c1, c2 = st.columns(2)
        with c1:
            if st.button("刷新页面", width="stretch"):
                st.rerun()
        with c2:
            if st.button("清空当前日志", width="stretch", disabled=st.session_state.running):
                st.session_state.logs_by_run[st.session_state.run_id] = []
    with right:

        # 控制按钮并排显示
        r1, r2, r3, r4 = st.columns([1, 1, 1, 1])
        with r1:
            if st.button("开始爬取", type="primary", width="stretch", disabled=st.session_state.running):
                if not link_urls_text.strip():
                    st.error("链接模式必须填入至少 1 个帖子 URL。")
                    st.stop()

                rid = st.session_state.run_id
                output_dir = BASE_DIR / f"outputs_{rid}"
                output_dir.mkdir(parents=True, exist_ok=True)

                cfg = {
                    "mode": "3",  # 链接模式
                    "output_dir": output_dir,
                    "link_urls": link_urls_text,
                    "copy_to_desktop": False,
                }
                start_crawl(cfg)
                st.stop()


        with r2:
            if st.button("暂停", width="stretch", disabled=not st.session_state.running):
                pause()

        with r3:
            if st.button("继续", width="stretch", disabled=not st.session_state.running):
                resume()

        with r4:
            if st.button("停止", width="stretch", disabled=False):
                stop_and_reset_ui()

        # 状态显示
        st.info(f"状态：{'运行中' if st.session_state.running else '空闲'}")
        st.caption(f"当前 run_id：{st.session_state.run_id}")

        # 任务进度
        st.subheader("⏱ 任务进度")
        s = st.session_state.runtime_state
        if s:
            now = time.time()
            elapsed = int((s.get("end_ts") or now) - s.get("start_ts", now))
            h, m, sec = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60

            active = s.get("active_keywords") or []
            active_show = ", ".join(active[:15]) + (" …" if len(active) > 15 else "")
            mode_text = "全站" if s.get("mode") == "ALL" else ("指定社群" if s.get("mode") == "SUBREDDIT" else "链接")
            st.markdown(f"""
                - 模式：**{mode_text}**
                - 已处理关键词组：**{s['processed_groups']} / {s['total_groups']}**
                - 当前并发关键词：**{active_show or '—'}**
                - 已抓取帖子数：**{s['posts_fetched']:,}**
                - 已运行时间：**{h:02d}:{m:02d}:{sec:02d}**
                - 当前状态：**{s['status']}**
                """)

            if s["total_groups"] > 0:
                st.progress(s["processed_groups"] / s["total_groups"])

        if s and s.get("status") == "finished":
            total = int(s["end_ts"] - s["start_ts"])
            h, m, sec = total // 3600, (total % 3600) // 60, total % 60

            st.subheader("📊 本次结果概览")
            st.markdown(f"""
                - 覆盖关键词组：**{s['total_groups']}**
                - 帖子数：**{s['posts_saved']:,}**
                - 评论数：**{s['comments_saved']:,}**
                - 数据时间范围：**{s['start_date']} ~ {s['end_date']}**
                - 总运行时间：**{h:02d}:{m:02d}:{sec:02d}**
                """)

        # 结果文件预览与下载
        st.markdown("### 结果文件（预览与下载）")
        out_path = st.session_state.last_output_file

        # 自动下载触发
        if out_path and os.path.exists(out_path) and (not st.session_state.running):
            auto_trigger_download_once(out_path)

        if out_path and os.path.exists(out_path):
            try:
                mtime = os.path.getmtime(out_path)

                if out_path.lower().endswith(".zip"):
                    st.caption("已生成多个关键词组结果，已自动打包为 ZIP。请下载后解压查看各组 Excel。")
                else:
                    sheet = st.selectbox("预览子表", ["merged", "posts", "comments"], index=0)
                    df = read_excel_cached(out_path, mtime, sheet)
                    st.dataframe(df.head(200), width="stretch")

                st.caption(f"输出文件：{os.path.abspath(out_path)}")

                with open(out_path, "rb") as f:
                    xbytes = f.read()
                fn = os.path.basename(out_path)
                mime = "application/zip" if out_path.lower().endswith(
                    ".zip") else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                st.download_button(
                    label="下载结果文件",
                    data=xbytes,
                    file_name=fn,
                    mime=mime,
                    width="stretch"
                )
            except Exception as e:
                st.error(f"读取/下载失败：{e}")
        else:
            st.caption("任务完成后这里会出现：预览 + 下载（Excel 总表）。")

if mode == "1":
    left, right = st.columns([1.08, 0.92], gap="large")
    with left:
        a1,a2=st.columns(2)
        with a1:
            # 选择排序方式
            sort_option = st.selectbox(
                "选择排序方式",
                options=["new", "relevance", "top"],
                index=0,  # 默认选择 "new"
                disabled=st.session_state.running
            )
            # 如果选择了 "new" 排序方式，显示开始日期和结束日期
            if sort_option == "new":
                b1, b2 = st.columns(2)
                with b1:
                    start_d = st.date_input("开始日期 start_date", value=date.today(),
                                            disabled=st.session_state.running,
                                            key="start_date_input")
                with b2:
                    end_d = st.date_input("结束日期 end_date", value=date.today(), disabled=st.session_state.running,
                                          key="end_date_input")
                post_count = None
            else:
                start_d = None
                end_d = None
                post_count = st.number_input("指定爬取的帖子数量", min_value=1, value=10,
                                             disabled=st.session_state.running,
                                             key="post_count_input")
        with a2:

            if sort_option == "new":
                t_option = st.selectbox(
                    "选择时间范围",
                    options=["all"],
                    index=0,  # 默认选择 "all"
                    disabled=st.session_state.running
                )
            else:
                t_option = st.selectbox(
                    "选择时间范围",
                    options=["all", "year", "month", "week", "day", "hour"],
                    index=0,  # 默认选择 "all"
                    disabled=st.session_state.running
                )
            com_down = st.selectbox(
                "是否点击Ceomments按钮抓取",
                options=["是", "否"],
                index=1,  # 默认选择 "是"
                disabled=st.session_state.running
            )



        # 如果选择了 "new" 排序方式，保留日期范围；否则设置日期为全选范围
        if sort_option != "new":
            start_d = date(2000, 1, 1)  # 设置为全选范围的开始日期
            end_d = date.today()  # 设置为今天作为结束日期

        # ✅ 替换原“同时复制一份到桌面”
        st.session_state.auto_download_enabled = st.checkbox(
            "自动下载",
            value=st.session_state.auto_download_enabled,
            disabled=st.session_state.running
        )
        # ✅ 新增：是否允许“空格关键词”爬取（仅指定社群模式可选）

        st.markdown("### 关键词组（每行一个组）")
        kg_df = st.data_editor(
            pd.DataFrame([{"group": "brand", "keywords": " "}]),
            width="stretch",
            num_rows="dynamic",
            disabled=st.session_state.running,
            column_config={
                "group": st.column_config.TextColumn("group（组名）"),
                "keywords": st.column_config.TextColumn("keywords（多个关键词逗号分隔）")
            }
        )

        max_workers = st.slider("并发线程数(同时爬关键词的数量)", 1, 8, 5, disabled=st.session_state.running)

        st.markdown("### 日志")
        render_log_panel(get_logs(st.session_state.run_id), height_px=420)

        c1, c2 = st.columns(2)
        with c1:
            if st.button("刷新页面", width="stretch"):
                st.rerun()
        with c2:
            if st.button("清空当前日志", width="stretch", disabled=st.session_state.running):
                st.session_state.logs_by_run[st.session_state.run_id] = []

    with right:
        r1, r2, r3, r4 = st.columns(4)
        with r1:
            allow_space_keyword=False
            if st.button("开始爬取", type="primary", width="stretch", disabled=st.session_state.running):

                keyword_groups = build_keyword_groups_from_table(kg_df, allow_space_keyword=allow_space_keyword)
                if not keyword_groups:
                    st.error("关键词组为空：至少填一行 group + keywords。")
                elif start_d.isoformat() > end_d.isoformat():
                    st.error("日期不合法：start_date 不能晚于 end_date。")
                else:
                    rid = st.session_state.run_id
                    output_dir = BASE_DIR / f"outputs_{rid}"
                    output_dir.mkdir(parents=True, exist_ok=True)
                    os.makedirs(output_dir, exist_ok=True)

                    keyword_groups = build_keyword_groups_from_table(kg_df, allow_space_keyword=allow_space_keyword)

                    if not keyword_groups:
                        st.error("关键词组为空：至少填一行 group + keywords。")
                        st.stop()

                    # 日期校验等你原来怎么写就怎么保留
                    cfg = {
                        "mode": mode,
                        "start_date": start_d.isoformat() if start_d else None,  # 如果选择了日期范围，传入日期
                        "end_date": end_d.isoformat() if end_d else None,  # 同上
                        "keyword_groups": keyword_groups,
                        "max_workers": int(max_workers),
                        "output_dir": output_dir,
                        "copy_to_desktop": False,
                        "sort": sort_option,
                        "post_count": post_count,
                        "t":t_option,
                        "com_down":com_down
                    }
                    start_crawl(cfg)

        with r2:
            if st.button("暂停", width="stretch", disabled=not st.session_state.running):
                pause()
        with r3:
            if st.button("继续", width="stretch", disabled=not st.session_state.running):
                resume()
        with r4:
            if st.button("停止", width="stretch", disabled=False):
                stop_and_reset_ui()

        st.info(f"状态：{'运行中' if st.session_state.running else '空闲'}")
        st.caption(f"当前 run_id：{st.session_state.run_id}")

        st.subheader("⏱ 任务进度")

        s = st.session_state.runtime_state
        if s:
            now = time.time()
            elapsed = int((s.get("end_ts") or now) - s.get("start_ts", now))
            h, m, sec = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60

            active = s.get("active_keywords") or []
            active_show = ", ".join(active[:15]) + (" …" if len(active) > 15 else "")
            mode_text = "全站" if s.get("mode") == "ALL" else ("指定社群" if s.get("mode") == "SUBREDDIT" else "链接")
            st.markdown(f"""
                - 模式：**{mode_text}**
                - 已处理关键词组：**{s['processed_groups']} / {s['total_groups']}**
                - 当前并发关键词：**{active_show or '—'}**
                - 已抓取帖子数：**{s['posts_fetched']:,}**
                - 已运行时间：**{h:02d}:{m:02d}:{sec:02d}**
                - 当前状态：**{s['status']}**
                """)

            if s["total_groups"] > 0:
                st.progress(s["processed_groups"] / s["total_groups"])
        if s and s.get("status") == "finished":
            total = int(s["end_ts"] - s["start_ts"])
            h, m, sec = total // 3600, (total % 3600) // 60, total % 60

            st.subheader("📊 本次结果概览")
            st.markdown(f"""
                - 覆盖关键词组：**{s['total_groups']}**
                - 帖子数：**{s['posts_saved']:,}**
                - 评论数：**{s['comments_saved']:,}**
                - 数据时间范围：**{s['start_date']} ~ {s['end_date']}**
                - 总运行时间：**{h:02d}:{m:02d}:{sec:02d}**
                """)

        st.markdown("### 结果文件（预览与下载）")
        out_path = st.session_state.last_output_file

        # ✅ 一旦文件存在且任务结束，就触发一次自动下载
        if out_path and os.path.exists(out_path) and (not st.session_state.running):
            auto_trigger_download_once(out_path)

        if out_path and os.path.exists(out_path):
            try:
                mtime = os.path.getmtime(out_path)

                if out_path.lower().endswith(".zip"):
                    st.caption("已生成多个关键词组结果，已自动打包为 ZIP。请下载后解压查看各组 Excel。")
                else:
                    sheet = st.selectbox("预览子表", ["merged", "posts", "comments"], index=0)
                    df = read_excel_cached(out_path, mtime, sheet)
                    st.dataframe(df.head(200), width="stretch")

                st.caption(f"输出文件：{os.path.abspath(out_path)}")

                # 手动下载兜底
                with open(out_path, "rb") as f:
                    xbytes = f.read()
                fn = os.path.basename(out_path)
                mime = "application/zip" if out_path.lower().endswith(
                    ".zip") else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                st.download_button(
                    label="下载结果文件",
                    data=xbytes,
                    file_name=fn,
                    mime=mime,
                    width="stretch"
                )
            except Exception as e:
                st.error(f"读取/下载失败：{e}")
        else:
            st.caption("任务完成后这里会出现：预览 + 下载（Excel 总表）。")

if mode == "2":
    left, right = st.columns([1.08, 0.92], gap="large")
    with left:
        subreddits = st.text_area(
            "Subreddits",
            value="",
            height=70,
            disabled=(st.session_state.running )
        )
        a1, a2 = st.columns(2)
        with a1:
            # 选择排序方式
            sort_option = st.selectbox(
                "选择排序方式",
                options=["new", "relevance", "top"],
                index=0,  # 默认选择 "new"
                disabled=st.session_state.running
            )
            # 如果选择了 "new" 排序方式，显示开始日期和结束日期
            if sort_option == "new":
                b1, b2 = st.columns(2)
                with b1:
                    start_d = st.date_input("开始日期 start_date", value=date.today(),
                                            disabled=st.session_state.running,
                                            key="start_date_input")
                with b2:
                    end_d = st.date_input("结束日期 end_date", value=date.today(), disabled=st.session_state.running,
                                          key="end_date_input")
                post_count = None
            else:
                start_d = None
                end_d = None
                post_count = st.number_input("指定爬取的帖子数量", min_value=1, value=10,
                                             disabled=st.session_state.running,
                                             key="post_count_input")
        with a2:

            if sort_option == "new":
                t_option = st.selectbox(
                    "选择时间范围",
                    options=["all"],
                    index=0,  # 默认选择 "all"
                    disabled=st.session_state.running
                )
            else:
                t_option = st.selectbox(
                    "选择时间范围",
                    options=["all", "year", "month", "week", "day", "hour"],
                    index=0,  # 默认选择 "all"
                    disabled=st.session_state.running
                )
            com_down = st.selectbox(
                "是否点击Ceomments按钮抓取",
                options=["是", "否"],
                index=1,  # 默认选择 "是"
                disabled=st.session_state.running
            )

        # 如果选择了 "new" 排序方式，保留日期范围；否则设置日期为全选范围
        if sort_option != "new":
            start_d = date(2000, 1, 1)  # 设置为全选范围的开始日期
            end_d = date.today()  # 设置为今天作为结束日期

        f1,f2=st.columns(2)
        with f1:
            # ✅ 替换原“同时复制一份到桌面”
            st.session_state.auto_download_enabled = st.checkbox(
                "自动下载",
                value=st.session_state.auto_download_enabled,
                disabled=st.session_state.running
            )
        with f2:
            # ✅ 新增：是否允许“空格关键词”爬取（仅指定社群模式可选）
            allow_space_keyword = st.checkbox(
                "指定社群爬取所有贴文(确保关键词为空)",
                value=False,
                disabled=st.session_state.running
            )

        st.markdown("### 关键词组（每行一个组）")
        kg_df = st.data_editor(
            pd.DataFrame([{"group": "brand", "keywords": " "}]),
            width="stretch",
            num_rows="dynamic",
            disabled=st.session_state.running,
            column_config={
                "group": st.column_config.TextColumn("group（组名）"),
                "keywords": st.column_config.TextColumn("keywords（多个关键词逗号分隔）")
            }
        )

        max_workers = st.slider("并发线程数(同时爬关键词的数量)", 1, 8, 5, disabled=st.session_state.running)

        st.markdown("### 日志")
        render_log_panel(get_logs(st.session_state.run_id), height_px=420)

        c1, c2 = st.columns(2)
        with c1:
            if st.button("刷新页面", width="stretch"):
                st.rerun()
        with c2:
            if st.button("清空当前日志", width="stretch", disabled=st.session_state.running):
                st.session_state.logs_by_run[st.session_state.run_id] = []

    with right:
        r1, r2, r3, r4 = st.columns(4)
        with r1:
            if st.button("开始爬取", type="primary", width="stretch", disabled=st.session_state.running):

                keyword_groups = build_keyword_groups_from_table(kg_df, allow_space_keyword=allow_space_keyword)
                if not keyword_groups:
                    st.error("关键词组为空：至少填一行 group + keywords。")
                elif start_d.isoformat() > end_d.isoformat():
                    st.error("日期不合法：start_date 不能晚于 end_date。")
                else:
                    rid = st.session_state.run_id
                    output_dir = BASE_DIR / f"outputs_{rid}"
                    output_dir.mkdir(parents=True, exist_ok=True)
                    os.makedirs(output_dir, exist_ok=True)

                    # All / Subreddits 模式：照旧校验关键词组
                    keyword_groups = build_keyword_groups_from_table(kg_df, allow_space_keyword=allow_space_keyword)

                    if not keyword_groups:
                        st.error("关键词组为空：至少填一行 group + keywords。")
                        st.stop()

                    # 日期校验等你原来怎么写就怎么保留
                    cfg = {
                        "mode": mode,
                        "subreddits": subreddits,
                        "start_date": start_d.isoformat() if start_d else None,  # 如果选择了日期范围，传入日期
                        "end_date": end_d.isoformat() if end_d else None,  # 同上
                        "keyword_groups": keyword_groups,
                        "max_workers": int(max_workers),
                        "output_dir": output_dir,
                        "copy_to_desktop": False,
                        "allow_space_keyword": bool(allow_space_keyword),
                        "sort": sort_option,
                        "post_count": post_count,
                        "t":t_option,
                        "com_down": com_down,
                    }
                    start_crawl(cfg)

        with r2:
            if st.button("暂停", width="stretch", disabled=not st.session_state.running):
                pause()
        with r3:
            if st.button("继续", width="stretch", disabled=not st.session_state.running):
                resume()
        with r4:
            if st.button("停止", width="stretch", disabled=False):
                stop_and_reset_ui()

        st.info(f"状态：{'运行中' if st.session_state.running else '空闲'}")
        st.caption(f"当前 run_id：{st.session_state.run_id}")

        st.subheader("⏱ 任务进度")

        s = st.session_state.runtime_state
        if s:
            now = time.time()
            elapsed = int((s.get("end_ts") or now) - s.get("start_ts", now))
            h, m, sec = elapsed // 3600, (elapsed % 3600) // 60, elapsed % 60

            active = s.get("active_keywords") or []
            active_show = ", ".join(active[:15]) + (" …" if len(active) > 15 else "")
            mode_text = "全站" if s.get("mode") == "ALL" else ("指定社群" if s.get("mode") == "SUBREDDIT" else "链接")
            st.markdown(f"""
                    - 模式：**{mode_text}**
                    - 已处理关键词组：**{s['processed_groups']} / {s['total_groups']}**
                    - 当前并发关键词：**{active_show or '—'}**
                    - 已抓取帖子数：**{s['posts_fetched']:,}**
                    - 已运行时间：**{h:02d}:{m:02d}:{sec:02d}**
                    - 当前状态：**{s['status']}**
                    """)

            if s["total_groups"] > 0:
                st.progress(s["processed_groups"] / s["total_groups"])
        if s and s.get("status") == "finished":
            total = int(s["end_ts"] - s["start_ts"])
            h, m, sec = total // 3600, (total % 3600) // 60, total % 60

            st.subheader("📊 本次结果概览")
            st.markdown(f"""
                    - 覆盖关键词组：**{s['total_groups']}**
                    - 帖子数：**{s['posts_saved']:,}**
                    - 评论数：**{s['comments_saved']:,}**
                    - 数据时间范围：**{s['start_date']} ~ {s['end_date']}**
                    - 总运行时间：**{h:02d}:{m:02d}:{sec:02d}**
                    """)

        st.markdown("### 结果文件（预览与下载）")
        out_path = st.session_state.last_output_file

        # ✅ 一旦文件存在且任务结束，就触发一次自动下载
        if out_path and os.path.exists(out_path) and (not st.session_state.running):
            auto_trigger_download_once(out_path)

        if out_path and os.path.exists(out_path):
            try:
                mtime = os.path.getmtime(out_path)

                if out_path.lower().endswith(".zip"):
                    st.caption("已生成多个关键词组结果，已自动打包为 ZIP。请下载后解压查看各组 Excel。")
                else:
                    sheet = st.selectbox("预览子表", ["merged", "posts", "comments"], index=0)
                    df = read_excel_cached(out_path, mtime, sheet)
                    st.dataframe(df.head(200), width="stretch")

                st.caption(f"输出文件：{os.path.abspath(out_path)}")

                # 手动下载兜底
                with open(out_path, "rb") as f:
                    xbytes = f.read()
                fn = os.path.basename(out_path)
                mime = "application/zip" if out_path.lower().endswith(
                    ".zip") else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                st.download_button(
                    label="下载结果文件",
                    data=xbytes,
                    file_name=fn,
                    mime=mime,
                    width="stretch"
                )
            except Exception as e:
                st.error(f"读取/下载失败：{e}")
        else:
            st.caption("任务完成后这里会出现：预览 + 下载（Excel 总表）。")
