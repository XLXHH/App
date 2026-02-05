import os
import time
import json
import csv
import html
import random
import shutil
import threading
import traceback
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlencode, urljoin
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# =========================
# 固定代理（只在代码里维护）
# =========================
PROXY_RAW_LIST = [
     "geo.iproyal.com:12321:6YtHpQvFuwTRIGnK:cYHLN1rIbbutV2fU_country-de_session-0ST7md3m_lifetime-3m",
    "geo.iproyal.com:12321:6YtHpQvFuwTRIGnK:cYHLN1rIbbutV2fU_country-de_session-dn0VJTiS_lifetime-3m",
    "geo.iproyal.com:12321:6YtHpQvFuwTRIGnK:cYHLN1rIbbutV2fU_country-de_session-85sH0jni_lifetime-3m",
    "geo.iproyal.com:12321:6YtHpQvFuwTRIGnK:cYHLN1rIbbutV2fU_country-de_session-YU5yOjNI_lifetime-3m",
    "geo.iproyal.com:12321:6YtHpQvFuwTRIGnK:cYHLN1rIbbutV2fU_country-de_session-CFzRBnNz_lifetime-3m",
    "geo.iproyal.com:12321:6YtHpQvFuwTRIGnK:cYHLN1rIbbutV2fU_country-de_session-N4WS6maZ_lifetime-3m",
    "geo.iproyal.com:12321:6YtHpQvFuwTRIGnK:cYHLN1rIbbutV2fU_country-de_session-8HWrb3p8_lifetime-3m",
    "geo.iproyal.com:12321:6YtHpQvFuwTRIGnK:cYHLN1rIbbutV2fU_country-de_session-ocJdGmxa_lifetime-3m",
    "geo.iproyal.com:12321:6YtHpQvFuwTRIGnK:cYHLN1rIbbutV2fU_country-de_session-zq3QgnPQ_lifetime-3m",
    "geo.iproyal.com:12321:6YtHpQvFuwTRIGnK:cYHLN1rIbbutV2fU_country-de_session-HqCnpfvF_lifetime-3m"
]
def parse_link_urls(text: str) -> list[str]:
    if not text:
        return []
    s = str(text).replace("，", ",")
    # 允许逗号或换行
    parts = []
    for chunk in s.split("\n"):
        parts.extend(chunk.split(","))
    out = []
    for p in parts:
        u = (p or "").strip()
        if not u:
            continue
        out.append(u)
    # 去重但保序
    seen = set()
    uniq = []
    for u in out:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


def parse_reddit_post_url(url: str):
    """
    支持：
    - https://www.reddit.com/r/{sub}/comments/{post_id}/...
    - https://old.reddit.com/r/{sub}/comments/{post_id}/...
    - https://www.reddit.com/comments/{post_id}/...   (无sub时后面会从json里读sub)
    返回: (subreddit_or_None, post_id_or_None)
    """
    try:
        u = (url or "").strip()
        if not u:
            return None, None

        # 去掉 query
        u0 = u.split("?")[0]

        if "/comments/" in u0:
            # /r/{sub}/comments/{pid}/...
            if "/r/" in u0:
                sub = u0.split("/r/")[1].split("/")[0]
                pid = u0.split("/comments/")[1].split("/")[0]
                return sub, pid
            # /comments/{pid}/...
            pid = u0.split("/comments/")[1].split("/")[0]
            return None, pid

        # 兜底：可能是短链等
        return None, None
    except Exception:
        return None, None

def build_proxy_pool_from_raw_list(proxy_raw_list):
    pool = []
    for raw in proxy_raw_list:
        try:
            host, port, user, password = raw.strip().split(":", 3)
            proxy_url = f"http://{user}:{password}@{host}:{port}"
            pool.append({"http": proxy_url, "https": proxy_url})
        except Exception:
            continue
    return pool

PROXY_POOL = build_proxy_pool_from_raw_list(PROXY_RAW_LIST)

UA_POOL = [
    # ------- Desktop Chrome -------
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6023.67 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.70 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.5938.92 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",

    # ------- Desktop Firefox -------
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:118.0) Gecko/20100101 Firefox/118.0",

    # ------- Desktop Edge -------
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.61",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6023.70 Safari/537.36 Edg/119.0.2151.58",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.88 Safari/537.36 Edg/118.0.2088.76",

    # ------- Android Chrome -------
    "Mozilla/5.0 (Linux; Android 12; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6022.20 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 13; Xiaomi 12) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.60 Mobile Safari/537.36",

    # ------- iPhone Safari -------
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_7 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.7 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Mobile/15E148 Safari/604.1",

    # ------- iPad Safari -------
    "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",

    # ------- Official Reddit App -------
    "reddit/2024.10.0 (Android 12; Phone; com.reddit.frontpage)",
    "reddit/2024.9.1 (iOS 17.1; iPhone12,1; Scale/3.00)",
    "reddit/2024.8.0 (Android 13; Tablet; com.reddit.frontpage)",
    "reddit/2024.6.0 (iOS 16.7; iPhone14,5; Scale/2.00)"
]

CSV_HEADERS = [
    "post_subreddit", "post_id", "post_author", "post_author_flair","post_title", "post_body",
    "post_ups", "post_downs", "post_score", "post_created_utc", "post_url",
    "comment_id", "comment_parent_id", "comment_author","comment_author_flair", "comment_body",
    "comment_ups", "comment_downs", "comment_score", "comment_created_utc",
    "comment_url",
    "source"
]

def desktop_dir() -> Path:
    home = Path.home()
    candidates = [home / "Desktop", home / "桌面"]
    for p in candidates:
        if p.exists():
            return p
    return home

def interruptible_sleep(rt, total_seconds: float, step: float = 0.2):
    slept = 0.0
    while slept < total_seconds:
        if rt.stop_event.is_set():
            return
        time.sleep(step)
        slept += step

class CrawlerRuntime:
    def __init__(self, cfg: dict, log_q, logger_prefix: str = ""):
        self.cfg = cfg
        self.log_q = log_q
        self.logger_prefix = logger_prefix

        self.file_write_lock = threading.Lock()
        self.seen_data_lock = threading.Lock()

        self.seen_posts = set()
        self.seen_comments = set()

        self.pause_event = threading.Event()
        self.stop_event = threading.Event()

        self.consecutive_net_errors = 0
        self.consecutive_net_errors_lock = threading.Lock()
        self.NETWORK_ERROR_LIMIT = 5

        self.state_lock = threading.Lock()

        self.seen_post_ids = set()
        self._active_keywords = set()

        # =========================
        # 运行态状态（新增，不影响爬取）
        # =========================
        _mode = str(cfg.get("mode"))
        if _mode == "1":
            mode_name = "ALL"
        elif _mode == "2":
            mode_name = "SUBREDDIT"
        else:
            mode_name = "LINK"

        self.runtime_state = {
            "mode": mode_name,
            "start_date": cfg.get("start_date"),
            "end_date": cfg.get("end_date"),

            "total_groups": len(cfg.get("keyword_groups", {})),
            "processed_groups": 0,
            "hit_groups": set(),

            "posts_fetched": 0,
            "comments_fetched": 0,
            "posts_saved": 0,
            "comments_saved": 0,

            "start_ts": time.time(),
            "end_ts": None,
            "status": "running",

            "current_group_has_data": False,
            "current_keyword": None,

            "active_keywords": [],
        }

        self._emit_state()

    def log(self, msg: str):
        """
        兼容两种 logger：
        1) queue-like：有 put_nowait/put 方法
        2) callable：log_q(msg) 直接可调用
        """
        try:
            text = (self.logger_prefix + str(msg)) if self.logger_prefix else str(msg)

            if callable(self.log_q):
                self.log_q(text)
                return

            if hasattr(self.log_q, "put_nowait"):
                self.log_q.put_nowait(text)
                return
            if hasattr(self.log_q, "put"):
                self.log_q.put(text)
                return

        except Exception:
            try:
                print("[LOG_FAIL]", msg)
            except Exception:
                pass

    # =========================
    # 运行态状态更新（新增）
    # =========================
    def update_state(self, **kwargs):
        for k, v in kwargs.items():
            if k == "hit_group":
                self.runtime_state["hit_groups"].add(v)
            else:
                self.runtime_state[k] = v
        self._emit_state()

    def _emit_state(self):
        payload = {
            "type": "state",
            "state": self.serialize_state()
        }
        if callable(self.log_q):
            self.log_q(payload)
        else:
            try:
                self.log_q.put_nowait(payload)
            except Exception:
                pass

    def serialize_state(self):
        s = dict(self.runtime_state)
        s["hit_groups"] = len(s["hit_groups"])
        return s

    def wait_if_paused_or_stopped(self):
        while self.pause_event.is_set() and (not self.stop_event.is_set()):
            self.log("[系统] 暂停中…")
            time.sleep(30)

    def headers(self):
        return {"User-Agent": random.choice(UA_POOL), "Connection": "close"}

    def proxy(self):
        return random.choice(PROXY_POOL) if PROXY_POOL else None

def parse_date_to_timestamp(date_str, end_of_day=False):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    if end_of_day:
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.replace(tzinfo=timezone.utc).timestamp()

def format_ts(ts):
    if ts is None:
        return ""
    ts_float = float(ts)
    if ts_float > 1e12:
        ts_float /= 1000.0
    dt_utc = datetime.utcfromtimestamp(ts_float)
    dt_beijing = dt_utc + timedelta(hours=8)
    return dt_beijing.strftime("%Y-%m-%d")

def clean_text(s):
    if s is None:
        return ""
    return str(s).replace("\r", " ").replace("\n", " ").strip()

def is_blocked_author(author: str) -> bool:
    return author in {"AutoModerator", "timee_bot"} if author else False

def robust_get(rt: CrawlerRuntime, url, max_retries=4, timeout=8):
    if rt.stop_event.is_set():
        return None, False, None

    display_url = url if len(url) < 160 else f"{url[:150]}..."
    for attempt in range(1, max_retries + 1):
        if rt.stop_event.is_set():
            return None, False, None
        rt.wait_if_paused_or_stopped()

        interruptible_sleep(rt, random.uniform(0.05, 0.15), step=0.05)
        if rt.stop_event.is_set():
            return None, False, None

        proxy = rt.proxy()
        rt.log(f"[请求] {'Proxy' if proxy else 'Direct'} | {display_url} | attempt={attempt}/{max_retries}")

        try:
            resp = requests.get(
                url,
                headers=rt.headers(),
                proxies=proxy,
                timeout=timeout,
                verify=False
            )

            if rt.stop_event.is_set():
                return None, False, None

            if resp.status_code == 200:
                with rt.consecutive_net_errors_lock:
                    rt.consecutive_net_errors = 0
                rt.log(f"[成功] status=200 | {display_url}")
                return resp, True, 200

            if resp.status_code in (404, 410):
                rt.log(f"[失败] status={resp.status_code} | {display_url}")
                return None, False, resp.status_code

            if resp.status_code == 429:
                with rt.consecutive_net_errors_lock:
                    rt.consecutive_net_errors += 1
                    cur_err = rt.consecutive_net_errors
                rt.log(f"[限流] status=429 | 连续错误={cur_err}")

                wait_time = random.uniform(8, 15)
                rt.log(f"[冷却] {wait_time:.1f}s 后重试…")
                interruptible_sleep(rt, wait_time, step=0.2)
                continue

            with rt.consecutive_net_errors_lock:
                rt.consecutive_net_errors += 1
                cur_err = rt.consecutive_net_errors
            rt.log(f"[失败] status={resp.status_code} | 连续错误={cur_err}")

        except requests.RequestException as e:
            rt.log(f"[网络错误] {e}")
            interruptible_sleep(rt, random.uniform(0.2, 1), step=0.2)

    rt.log(f"[失败] 多次重试仍失败：{display_url}")
    return None, False, None


def fetch_post_json(rt: CrawlerRuntime, subreddit, post_id):
    # 如果 subreddit 是 None，就不加入 /r/{subreddit} 部分
    if subreddit:
        url = f"https://www.reddit.com/r/{subreddit}/comments/{post_id}.json?limit=500"
    else:
        url = f"https://www.reddit.com/comments/{post_id}.json?limit=500"  # 直接构建全站帖子 URL

    resp, ok, _ = robust_get(rt, url, max_retries=3)
    if not ok or not resp:
        return None
    try:
        data = resp.json()
        return data if isinstance(data, list) and len(data) >= 2 else None
    except Exception as e:
        rt.log(f"[解析错误] JSON 失败: {e}")
        return None


def extract_post_info_from_json(data, subreddit, post_id, original_url=None):
    try:
        p = data[0]["data"]["children"][0]["data"]
    except Exception:
        return None
    return {
        "subreddit": p.get("subreddit", subreddit),
        "post_id": post_id,
        "post_author": p.get("author"),
        "post_author_flair":p.get("author_flair_text"),
        "post_title": p.get("title"),
        "post_selftext": p.get("selftext"),
        "post_ups": p.get("ups"),
        "post_downs": p.get("downs"),
        "post_score": p.get("score"),
        "post_created_utc": p.get("created_utc"),
        "post_url": original_url or ("https://www.reddit.com" + p.get("permalink", "")),
    }

def _build_comment_url(post_url: str, comment: dict) -> str:
    """Build a stable comment URL.
    Prefer Reddit's permalink field when available.
    """
    try:
        if isinstance(comment, dict):
            permalink = comment.get("permalink")
            if permalink:
                return "https://www.reddit.com" + str(permalink)
    except Exception:
        pass

    # Fallback: best-effort. If we can't reliably construct it, return empty.
    return ""

def flatten_comments(children, out_list):
    for item in children:
        if item.get("kind") == "t1":
            data = item.get("data", {})
            out_list.append(data)
            if isinstance(data.get("replies"), dict):
                flatten_comments(data.get("replies", {}).get("data", {}).get("children", []), out_list)
        elif item.get("kind") == "Listing":
            flatten_comments(item.get("data", {}).get("children", []), out_list)


def build_search_url(keyword, subreddit=None, search_type="comments", sort="new",t="all"):
    params = {"q": keyword, "type": search_type, "sort": sort, "t": t}

    # 只有在指定了 subreddit 时才构建以 "/r/{subreddit}" 为基础的 URL
    if subreddit:
        base = f"https://www.reddit.com/r/{subreddit}/search/"
    else:
        base = "https://www.reddit.com/search/"

    return base + "?" + urlencode(params)


def parse_posts_search_page_with_cursor(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    meta = []
    for a in soup.select('a[data-testid="post-title"]'):
        try:
            href = a.get("href", "")
            full = urljoin("https://www.reddit.com", href)
            if "/comments/" not in full:
                continue
            pid = full.split("/comments/")[1].split("/")[0]
            meta.append({"post_id_short": pid, "post_url": full, "post_title": a.get_text(strip=True)})
        except Exception:
            continue

    next_url = None
    for part in soup.find_all("faceplate-partial"):
        src = part.get("src")
        if src and "cursor=" in src:
            next_url = urljoin("https://www.reddit.com", src)
            break
    return meta, next_url

def parse_comments_search_page_with_cursor(html_text):
    soup = BeautifulSoup(html_text, "html.parser")
    cards = soup.select('div[data-testid="search-sdui-comment-unit"]')
    ptc = {}
    for card in cards:
        try:
            tracker = card.find("search-telemetry-tracker")
            if not tracker:
                continue
            ctx = json.loads(html.unescape(tracker.get("data-faceplate-tracking-context")))
            p, c, s = ctx.get("post", {}), ctx.get("comment", {}), ctx.get("subreddit", {})
            pid, cid = p.get("id"), c.get("id")
            if not pid or not cid:
                continue
            pid_s = pid[3:] if pid.startswith("t3_") else pid
            cid_s = cid[3:] if cid.startswith("t1_") else cid
            sub_name = s.get("name")
            if not sub_name:
                continue
            ptc.setdefault((sub_name, pid_s), set()).add(cid_s)
        except Exception:
            continue

    next_url = None
    for part in soup.find_all("faceplate-partial"):
        src = part.get("src")
        if src and "cursor=" in src:
            next_url = urljoin("https://www.reddit.com", src)
            break
    return ptc, next_url

def prepare_row(post, comment, source):
    row = {
        "post_subreddit": clean_text(post.get("subreddit")),
        "post_id": clean_text(post.get("post_id")),
        "post_author": clean_text(post.get("post_author")),
        "post_author_flair": clean_text(post.get("author_flair_text")),
        "post_title": clean_text(post.get("post_title")),
        "post_body": clean_text(post.get("post_selftext")),
        "post_ups": post.get("post_ups"),
        "post_downs": post.get("post_downs"),
        "post_score": post.get("post_score"),
        "post_created_utc": format_ts(post.get("post_created_utc")),
        "post_url": clean_text(post.get("post_url")),
    }
    if comment:
        row.update({
            "comment_id": clean_text(comment.get("id")),
            "comment_parent_id": clean_text(comment.get("parent_id")),
            "comment_author": clean_text(comment.get("author")),
            "comment_author_flair": clean_text(comment.get("author_flair_text")),
            "comment_body": clean_text(comment.get("body")),
            "comment_ups": comment.get("ups"),
            "comment_downs": comment.get("downs"),
            "comment_score": comment.get("score"),
            "comment_created_utc": format_ts(comment.get("created_utc")),
            "comment_url": clean_text(_build_comment_url(post.get("post_url"), comment)),
        })
    else:
        for k in ["comment_id", "comment_parent_id", "comment_author","comment_author_flair", "comment_body",
                  "comment_ups", "comment_downs", "comment_score", "comment_created_utc", "comment_url"]:
            row[k] = ""
    row["source"] = source
    return {k: ("" if v is None else v) for k, v in row.items()}

def append_rows(rt: CrawlerRuntime, path: str, rows: list):
    if not rows:
        return
    df = pd.DataFrame(rows)
    with rt.file_write_lock:
        header = not os.path.exists(path)
        df.to_csv(path, mode="a", header=header, index=False, encoding="utf-8-sig")

def crawl_posts_listing_for_subreddit(rt: CrawlerRuntime, subreddit: str, start_ts: float, end_ts: float, out_posts_csv: str):
    """
    用 listing 接口抓指定 subreddit 的最新帖子（new），替代 “空格关键词 + search” 的做法。
    依旧按时间范围过滤，并保持输出格式与 posts 阶段一致（必要时也顺带抓评论）。
    """
    if not subreddit:
        rt.log("[Listing] subreddit 为空，无法使用 listing 接口")
        return

    after = None
    page = 1
    consecutive_old = 0
    post_fetched = 0
    post_count = rt.cfg.get("post_count", None)
    while not rt.stop_event.is_set():
        rt.wait_if_paused_or_stopped()
        if rt.stop_event.is_set():
            return

        params = {"limit": 100}
        if after:
            params["after"] = after

        url = f"https://www.reddit.com/r/{subreddit}/new/.json?{urlencode(params)}"
        rt.log(f"[Listing] r/{subreddit} | page={page} | after={after or 'None'}")

        resp, ok, status = robust_get(rt, url, max_retries=3, timeout=10)
        if not ok or not resp:
            rt.log(f"[Listing-New] r/{subreddit} | page={page} | RESULT=FAIL status={status}")
            break

        try:
            data = resp.json()
            children = (data or {}).get("data", {}).get("children", [])
            after = (data or {}).get("data", {}).get("after", None)
        except Exception as e:
            rt.log(f"[Listing] JSON 解析失败: {e}")
            break

        if not children:
            rt.log(f"[Listing] r/{subreddit} | page={page} | 无数据，停止")
            break

        rows = []
        saved = 0

        for item in children:
            if rt.stop_event.is_set():
                return

            d = (item or {}).get("data", {})
            pid = d.get("id")
            if not pid:
                continue

            with rt.seen_data_lock:
                if ("p", pid) in rt.seen_posts:
                    continue
                rt.seen_posts.add(("p", pid))

            p_ts = float(d.get("created_utc") or 0)

            # 时间过滤 & 提前停止（越往后越旧）
            if post_count is None:
                if p_ts < start_ts:
                    consecutive_old += 1
                    if consecutive_old >= 5:
                        rt.log(f"[Listing-New] r/{subreddit} | 连续旧帖>=5，停止翻页")
                        after = None
                        break
                    continue
                if p_ts > end_ts:
                    continue
                consecutive_old = 0
            else:
                post_fetched += 1
                # 停止条件：当抓取的评论数 >= post_count 时停止
                if post_fetched > post_count:
                    rt.log(f"[Listing] 已经抓取了 {post_count} 个帖子，停止爬取，跳转到下载文件。")
                    after = None
                    break

            # 为了和你现有逻辑完全一致：依旧用 comments/{id}.json 拉完整信息 +（可选）顺带抓评论
            post_json = fetch_post_json(rt, subreddit, pid)
            if not post_json:
                continue

            post_info = extract_post_info_from_json(post_json, subreddit, pid, original_url=("https://www.reddit.com" + d.get("permalink", "")))
            if not post_info:
                continue

            # 先写 post 行
            rows.append(prepare_row(post_info, None, f"listing_new_{subreddit}"))
            saved += 1

            # 命中计数（保持你 UI 口径一致）
            with rt.state_lock:
                rt.runtime_state["posts_fetched"] += 1
                rt.runtime_state["current_group_has_data"] = True
                cur_posts = rt.runtime_state["posts_fetched"]
            rt.update_state(posts_fetched=cur_posts)

            # 再顺带抓该贴所有评论（和 crawl_posts_for_keyword 一致）
            all_c = []
            try:
                flatten_comments(post_json[1]["data"]["children"], all_c)
            except Exception:
                all_c = []

            for c in all_c:
                if rt.stop_event.is_set():
                    return
                if is_blocked_author(c.get("author")):
                    continue
                c_ts = float(c.get("created_utc") or 0)
                if c_ts < start_ts or c_ts > end_ts:
                    continue

                rows.append(prepare_row(post_info, c, f"listing_new_comments_{subreddit}"))
                saved += 1

        if rows:
            append_rows(rt, out_posts_csv, rows)

        rt.log(f"[Listing-New] r/{subreddit} | page={page} | saved={saved} | next_after={bool(after)}")

        if not after:
            break
        page += 1

def crawl_posts_for_keyword(rt: CrawlerRuntime, keyword: str, subreddit: str, start_ts: float, end_ts: float, out_posts_csv: str):
    url = build_search_url(keyword, subreddit, "posts", sort=rt.cfg["sort"],t=rt.cfg["t"])
    page = 1
    consecutive_old = 0  # 追踪连续的旧帖子数量
    posts_fetched = 0  # 追踪已抓取的帖子数量

    # 获取 post_count，如果没有传入则默认为 None
    post_count = rt.cfg.get("post_count", None)  # 获取 post_count，如果没有提供则为 None
    if post_count is None:
        rt.log("[INFO] 未传入 post_count，爬取将基于连续旧帖数量（5）来停止")
    else:
        rt.log(f"[INFO] 传入 post_count={post_count}，爬取将基于该数量来停止")

    while url and (not rt.stop_event.is_set()):
        rt.wait_if_paused_or_stopped()
        if rt.stop_event.is_set():
            rt.log("[系统] 收到停止信号，终止 Posts 爬取")
            return

        rt.log(f"[Posts] keyword={keyword} | page={page} | url={url}")
        resp, ok, status = robust_get(rt, url)
        if not ok or not resp:
            rt.log(f"[Posts] keyword={keyword} | page={page} | RESULT=FAIL status={status}")
            break

        metas, next_url = parse_posts_search_page_with_cursor(resp.text)

        new_count, saved = 0, 0
        rows = []

        for m in metas:
            if rt.stop_event.is_set():
                rt.log("[系统] 收到停止信号，退出当前 posts 循环")
                return

            pid = m["post_id_short"]
            with rt.seen_data_lock:
                if ("p", pid) in rt.seen_posts:
                    continue
                rt.seen_posts.add(("p", pid))
            new_count += 1

            # 1. 如果传入了 post_count，检查是否达到指定数量，停止爬取
            if post_count is not None and posts_fetched >= post_count:
                rt.log(f"[Posts] 已经抓取了 {post_count} 个帖子，停止爬取，跳转到下载文件。")
                url = None  # 设置 URL 为 None 停止继续翻页
                break

            # 2. 如果没有传入 post_count，检查连续旧帖数量，达到 5 时停止爬取
            if post_count is None and consecutive_old >= 5:
                rt.log(f"[Posts] 连续抓取了 5 个旧帖子，停止爬取，跳转到下载文件。")
                url = None  # 设置 URL 为 None 停止继续翻页
                break

            # 继续处理帖子信息
            guessed_sub = subreddit
            data = fetch_post_json(rt, guessed_sub, pid)
            if not data:
                continue

            post_info = extract_post_info_from_json(data, guessed_sub, pid, m.get("post_url"))
            if not post_info:
                continue

            p_ts = float(post_info.get("post_created_utc") or 0)
            if p_ts < start_ts:
                consecutive_old += 1
                if consecutive_old >= 5:
                    rt.log(f"[Posts] 连续抓取了 5 个旧帖子，停止翻页，跳转到下载文件。")
                    url = None  # 停止翻页
                    break
                continue
            if p_ts > end_ts:
                continue
            consecutive_old = 0

            # 先写 post 行
            rows.append(prepare_row(post_info, None, f"posts_{keyword}"))
            saved += 1

            # posts 计数（保持你原口径）
            posts_fetched += 1
            with rt.state_lock:
                rt.runtime_state["posts_fetched"] = posts_fetched
                rt.runtime_state["current_group_has_data"] = True
            rt.update_state(posts_fetched=posts_fetched)

            # ✅ 新增：顺带抓该 post 下所有评论（含楼中楼）
            all_c = []
            try:
                # data 是 fetch_post_json() 返回的 [post_listing, comments_listing]
                flatten_comments(data[1]["data"]["children"], all_c)
            except Exception:
                all_c = []

            for c in all_c:
                if rt.stop_event.is_set():
                    return
                if is_blocked_author(c.get("author")):
                    continue

                # 如果你希望评论也受日期过滤，就保留这个时间判断
                c_ts = float(c.get("created_utc") or 0)
                if c_ts < start_ts or c_ts > end_ts:
                    continue

                rows.append(prepare_row(post_info, c, f"post_all_comments_{keyword}"))
                saved += 1

                # comments_fetched 计数
                with rt.state_lock:
                    rt.runtime_state["comments_fetched"] += 1
                    cur_c = rt.runtime_state["comments_fetched"]
                rt.update_state(comments_fetched=cur_c)

        rt.log(f"[Posts] keyword={keyword} | page={page} | new_posts={new_count} | saved={saved} | RESULT=SUCCESS | next={bool(next_url)}")

        if rows:
            append_rows(rt, out_posts_csv, rows)

        # 强制停止翻页：当达到指定帖子数量时，停止翻页
        # 只有在 `post_count` 不为 `None` 时才进行该判断
        if post_count is not None and posts_fetched >= post_count:
            break

        # 如果没有新的 URL 或已到达最后一页，结束循环
        if not next_url or url is None:
            break

        url = next_url
        page += 1
        rt.update_state(
            posts_fetched=rt.runtime_state["posts_fetched"],
            comments_fetched=rt.runtime_state["comments_fetched"],
        )



def crawl_comments_for_keyword(rt: CrawlerRuntime, keyword: str, subreddit: str, start_ts: float, end_ts: float, out_comments_csv: str):
    url = build_search_url(keyword, subreddit, "comments", sort=rt.cfg["sort"])
    page = 1
    consecutive_old = 0
    post_count = rt.cfg.get("post_count", None)  # 获取 post_count，如果没有提供则为 None
    comments_fetched = 0
    if post_count is None:
        rt.log("[INFO] 未传入 post_count，爬取将基于连续旧评论数量（5）来停止")
        while url and (not rt.stop_event.is_set()):
            rt.wait_if_paused_or_stopped()
            if rt.stop_event.is_set():
                rt.log("[系统] 收到停止信号，终止 Comments 爬取")
                return

            with rt.state_lock:
                pf = rt.runtime_state["posts_fetched"]
                cf = rt.runtime_state["comments_fetched"]
            rt.update_state(posts_fetched=pf, comments_fetched=cf)

            resp, ok, status = robust_get(rt, url)
            if not ok or not resp:
                rt.log(f"[Comments] keyword={keyword} | page={page} | RESULT=FAIL status={status}")
                break

            ptc, next_url = parse_comments_search_page_with_cursor(resp.text)

            new_comment_refs, saved = 0, 0
            rows = []

            for (sub_name, pid), cids in ptc.items():
                if rt.stop_event.is_set():
                    rt.log("[系统] 收到停止信号，退出当前 comments 循环")
                    return

                with rt.seen_data_lock:
                    new_cids = {cid for cid in cids if ("c", sub_name, pid, cid) not in rt.seen_comments}
                    for cid in new_cids:
                        rt.seen_comments.add(("c", sub_name, pid, cid))
                if not new_cids:
                    continue

                new_comment_refs += len(new_cids)

                data = fetch_post_json(rt, sub_name, pid)
                if not data:
                    continue
                post_info = extract_post_info_from_json(data, sub_name, pid)
                if not post_info:
                    continue

                all_c = []
                try:
                    flatten_comments(data[1]["data"]["children"], all_c)
                except Exception:
                    pass

                for c in all_c:
                    if rt.stop_event.is_set():
                        return
                    cid = c.get("id")
                    if cid not in new_cids:
                        continue
                    if is_blocked_author(c.get("author")):
                        continue

                    c_ts = float(c.get("created_utc") or 0)
                    if c_ts < start_ts:
                        consecutive_old += 1
                        if consecutive_old >= 5:
                            rt.log(f"[Comments] keyword={keyword} | 连续旧评论>=5，停止翻页")
                            url = None
                            break
                        continue
                    if c_ts > end_ts:
                        continue
                    consecutive_old = 0

                    rows.append(prepare_row(post_info, c, f"comments_{keyword}"))
                    saved += 1
                    with rt.state_lock:
                        rt.runtime_state["comments_fetched"] += 1
                        rt.runtime_state["current_group_has_data"] = True

                if url is None:
                    break

            rt.log(
                f"[Comments] keyword={keyword} | page={page} | new_comment_refs={new_comment_refs} | saved={saved} | RESULT=SUCCESS | next={bool(next_url)}")

            if rows:
                append_rows(rt, out_comments_csv, rows)

            if not next_url or url is None:
                break
            url = next_url
            page += 1

        rt.update_state(
            posts_fetched=rt.runtime_state["posts_fetched"],
            comments_fetched=rt.runtime_state["comments_fetched"],
        )

    else:  # 当传入了 post_count
        rt.log(f"[INFO] 传入 post_count={post_count}，爬取将基于该数量来停止")
        while url and (not rt.stop_event.is_set()):
            rt.wait_if_paused_or_stopped()
            if rt.stop_event.is_set():
                rt.log("[系统] 收到停止信号，终止 Comments 爬取")
                return

            with rt.state_lock:
                pf = rt.runtime_state["posts_fetched"]
                cf = rt.runtime_state["comments_fetched"]
            rt.update_state(posts_fetched=pf, comments_fetched=cf)

            resp, ok, status = robust_get(rt, url)
            if not ok or not resp:
                rt.log(f"[Comments] keyword={keyword} | page={page} | RESULT=FAIL status={status}")
                break

            ptc, next_url = parse_comments_search_page_with_cursor(resp.text)

            new_comment_refs, saved = 0, 0
            rows = []
            for (sub_name, pid), cids in ptc.items():
                if rt.stop_event.is_set():
                    rt.log("[系统] 收到停止信号，退出当前 comments 循环")
                    return

                with rt.seen_data_lock:
                    new_cids = {cid for cid in cids if ("c", sub_name, pid, cid) not in rt.seen_comments}
                    for cid in new_cids:
                        rt.seen_comments.add(("c", sub_name, pid, cid))
                if not new_cids:
                    continue

                new_comment_refs += len(new_cids)

                data = fetch_post_json(rt, sub_name, pid)
                if not data:
                    continue
                post_info = extract_post_info_from_json(data, sub_name, pid)
                if not post_info:
                    continue

                all_c = []
                try:
                    flatten_comments(data[1]["data"]["children"], all_c)
                except Exception:
                    pass

                for c in all_c:
                    if rt.stop_event.is_set():
                        return
                    cid = c.get("id")
                    if cid not in new_cids:
                        continue
                    if is_blocked_author(c.get("author")):
                        continue

                    rows.append(prepare_row(post_info, c, f"comments_{keyword}"))
                    saved += 1
                    comments_fetched+=1
                    # 停止条件：当抓取的评论数 >= post_count 时停止
                    if comments_fetched >= post_count:
                        rt.log(f"[Comments] 已经抓取了 {post_count} 个评论，停止爬取，跳转到下载文件。")
                        url = None
                        break
                    with rt.state_lock:
                        rt.runtime_state["comments_fetched"] += 1
                        rt.runtime_state["current_group_has_data"] = True

                if url is None:
                    break

            rt.log(
                f"[Comments] keyword={keyword} | page={page} | new_comment_refs={new_comment_refs} | saved={saved} | RESULT=SUCCESS | next={bool(next_url)}")

            if rows:
                append_rows(rt, out_comments_csv, rows)

            if not next_url or url is None:
                break
            url = next_url
            page += 1

        rt.update_state(
            posts_fetched=rt.runtime_state["posts_fetched"],
            comments_fetched=rt.runtime_state["comments_fetched"],
        )

def finalize_outputs(rt: CrawlerRuntime, file_prefix: str, output_dir: str, copy_to_desktop: bool = False):
    out_posts = os.path.join(output_dir, f"{file_prefix}_raw_posts.csv")
    out_comments = os.path.join(output_dir, f"{file_prefix}_raw_comments.csv")

    # ✅ 磁盘最终只保留这一个
    xlsx_path = os.path.join(output_dir, f"{file_prefix}.xlsx")

    df_p = pd.read_csv(out_posts) if os.path.exists(out_posts) else pd.DataFrame()
    df_c = pd.read_csv(out_comments) if os.path.exists(out_comments) else pd.DataFrame()

    # ===== 新增：把 df_p 拆成 post 行 和 comment 行（因为 posts 阶段已经会顺便抓评论） =====
    df_p_posts = pd.DataFrame()
    df_p_comments = pd.DataFrame()

    if not df_p.empty:
        if "comment_id" not in df_p.columns:
            df_p["comment_id"] = ""
        mask_is_comment = df_p["comment_id"].notna() & (df_p["comment_id"].astype(str).str.strip() != "")
        df_p_comments = df_p[mask_is_comment].copy()
        df_p_posts = df_p[~mask_is_comment].copy()
    else:
        df_p_posts = df_p
        df_p_comments = pd.DataFrame()

    body = df_p_posts.get("post_body")
    title = df_p_posts.get("post_title")

    if "post_body" not in df_p_posts.columns:
        df_p_posts["post_body"] = ""
        body = df_p_posts["post_body"]
    if "post_title" not in df_p_posts.columns:
        df_p_posts["post_title"] = ""
        title = df_p_posts["post_title"]

    #mask_empty_body = body.isna() | (body.astype(str).str.strip() == "")
    #df_p_posts.loc[mask_empty_body, "post_body"] = title.astype(str)

    if "post_id" in df_p_posts.columns:
        before = len(df_p_posts)
        df_p_posts = df_p_posts.drop_duplicates(subset=["post_id"])
        rt.log(f"[去重] posts: {before} -> {len(df_p_posts)} （按 post_id；body空则用title填充）")
    else:
        rt.log("[提示] posts 缺少 post_id 列，跳过去重（按 post_id）")

    # comments：按 comment_body 去重
    if not df_c.empty and "comment_body" in df_c.columns:
        df_c = df_c[df_c["comment_body"].notna() & (df_c["comment_body"].astype(str).str.strip() != "")]
        before = len(df_c)
        df_c = df_c.drop_duplicates(subset=["comment_body"])
        rt.log(f"[去重] comments: {before} -> {len(df_c)} （按 comment_body）")

    # posts阶段顺便抓到的 comments：按 comment_body 去重
    if not df_p_comments.empty:
        if "comment_body" in df_p_comments.columns:
            df_p_comments = df_p_comments[
                df_p_comments["comment_body"].notna() & (df_p_comments["comment_body"].astype(str).str.strip() != "")]
            before = len(df_p_comments)
            df_p_comments = df_p_comments.drop_duplicates(subset=["comment_body"])
            rt.log(f"[去重] posts_comments: {before} -> {len(df_p_comments)} （按 comment_body）")

    out_p = pd.DataFrame()
    if not df_p_posts.empty:
        out_p["post_subreddit"] = df_p_posts.get("post_subreddit")
        out_p["username"] = df_p_posts.get("post_author")
        out_p["author_flair"] = df_p_posts.get("post_author_flair")
        out_p["id"] = df_p_posts.get("post_id")
        out_p["parent_id"] = pd.NA
        out_p["post_title"] = df_p_posts.get("post_title")
        out_p["text_score"] = df_p_posts.get("post_score")
        out_p["text_created_utc"] = df_p_posts.get("post_created_utc")
        out_p["text_url"] = df_p_posts.get("post_url")
        out_p["_post_url"] = df_p_posts.get("post_url")
        out_p["text"] = df_p_posts.get("post_body")
        out_p["type"] = "post"

    def build_out_c(df_src: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame()
        if df_src is None or df_src.empty:
            return out
        out["post_subreddit"] = df_src.get("post_subreddit")
        out["username"] = df_src.get("comment_author")
        out["author_flair"] = df_src.get("comment_author_flair")
        out["id"] = df_src.get("comment_id")
        out["parent_id"] = df_src.get("comment_parent_id")
        out["post_title"] = df_src.get("post_title")
        out["text_score"] = df_src.get("comment_score")
        out["text_created_utc"] = df_src.get("comment_created_utc")
        out["_post_url"] = df_src.get("post_url")
        # ✅ 需求：merged 中 type=comment 的 text_url 改为 comment_url
        out["text_url"] = df_src.get("comment_url")
        out["text"] = df_src.get("comment_body")
        out["type"] = "comment"
        return out

    out_c1 = build_out_c(df_p_comments)  # posts阶段顺便抓的评论
    out_c2 = build_out_c(df_c)          # comments搜索抓的评论
    out_c = pd.concat([out_c1, out_c2], ignore_index=True)

    final_df = pd.concat([out_p, out_c], ignore_index=True)

    # merged：按 id 去重（post_id / comment_id）
    if not final_df.empty and "id" in final_df.columns:
        before = len(final_df)

        # id 不能为空
        final_df = final_df[
            final_df["id"].notna() &
            (final_df["id"].astype(str).str.strip() != "")
        ]

        final_df = final_df.drop_duplicates(subset=["id"])

        # ✅ 统计 merged（去重后）里各 type 数量
        merged_posts = 0
        merged_comments = 0
        if "type" in final_df.columns:
            merged_posts = int((final_df["type"] == "post").sum())
            merged_comments = int((final_df["type"] == "comment").sum())

        # ✅ 把统计结果写回 runtime_state（给 UI 用）
        with rt.state_lock:
            rt.runtime_state["comments_saved"] = int(
                rt.runtime_state.get("comments_saved", 0)
            ) + merged_comments

        rt.log(f"[去重] merged: {before} -> {len(final_df)} （按 id）")

    # ✅ 新增：merged 子表里统计每个 post 对应的评论数（仅填充 type==post 行）
    try:
        final_df["post_comment_count"] = ""
        if (not final_df.empty) and ("type" in final_df.columns) and ("_post_url" in final_df.columns):
            comment_counts = (
                final_df[final_df["type"] == "comment"]
                .groupby("_post_url")
                .size()
            )
            mask_post = final_df["type"] == "post"
            final_df.loc[mask_post, "post_comment_count"] = (
                final_df.loc[mask_post, "_post_url"].map(comment_counts).fillna(0).astype(int)
            )
    except Exception:
        pass

    # 统计完成后，删除内部字段（避免影响你下游使用）
    if "_post_url" in final_df.columns:
        try:
            final_df = final_df.drop(columns=["_post_url"])
        except Exception:
            pass

    # ✅ 写 3 个子表：posts / comments / merged
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
        # ✅ posts 子表：直接写 raw_posts.csv 的全量内容（post + comments）
        (df_p if not df_p.empty else pd.DataFrame(columns=CSV_HEADERS)).to_excel(
            writer, sheet_name="posts", index=False
        )

        # 2) comments 子表：清洗/去重后的 df_c
        (df_c if not df_c.empty else pd.DataFrame(columns=CSV_HEADERS)).to_excel(
            writer, sheet_name="comments", index=False
        )

        # 3) 合并子表：final_df（你原来的合并逻辑不变 + 新增列）
        final_df.to_excel(writer, sheet_name="merged", index=False)

    abs_xlsx = os.path.abspath(xlsx_path)
    rt.log(f"[完成] 输出: {abs_xlsx} | sheets=3 | posts={len(df_p_posts)} | comments={len(df_c)} | merged={len(final_df)}")

    # ✅ 桌面复制：交给页面开关控制（不勾就不复制）
    if copy_to_desktop:
        try:
            desk = desktop_dir()
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            desk_out = desk / f"{Path(xlsx_path).stem}_{stamp}.xlsx"
            shutil.copy2(abs_xlsx, desk_out)
            rt.log(f"[桌面导出] 已复制到：{desk_out}")
        except Exception as e:
            rt.log(f"[桌面导出失败] {e}")
    else:
        rt.log("[桌面导出] 未勾选：跳过复制")

    # ✅ 删除临时 CSV：确保“只生成一个文件”
    try:
        if os.path.exists(out_posts):
            os.remove(out_posts)
        if os.path.exists(out_comments):
            os.remove(out_comments)
        rt.log("[清理] 已删除临时 raw_posts/raw_comments CSV（仅保留 xlsx）")
    except Exception as e:
        rt.log(f"[清理失败] {e}")

    return abs_xlsx


def run_crawler(rt: CrawlerRuntime):
    cfg = rt.cfg
    mode = str(cfg.get("mode", "1"))  # 1=全站 2=指定社群
    subreddits = str(cfg.get("subreddits", ""))
    start_date = str(cfg.get("start_date", "")).strip()  # 开始日期
    end_date = str(cfg.get("end_date", "")).strip()  # 结束日期
    keyword_groups = cfg.get("keyword_groups", {})
    max_workers = int(cfg.get("max_workers", 4))
    post_count = cfg.get("post_count")  # 获取指定爬取的帖子数量
    sort_option = cfg.get("sort", "new")  # 获取排序方式
    com_down=cfg.get("com_down", False)

    output_dir = cfg.get("output_dir") or os.getcwd()
    os.makedirs(output_dir, exist_ok=True)

    rt.log("=== Reddit Crawler ===")
    rt.log(f"模式: {'全站' if mode == '1' else '指定社群'} | 日期: {start_date} -> {end_date} | workers={max_workers}")
    rt.log(f"输出目录: {os.path.abspath(output_dir)}")

    # 如果选择了 LINK 模式 (mode == "3")，跳过日期和数量参数，直接抓取链接
    if mode == "3":
        link_text = cfg.get("link_urls", "")
        link_urls = parse_link_urls(link_text)  # 解析链接
        if not link_urls:
            rt.log("[错误] LINK 模式：未提供任何 URL（link_urls 为空）")
            return None

        output_dir = cfg.get("output_dir") or os.getcwd()
        os.makedirs(output_dir, exist_ok=True)

        file_prefix = f"Reddit_LINKS_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        out_posts_csv = os.path.join(output_dir, f"{file_prefix}_raw_posts.csv")
        out_comments_csv = os.path.join(output_dir, f"{file_prefix}_raw_comments.csv")

        # 初始化 raw csv（保持 finalize_outputs 输入不变）
        for path in [out_posts_csv, out_comments_csv]:
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                w.writeheader()

        rt.log(f"=== LINK 模式：共 {len(link_urls)} 个 URL（不做任何配置校验） ===")

        # 不过滤时间：直接抓取
        rows = []
        for i, u in enumerate(link_urls, 1):
            if rt.stop_event.is_set():
                break
            rt.wait_if_paused_or_stopped()

            sub, pid = parse_reddit_post_url(u)
            if not pid:
                rt.log(f"[LINK][跳过] 无法解析 post_id: {u}")
                continue

            if not sub:
                rt.log(f"[LINK][跳过] URL 缺少 /r/{{sub}}/ 结构：{u}")
                continue

            data = fetch_post_json(rt, sub, pid)
            if not data:
                rt.log(f"[LINK][失败] 拉取 json 失败: r/{sub} id={pid}")
                continue

            post_info = extract_post_info_from_json(data, sub, pid, original_url=u)
            if not post_info:
                rt.log(f"[LINK][失败] 解析 post_info 失败: {u}")
                continue

            # 写 post 行
            rows.append(prepare_row(post_info, None, f"link_{i}"))
            with rt.state_lock:
                rt.runtime_state["posts_fetched"] += 1
                cur_posts = rt.runtime_state["posts_fetched"]
            rt.update_state(posts_fetched=cur_posts)

            # 写 comment 行（含楼中楼）
            all_c = []
            try:
                flatten_comments(data[1]["data"]["children"], all_c)
            except Exception:
                all_c = []

            for c in all_c:
                if rt.stop_event.is_set():
                    break
                if is_blocked_author(c.get("author")):
                    continue
                rows.append(prepare_row(post_info, c, f"link_comments_{i}"))
                with rt.state_lock:
                    rt.runtime_state["comments_fetched"] += 1
                    cur_c = rt.runtime_state["comments_fetched"]
                rt.update_state(comments_fetched=cur_c)

            # 分批落盘
            if len(rows) >= 2000:
                append_rows(rt, out_posts_csv, rows)
                rows = []

            rt.log(f"[LINK] {i}/{len(link_urls)} done | post_id={pid}")

        if rows:
            append_rows(rt, out_posts_csv, rows)

        if rt.stop_event.is_set():
            rt.log("[系统] 已停止：跳过 finalize")
            return None

        out_xlsx = finalize_outputs(rt, file_prefix, output_dir,
                                    copy_to_desktop=bool(cfg.get("copy_to_desktop", False)))

        rt.update_state(
            posts_saved=int(rt.runtime_state.get("posts_fetched", 0)),
            comments_saved=int(rt.runtime_state.get("comments_fetched", 0)),
            end_ts=time.time(),
            status="finished"
        )
        rt.log("[任务结束]")
        return out_xlsx

    # 处理其他模式（mode == 1 或 mode == 2），即全站模式或指定社群模式
    if sort_option == "new":
        # 处理按日期范围爬取
        start_ts = parse_date_to_timestamp(start_date, False)
        end_ts = parse_date_to_timestamp(end_date, True)
    else:
        # 处理按帖子数量爬取，日期设置为全选范围
        start_ts = 0  # 从最早开始
        end_ts = time.time()  # 直到当前时间

    if start_ts > end_ts:
        rt.log("[错误] 日期范围不合法")
        return None

    if mode == "1":
        targets = [None]
        sub_label = "ALL"
    else:
        subs = [s.strip() for s in subreddits.replace("，", ",").split(",") if s.strip()]
        targets = subs if subs else [None]
        sub_label = "MULTI" if len(targets) > 1 else (targets[0] or "ALL")

    outputs = []

    for group, kws in (keyword_groups or {}).items():
        if rt.stop_event.is_set():
            break
        allow_space_keyword = bool(cfg.get("allow_space_keyword", False))
        allow_space = (mode != "1") and allow_space_keyword

        norm_kws = []
        for k in (kws or []):
            if k is None:
                continue
            s = str(k)
            if s.strip() == "":
                if allow_space:
                    norm_kws.append(" ")  # 统一成单空格
                else:
                    continue  # 默认：忽略空白关键词
            else:
                norm_kws.append(s.strip())

        kws = norm_kws
        if not kws:
            rt.log(f"[跳过] 关键词组 {group} 清洗后无有效关键词（可能全是空格/空项）")
            continue

        file_prefix = f"Reddit_{group}_in_{sub_label}"
        out_posts_csv = os.path.join(output_dir, f"{file_prefix}_raw_posts.csv")
        out_comments_csv = os.path.join(output_dir, f"{file_prefix}_raw_comments.csv")

        for path in [out_posts_csv, out_comments_csv]:
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                w.writeheader()

        rt.log(f"\n=== 关键词组: {group} | 前缀: {file_prefix} ===")
        rt.update_state(current_group=group)

        for sub in targets:
            if rt.stop_event.is_set():
                break
            rt.wait_if_paused_or_stopped()
            rt.log(f">>> Subreddit: {sub or 'ALL'}")

            def job(kw):
                if str(kw).strip() == "":
                    rt.log("[策略] 指定社群空格关键词：改走 listing(new) 仅抓 Posts（可顺带抓评论），跳过 search")
                    crawl_posts_listing_for_subreddit(rt, sub, start_ts, end_ts, out_posts_csv)
                    return
                if com_down=="是":
                    crawl_posts_for_keyword(rt, kw, sub, start_ts, end_ts, out_posts_csv)
                    crawl_comments_for_keyword(rt, kw, sub, start_ts, end_ts, out_comments_csv)
                else:
                    crawl_posts_for_keyword(rt, kw, sub, start_ts, end_ts, out_posts_csv)

            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                futures = {}
                for kw in kws:
                    with rt.state_lock:
                        rt._active_keywords.add(kw)
                        rt.runtime_state["active_keywords"] = sorted(rt._active_keywords)
                        rt.runtime_state["current_keyword"] = kw
                    rt._emit_state()

                    futures[ex.submit(job, kw)] = kw

                for fu in as_completed(futures):
                    kw_done = futures.get(fu)
                    try:
                        fu.result()
                    except Exception as e:
                        rt.log(f"[线程错误] kw={kw_done} | {e}")
                        rt.log(traceback.format_exc())
                    finally:
                        with rt.state_lock:
                            if kw_done in rt._active_keywords:
                                rt._active_keywords.remove(kw_done)
                            rt.runtime_state["active_keywords"] = sorted(rt._active_keywords)
                            if not rt._active_keywords:
                                rt.runtime_state["current_keyword"] = None
                        rt._emit_state()

        if rt.stop_event.is_set():
            rt.log("[系统] 已停止：跳过 finalize")
            break

        update = {
            "processed_groups": rt.runtime_state["processed_groups"] + 1,
        }
        if rt.runtime_state.get("current_group_has_data"):
            update["hit_group"] = group

        rt.update_state(**update)
        rt.runtime_state["current_group_has_data"] = False

        out_xlsx = finalize_outputs(rt, file_prefix, output_dir,
                                    copy_to_desktop=bool(cfg.get("copy_to_desktop", False)))
        if out_xlsx:
            outputs.append(out_xlsx)

    final_path = outputs[-1] if outputs else None
    if len(outputs) > 1:
        zip_path = os.path.join(output_dir, f"Reddit_outputs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip")
        import zipfile
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as z:
            for p in outputs:
                if p and os.path.exists(p):
                    z.write(p, arcname=os.path.basename(p))
        rt.log(f"[打包] 多组结果已打包：{os.path.abspath(zip_path)} | files={len(outputs)}")
        final_path = os.path.abspath(zip_path)

    with rt.state_lock:
        rt.runtime_state["output_files"] = [os.path.abspath(p) for p in outputs]
    rt._emit_state()

    rt.update_state(
        posts_saved=int(rt.runtime_state.get("posts_fetched", 0)),
        comments_saved=int(rt.runtime_state.get("comments_saved", 0)),
        end_ts=time.time(),
        status="finished"
    )

    rt.log("[任务结束]")
    return final_path

