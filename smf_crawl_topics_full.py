#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SMF 2.1.x (including 2.1.4) board/topic crawler
- Cookie-based authenticated crawling
- Crawls all topics in a board (auto pagination)
- From board list page: views, replies(list), last_post_at(list)
- From topic pages (optional):
    - op_user (author of first post)
    - created_at (time of first post)
    - non_op_posts (count of posts written by non-OP users; excludes OP anywhere)
    - unique_repliers (distinct authors excluding OP)
    - last_post_at_topic (time of last post from topic pages, if available)
    - topic_pages_fetched, topic_pages_estimated

Politeness:
- Random sleep between requests.
- Topic page fetch has a page cap by default.
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import random
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from typing import Iterable, Optional, Tuple, List, Set
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from dateutil import tz as dttz


logger = logging.getLogger("smf_crawler")


# -----------------------------
# Data model
# -----------------------------
@dataclass
class TopicRow:
    topic_id: str
    title: str
    url: str

    views: Optional[int]
    replies_listed: Optional[int]
    last_post_at_listed: Optional[str]
    last_post_by_listed: Optional[str]

    # Engagement (topic-page derived)
    op_user: Optional[str]
    created_at: Optional[str]
    non_op_posts: Optional[int]
    unique_repliers: Optional[int]
    last_post_at_topic: Optional[str]
    topic_pages_fetched: Optional[int]
    topic_pages_estimated: Optional[int]


# -----------------------------
# Utilities
# -----------------------------
def parse_cookie_header(cookie_header: str) -> dict:
    """
    Parse cookie header to dict.
    Accepts non-strict inputs copied from browser/network panels.
    """
    cookies = {}
    if not cookie_header:
        return cookies

    # Support multiline cookie text.
    raw = cookie_header.replace("\r", ";").replace("\n", ";")
    for part in raw.split(";"):
        token = part.strip()
        if not token or "=" not in token:
            continue
        name, value = token.split("=", 1)
        name = name.strip()
        value = value.strip()
        if not name:
            continue
        if len(value) >= 2 and value[0] == value[-1] == '"':
            value = value[1:-1]
        cookies[name] = value
    return cookies


def read_cookie_text(cookie_header: str, cookie_file: str) -> str:
    chunks: List[str] = []
    if cookie_file:
        with open(cookie_file, "r", encoding="utf-8") as f:
            txt = f.read().strip()
            if txt:
                chunks.append(txt)
    if cookie_header and cookie_header.strip():
        chunks.append(cookie_header.strip())
    return "; ".join(chunks)


def configure_logging(level_name: str) -> None:
    lvl = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=lvl,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )


def build_session(
    cookie_header: str,
    user_agent: str,
    smf_cookie_elle: str = "",
    use_system_proxy: bool = True,
    proxy: str = "",
    http_proxy: str = "",
    https_proxy: str = "",
) -> requests.Session:
    sess = requests.Session()
    sess.trust_env = use_system_proxy
    sess.headers.update({
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    })
    if cookie_header:
        sess.cookies.update(parse_cookie_header(cookie_header))
    if smf_cookie_elle:
        # Allow overriding just SMFCookieElle when users only provide this value.
        sess.cookies.set("SMFCookieElle", smf_cookie_elle.strip())

    proxies = {}
    if proxy:
        proxies["http"] = proxy.strip()
        proxies["https"] = proxy.strip()
    if http_proxy:
        proxies["http"] = http_proxy.strip()
    if https_proxy:
        proxies["https"] = https_proxy.strip()
    if proxies:
        sess.proxies.update(proxies)

    return sess


def polite_sleep(min_s: float, max_s: float) -> None:
    time.sleep(random.uniform(min_s, max_s))


def get_soup(session: requests.Session, url: str, timeout: float, retries: int, sleep_range: Tuple[float, float]) -> BeautifulSoup:
    last_err = None
    total_attempts = retries + 1
    for i in range(total_attempts):
        attempt_no = i + 1
        try:
            logger.debug(f"[http] GET {url} (attempt {attempt_no}/{total_attempts})")
            resp = session.get(url, timeout=timeout)
            logger.debug(f"[http] status={resp.status_code} url={url}")
            if resp.status_code in (429, 503):
                logger.warning(f"[http] throttled status={resp.status_code}, backing off: {url}")
                polite_sleep(sleep_range[0] * 2, sleep_range[1] * 2)
                continue
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            last_err = e
            if attempt_no < total_attempts:
                logger.warning(f"[http] failed (attempt {attempt_no}/{total_attempts}) {url}: {e}")
            else:
                logger.error(f"[http] failed (last attempt) {url}: {e}")
            polite_sleep(sleep_range[0], sleep_range[1])
    raise RuntimeError(f"Failed to fetch {url}: {last_err}")


def extract_int(s: str) -> Optional[int]:
    if not s:
        return None
    m = re.search(r"(\d[\d,]*)", s)
    if not m:
        return None
    return int(m.group(1).replace(",", ""))


def now_in_tz(tz_name: str) -> datetime:
    zone = dttz.gettz(tz_name)
    if zone is None:
        raise ValueError(f"Unknown timezone: {tz_name}")
    return datetime.now(tz=zone)


def parse_smf_time(text: str, tz_name: str) -> Optional[datetime]:
    """
    Parse SMF time strings like:
    - Today at 03:12:34 PM
    - Yesterday at 11:02:03 AM
    - February 20, 2026, 03:12:34 PM
    - wrapped: « on: ... »
    """
    if not text:
        return None

    zone = dttz.gettz(tz_name) or dttz.tzlocal()
    t = re.sub(r"\s+", " ", text.strip())

    # strip: « on: ... »
    m = re.search(r"on:\s*(.*?)\s*(?:»|$)", t, flags=re.IGNORECASE)
    if m:
        t = m.group(1).strip()

    # Strip common Chinese prefix from list cells.
    t = re.sub(r"^\s*于\s*", "", t)

    base = now_in_tz(tz_name)

    # Chinese relative forms: 今天 00:36:27 / 昨天 14:12:27
    m_cn_rel = re.search(r"(今天|昨天)\s*(\d{1,2}:\d{2}(?::\d{2})?)", t)
    if m_cn_rel:
        day_word = m_cn_rel.group(1)
        clock_part = m_cn_rel.group(2)
        day = base.date() if day_word == "今天" else (base - timedelta(days=1)).date()
        try:
            clock = dtparser.parse(clock_part).time()
            return datetime.combine(day, clock, tzinfo=zone)
        except Exception:
            return None

    # Chinese absolute form: 2026-02-18, 23:17:52 / 2026-02-18 23:17:52
    m_cn_abs = re.search(r"(\d{4}-\d{2}-\d{2})\s*,?\s*(\d{1,2}:\d{2}:\d{2})", t)
    if m_cn_abs:
        try:
            dt = datetime.strptime(f"{m_cn_abs.group(1)} {m_cn_abs.group(2)}", "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=zone)
        except Exception:
            return None

    m2 = re.match(r"^(Today|Yesterday)\s+at\s+(.+)$", t, flags=re.IGNORECASE)
    if m2:
        day_word = m2.group(1).lower()
        clock_part = m2.group(2).strip()
        day = base.date()
        if day_word == "yesterday":
            day = (base - timedelta(days=1)).date()
        try:
            clock = dtparser.parse(clock_part).time()
            return datetime.combine(day, clock, tzinfo=zone)
        except Exception:
            return None

    try:
        dt = dtparser.parse(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=zone)
        return dt
    except Exception:
        return None


def iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


# -----------------------------
# Board page parsing
# -----------------------------
def find_messageindex_table(soup: BeautifulSoup):
    table = soup.find("table", id="messageindex")
    if table:
        return table
    for t in soup.find_all("table", class_=re.compile(r"\btable_grid\b")):
        if t.find("a", href=re.compile(r"topic=\d+")):
            return t
    return None


def parse_topic_rows_from_topic_container(soup: BeautifulSoup, page_url: str, tz_name: str) -> List[TopicRow]:
    """
    Parse SMF 2.1.x board layout like:
    #topic_container > div.windowbg(.sticky/.locked)
    """
    container = soup.find("div", id="topic_container")
    if not container:
        return []

    rows: List[TopicRow] = []
    seen_topic_ids: Set[str] = set()

    for block in container.find_all("div", class_=re.compile(r"\bwindowbg\b")):
        a = (
            block.select_one("div.message_index_title span[id^='msg'] a[href*='topic=']")
            or block.select_one("div.message_index_title span.preview span a[href*='topic=']")
            or block.select_one("div.message_index_title a[href*='topic=']")
        )
        if not a or not a.get("href"):
            continue

        topic_url = urljoin(page_url, a["href"])
        m_tid = re.search(r"topic=(\d+)", topic_url)
        if not m_tid:
            continue
        topic_id = m_tid.group(1)
        if topic_id in seen_topic_ids:
            continue
        seen_topic_ids.add(topic_id)

        title = a.get_text(" ", strip=True)

        replies = None
        views = None
        stats_text = ""
        stats_node = block.find("div", class_=re.compile(r"\bboard_stats\b"))
        if stats_node:
            stats_text = stats_node.get_text(" ", strip=True)
            m_replies = re.search(r"(\d[\d,]*)\s*回复", stats_text)
            if m_replies:
                replies = int(m_replies.group(1).replace(",", ""))
            m_views = re.search(r"(\d[\d,]*)\s*阅读", stats_text)
            if m_views:
                views = int(m_views.group(1).replace(",", ""))

            if replies is None or views is None:
                nums = [int(v.replace(",", "")) for v in re.findall(r"\d[\d,]*", stats_text)]
                if len(nums) >= 2:
                    replies = replies if replies is not None else nums[0]
                    views = views if views is not None else nums[1]

        last_post_at = None
        last_post_by = None
        last_node = block.find("div", class_=re.compile(r"\blastpost\b"))
        if last_node:
            time_text = None
            # Usually first anchor in .lastpost is the timestamp link.
            for link in last_node.find_all("a", href=True):
                txt = link.get_text(" ", strip=True)
                if txt:
                    time_text = txt
                    break
            if not time_text:
                time_text = last_node.get_text(" ", strip=True)

            dt = parse_smf_time(time_text, tz_name)
            if dt:
                last_post_at = iso(dt)

            links = last_node.find_all("a")
            if links:
                # Last anchor is usually "由 XXX"
                last_post_by = links[-1].get_text(" ", strip=True) or None

            if not last_post_by:
                txt = last_node.get_text(" ", strip=True)
                m_by = re.search(r"由\s*(.+)$", txt)
                if m_by:
                    last_post_by = m_by.group(1).strip()

        rows.append(TopicRow(
            topic_id=topic_id,
            title=title,
            url=topic_url,
            views=views,
            replies_listed=replies,
            last_post_at_listed=last_post_at,
            last_post_by_listed=last_post_by,
            op_user=None,
            created_at=None,
            non_op_posts=None,
            unique_repliers=None,
            last_post_at_topic=None,
            topic_pages_fetched=None,
            topic_pages_estimated=None,
        ))

    return rows


def get_next_page_url(soup: BeautifulSoup, current_url: str) -> Optional[str]:
    # rel=next
    link_next = soup.find("link", rel=lambda v: v and "next" in v)
    if link_next and link_next.get("href"):
        return urljoin(current_url, link_next["href"])

    a_next = soup.find("a", rel=lambda v: v and "next" in v)
    if a_next and a_next.get("href"):
        return urljoin(current_url, a_next["href"])

    # common "next"
    for a in soup.find_all("a", href=True):
        txt = a.get_text(" ", strip=True).lower()
        if txt in ("next", "»", ">", "下一页", "下页"):
            return urljoin(current_url, a["href"])

    return None


def parse_topic_rows_from_board_page(soup: BeautifulSoup, page_url: str, tz_name: str) -> List[TopicRow]:
    # Newer Celeste-based board layout (your sample page).
    rows = parse_topic_rows_from_topic_container(soup, page_url, tz_name)
    if rows:
        return rows

    # Legacy table-based fallback.
    table = find_messageindex_table(soup)
    if not table:
        return []

    rows: List[TopicRow] = []
    for tr in table.find_all("tr"):
        tr_id = tr.get("id", "") or ""
        if not (tr_id.startswith("topic_") or tr.find("a", href=re.compile(r"topic=\d+"))):
            continue

        a = tr.find("a", href=re.compile(r"topic=\d+"))
        if not a or not a.get("href"):
            continue

        topic_url = urljoin(page_url, a["href"])
        title = a.get_text(" ", strip=True)

        # topic id
        topic_id = ""
        qs = parse_qs(urlparse(topic_url).query)
        if "topic" in qs and qs["topic"]:
            topic_id = qs["topic"][0].split(".")[0]
        else:
            m = re.search(r"topic=(\d+)", topic_url)
            if m:
                topic_id = m.group(1)

        if not topic_id:
            continue

        # replies / views
        replies = None
        views = None
        td_replies = tr.find("td", class_=re.compile(r"\breplies\b"))
        td_views = tr.find("td", class_=re.compile(r"\bviews\b"))
        if td_replies:
            replies = extract_int(td_replies.get_text(" ", strip=True))
        if td_views:
            views = extract_int(td_views.get_text(" ", strip=True))

        if replies is None or views is None:
            tds = tr.find_all("td")
            nums = []
            for td in tds:
                txt = td.get_text(" ", strip=True)
                if re.fullmatch(r"[\d,]+", txt):
                    nums.append(int(txt.replace(",", "")))
            if len(nums) >= 2:
                replies = replies if replies is not None else nums[-2]
                views = views if views is not None else nums[-1]

        # lastpost (listed)
        last_post_at = None
        last_post_by = None

        td_last = tr.find("td", class_=re.compile(r"\blastpost\b"))
        if not td_last:
            tds = tr.find_all("td")
            td_last = tds[-1] if tds else None

        if td_last:
            # time datetime preferred
            time_tag = td_last.find("time")
            if time_tag and time_tag.get("datetime"):
                try:
                    dt = dtparser.parse(time_tag["datetime"])
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=dttz.gettz(tz_name) or dttz.tzlocal())
                    last_post_at = iso(dt)
                except Exception:
                    pass

            if last_post_at is None:
                # try smalltext
                candidates = []
                for st in td_last.find_all(class_=re.compile(r"\bsmalltext\b")):
                    candidates.append(st.get_text(" ", strip=True))
                if not candidates:
                    candidates.append(td_last.get_text(" ", strip=True))
                dt = parse_smf_time(candidates[-1], tz_name) if candidates else None
                if dt:
                    last_post_at = iso(dt)

            # last post by
            txt = td_last.get_text(" ", strip=True)
            m_by = re.search(r"\bby\b\s+(.+)$", txt, flags=re.IGNORECASE)
            if m_by:
                last_post_by = m_by.group(1).strip()
            else:
                links = td_last.find_all("a")
                if links:
                    last_post_by = links[-1].get_text(" ", strip=True)

        rows.append(TopicRow(
            topic_id=str(topic_id),
            title=title,
            url=topic_url,
            views=views,
            replies_listed=replies,
            last_post_at_listed=last_post_at,
            last_post_by_listed=last_post_by,
            op_user=None,
            created_at=None,
            non_op_posts=None,
            unique_repliers=None,
            last_post_at_topic=None,
            topic_pages_fetched=None,
            topic_pages_estimated=None,
        ))

    return rows


# -----------------------------
# Topic page parsing (engagement)
# -----------------------------
MSG_ID_RE = re.compile(r"^msg_?\d+$", re.IGNORECASE)


def iter_post_blocks(soup: BeautifulSoup) -> List[BeautifulSoup]:
    """
    Return a list of post container elements for SMF 2.1 themes.

    Common patterns:
    - div.post_wrapper#msg_123
    - div#msg_123
    """
    blocks = []
    seen_msg_nums: Set[str] = set()
    # Most reliable: elements whose id matches msg_123 or msg123
    for el in soup.find_all(id=True):
        _id = str(el.get("id", ""))
        if not MSG_ID_RE.match(_id):
            continue
        msg_num = re.sub(r"\D", "", _id)
        classes = set(el.get("class") or [])
        # Skip inner content block when outer container already exists.
        if "inner" in classes and msg_num in seen_msg_nums:
            continue
        if "inner" in classes and el.find_parent("div", class_=re.compile(r"\bwindowbg\b")) is not None:
            continue
        if msg_num in seen_msg_nums:
            continue
        blocks.append(el)
        if msg_num:
            seen_msg_nums.add(msg_num)
    # fallback: some themes wrap posts differently; try class hints
    if not blocks:
        for el in soup.select("div.post_wrapper, div.windowbg[id^='msg'], div.post"):
            blocks.append(el)
    return blocks


def parse_author_from_post(block: BeautifulSoup) -> Optional[str]:
    """
    Extract author display name from a post block.
    Common:
    - h4.poster a
    - h4.poster
    - div.poster h4 a
    """
    # Prefer profile links: avoids grabbing the PM/offline icon anchor.
    for sel in (
        "div.poster h4 a[href*='action=profile']",
        "div.poster a[href*='action=profile']",
        "h4 a[href*='action=profile']",
    ):
        for node in block.select(sel):
            name = node.get_text(" ", strip=True)
            if name:
                return name

    # Generic fallback selectors
    for sel in ("h4.poster a", "h4.poster", "div.poster h4 a", "div.poster h4", "span.poster a", ".poster a"):
        for node in block.select(sel):
            name = node.get_text(" ", strip=True)
            if name:
                return name

    # fallback: look for "author" like patterns
    # (keep conservative to avoid grabbing quotes)
    return None


def parse_time_from_post(block: BeautifulSoup, tz_name: str) -> Optional[datetime]:
    # Prefer <time datetime="...">
    time_tag = block.find("time")
    if time_tag and time_tag.get("datetime"):
        try:
            dt = dtparser.parse(time_tag["datetime"])
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=dttz.gettz(tz_name) or dttz.tzlocal())
            return dt
        except Exception:
            pass

    # Common: keyinfo smalltext contains « on: ... »
    keyinfo = block.find(class_=re.compile(r"\bkeyinfo\b"))
    if keyinfo:
        st = keyinfo.find(class_=re.compile(r"\bsmalltext\b"))
        if st:
            return parse_smf_time(st.get_text(" ", strip=True), tz_name)

    # fallback: any smalltext with "on:"
    for st in block.find_all(class_=re.compile(r"\bsmalltext\b")):
        txt = st.get_text(" ", strip=True)
        if (
            re.search(r"\bon:\b", txt, flags=re.IGNORECASE)
            or re.search(r"\bToday\b|\bYesterday\b", txt, flags=re.IGNORECASE)
            or re.search(r"今天|昨天", txt)
        ):
            dt = parse_smf_time(txt, tz_name)
            if dt:
                return dt

    return None


def extract_topic_start_values(soup: BeautifulSoup, topic_id: str) -> Tuple[Set[int], Optional[int]]:
    """
    Extract pagination "start" offsets for topic pages.

    SMF usually uses:
      ...?topic=123.0
      ...?topic=123.20
      ...?topic=123.40

    Returns (starts_set, per_page_guess)
    """
    starts: Set[int] = set()
    hrefs = [a.get("href") for a in soup.find_all("a", href=True)]
    pat = re.compile(r"topic=" + re.escape(topic_id) + r"\.(\d+)", re.IGNORECASE)

    for h in hrefs:
        if not h:
            continue
        m = pat.search(h)
        if m:
            try:
                starts.add(int(m.group(1)))
            except Exception:
                pass

    starts.add(0)

    per_page = None
    if len(starts) >= 2:
        s_sorted = sorted(starts)
        diffs = [b - a for a, b in zip(s_sorted, s_sorted[1:]) if b - a > 0]
        if diffs:
            per_page = min(diffs)

    return starts, per_page


def estimate_total_pages(starts: Set[int], per_page: Optional[int]) -> Optional[int]:
    if not starts:
        return None
    max_start = max(starts)
    if max_start == 0:
        return 1
    if not per_page:
        return None
    return (max_start // per_page) + 1


def build_topic_page_url(base_topic_url: str, topic_id: str, start: int) -> str:
    """
    Build url with topic=ID.START, preserving other query parts if any.
    We do a simple replace on the 'topic=ID.xxx' part if present; else append.
    """
    if re.search(r"topic=" + re.escape(topic_id) + r"\.\d+", base_topic_url):
        return re.sub(r"(topic=" + re.escape(topic_id) + r")\.\d+", r"\1." + str(start), base_topic_url)
    # If URL has topic=ID (no .start), append .start
    if re.search(r"topic=" + re.escape(topic_id) + r"\b", base_topic_url):
        return re.sub(r"(topic=" + re.escape(topic_id) + r")\b", r"\1." + str(start), base_topic_url)
    # Fallback: return as-is (unlikely)
    return base_topic_url


def fetch_topic_engagement(
    session: requests.Session,
    topic: TopicRow,
    tz_name: str,
    timeout: float,
    retries: int,
    sleep_range: Tuple[float, float],
    topic_page_cap: int,
) -> TopicRow:
    """
    Fetch topic pages (possibly multiple) and fill:
    - op_user
    - created_at
    - non_op_posts
    - unique_repliers
    - last_post_at_topic
    - topic_pages_fetched/topic_pages_estimated
    """
    # Fetch first page
    first_soup = get_soup(session, topic.url, timeout, retries, sleep_range)

    # Parse pagination info
    starts, per_page = extract_topic_start_values(first_soup, topic.topic_id)
    est_pages = estimate_total_pages(starts, per_page)
    topic.topic_pages_estimated = est_pages

    # Decide which starts to fetch
    if per_page is None:
        # no clear per-page; only fetch first (or respect cap=1)
        start_list = [0]
    else:
        max_start = max(starts) if starts else 0
        full = list(range(0, max_start + 1, per_page))
        start_list = full

    # Apply cap
    if topic_page_cap > 0:
        start_list = start_list[:topic_page_cap]

    # Now iterate pages
    op_user = None
    created_at = None
    non_op_posts = 0
    repliers: Set[str] = set()
    last_post_dt: Optional[datetime] = None

    pages_fetched = 0

    for idx, start in enumerate(start_list):
        page_url = topic.url if start == 0 else build_topic_page_url(topic.url, topic.topic_id, start)
        soup = first_soup if start == 0 else get_soup(session, page_url, timeout, retries, sleep_range)
        pages_fetched += 1

        blocks = iter_post_blocks(soup)
        if not blocks:
            logger.warning("[topic] no post blocks topic_id=%s page_url=%s", topic.topic_id, page_url)
            continue

        for bi, block in enumerate(blocks):
            author = parse_author_from_post(block) or ""
            dt = parse_time_from_post(block, tz_name)

            # Determine OP from the first post of the first page (most reliable)
            if op_user is None and start == 0:
                # first post on first page
                if bi == 0 and author:
                    op_user = author
                    # created_at from that post
                    if dt:
                        created_at = iso(dt)

            # Update last post time
            if dt:
                if last_post_dt is None or dt > last_post_dt:
                    last_post_dt = dt

            # Count non-op posts / unique repliers (exclude OP anywhere)
            if op_user and author and author != op_user:
                non_op_posts += 1
                repliers.add(author)

        # Be polite between page fetches (except first already included in request cost)
        polite_sleep(sleep_range[0], sleep_range[1])

    topic.op_user = op_user
    topic.created_at = created_at
    topic.non_op_posts = non_op_posts if op_user else None
    topic.unique_repliers = len(repliers) if op_user else None
    topic.last_post_at_topic = iso(last_post_dt) if last_post_dt else None
    topic.topic_pages_fetched = pages_fetched

    return topic


# -----------------------------
# CSV helpers
# -----------------------------
def csv_write_header_if_needed(path: str, fieldnames: List[str]) -> None:
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            if f.read(1):
                return
    except FileNotFoundError:
        pass

    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()


def csv_append_rows(path: str, rows: Iterable[TopicRow], fieldnames: List[str]) -> None:
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        for r in rows:
            w.writerow(asdict(r))


def load_existing_topic_ids(path: str) -> Set[str]:
    ids: Set[str] = set()
    try:
        with open(path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if "topic_id" not in (reader.fieldnames or []):
                return ids
            for row in reader:
                tid = (row.get("topic_id") or "").strip()
                if tid:
                    ids.add(tid)
    except FileNotFoundError:
        pass
    return ids


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--board-url", required=True, help="Board message index url, e.g. https://host/index.php?board=123.0")
    ap.add_argument("--cookie", default="", help="Cookie header: 'a=b; c=d'")
    ap.add_argument("--cookie-file", default="", help="Path to a text file containing the cookie header")
    ap.add_argument("--smf-cookie-elle", default="", help="SMFCookieElle value only (can be raw or URL-encoded)")
    ap.add_argument("--out", default="topics.csv", help="Output CSV path")
    ap.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"),
                    help="Realtime log level")

    ap.add_argument("--tz", default="Asia/Shanghai", help="Timezone for parsing Today/Yesterday")
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--retries", type=int, default=3)
    ap.add_argument("--sleep", nargs=2, type=float, default=(1.0, 2.0), metavar=("MIN", "MAX"))

    ap.add_argument("--fetch-engagement", action="store_true",
                    help="Fetch topic pages to compute op_user/non_op_posts/unique_repliers/created_at/last_post_at_topic")
    ap.add_argument("--topic-page-cap", type=int, default=3,
                    help="Max pages to fetch per topic (0=unlimited). Default=3 for politeness")
    ap.add_argument("--max-board-pages", type=int, default=0, help="Stop after N board pages (0=all)")

    ap.add_argument("--resume", action="store_true", help="Resume: skip topic_ids already in output CSV")
    ap.add_argument("--user-agent", default="Mozilla/5.0 (compatible; SMFTopicCrawler/2.1)", help="Custom user agent")
    ap.add_argument("--proxy", default="", help="Set both HTTP/HTTPS proxy, e.g. http://127.0.0.1:7890")
    ap.add_argument("--http-proxy", default="", help="Set HTTP proxy only")
    ap.add_argument("--https-proxy", default="", help="Set HTTPS proxy only")
    ap.add_argument("--no-system-proxy", action="store_true", help="Disable system proxy (env vars)")
    args = ap.parse_args()

    configure_logging(args.log_level)
    sleep_range = (float(args.sleep[0]), float(args.sleep[1]))
    if args.cookie_file and not os.path.exists(args.cookie_file):
        raise FileNotFoundError(f"Cookie file not found: {args.cookie_file}")
    cookie_text = read_cookie_text(args.cookie, args.cookie_file)
    sess = build_session(
        cookie_header=cookie_text,
        user_agent=args.user_agent,
        smf_cookie_elle=args.smf_cookie_elle,
        use_system_proxy=not args.no_system_proxy,
        proxy=args.proxy,
        http_proxy=args.http_proxy,
        https_proxy=args.https_proxy,
    )

    out_path = os.path.abspath(args.out)
    logger.info(
        "start crawl board=%s out=%s fetch_engagement=%s resume=%s tz=%s proxy_mode=%s",
        args.board_url,
        out_path,
        args.fetch_engagement,
        args.resume,
        args.tz,
        "system+manual" if (not args.no_system_proxy and (args.proxy or args.http_proxy or args.https_proxy))
        else ("manual-only" if (args.no_system_proxy and (args.proxy or args.http_proxy or args.https_proxy))
              else ("system-only" if not args.no_system_proxy else "direct")),
    )

    fieldnames = [
        "topic_id", "title", "url",
        "views", "replies_listed", "last_post_at_listed", "last_post_by_listed",
        "op_user", "created_at", "non_op_posts", "unique_repliers", "last_post_at_topic",
        "topic_pages_fetched", "topic_pages_estimated",
    ]
    csv_write_header_if_needed(args.out, fieldnames)
    logger.info("[write] csv ready: %s", out_path)

    seen = load_existing_topic_ids(args.out) if args.resume else set()

    current_url = args.board_url
    board_pages = 0
    total_written = 0

    while current_url:
        board_pages += 1
        if args.max_board_pages and board_pages > args.max_board_pages:
            break

        logger.info("[board] fetching page=%s url=%s", board_pages, current_url)
        soup = get_soup(sess, current_url, args.timeout, args.retries, sleep_range)
        topics = parse_topic_rows_from_board_page(soup, current_url, args.tz)
        if not topics:
            logger.warning("[stop] no topics found on: %s", current_url)
            break
        logger.info("[board] parsed topics=%s page=%s", len(topics), board_pages)

        # Resume skip
        before_resume = len(topics)
        topics = [t for t in topics if t.topic_id not in seen]
        skipped = before_resume - len(topics)
        if skipped:
            logger.info("[resume] skipped existing topics=%s page=%s", skipped, board_pages)

        for i, t in enumerate(topics, 1):
            if args.fetch_engagement:
                logger.info("[topic] (%s/%s) fetching topic_id=%s", i, len(topics), t.topic_id)
                try:
                    fetch_topic_engagement(
                        session=sess,
                        topic=t,
                        tz_name=args.tz,
                        timeout=args.timeout,
                        retries=args.retries,
                        sleep_range=sleep_range,
                        topic_page_cap=args.topic_page_cap,
                    )
                    logger.info(
                        "[topic] done topic_id=%s replies_listed=%s op_user=%s created_at=%s non_op_posts=%s unique_repliers=%s pages=%s/%s",
                        t.topic_id,
                        t.replies_listed,
                        t.op_user,
                        t.created_at,
                        t.non_op_posts,
                        t.unique_repliers,
                        t.topic_pages_fetched,
                        t.topic_pages_estimated,
                    )
                    if (
                        t.replies_listed is not None
                        and t.replies_listed > 0
                        and t.non_op_posts is not None
                        and t.non_op_posts == 0
                    ):
                        logger.warning(
                            "[topic] replies_listed=%s but non_op_posts=0 topic_id=%s (可能是楼主自回，或页面结构变化)",
                            t.replies_listed,
                            t.topic_id,
                        )
                except Exception as e:
                    logger.warning("[warn] engagement failed topic_id=%s: %s", t.topic_id, e)

            # Realtime append: one topic, one write.
            csv_append_rows(args.out, [t], fieldnames)
            seen.add(t.topic_id)
            total_written += 1
            logger.info("[write] appended topic_id=%s total=%s", t.topic_id, total_written)

            # extra sleep between topics (polite)
            polite_sleep(sleep_range[0], sleep_range[1])

        logger.info("[ok] board_page=%s wrote=%s total=%s url=%s", board_pages, len(topics), total_written, current_url)

        next_url = get_next_page_url(soup, current_url)
        if next_url:
            logger.info("[board] next=%s", next_url)
        else:
            logger.info("[board] no next page found")
        current_url = next_url
        polite_sleep(sleep_range[0], sleep_range[1])

    logger.info("[done] board_pages=%s topics_written=%s output=%s", board_pages, total_written, args.out)


if __name__ == "__main__":
    main()
