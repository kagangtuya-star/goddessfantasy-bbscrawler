#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
成熟热度榜分析脚本（原生 .xlsx 输出）
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import zipfile
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from xml.sax.saxutils import escape


CONTROL_CHAR_RE = re.compile(r"[\x00-\x08\x0B\x0C\x0E-\x1F]")


@dataclass
class TopicRow:
    topic_id: str
    title: str
    url: str
    op_user: str
    views: int
    replies_listed: int
    non_op_posts: int
    created_at: datetime
    last_post_at_topic: datetime
    source_row: Dict[str, str]


@dataclass
class ScoredTopic:
    topic: TopicRow
    group_month: str
    cohort_window: str
    cohort_size: int
    age_days: float
    lifecycle_days: float
    replies_used: int
    replies_source: str
    e_raw: float
    i_raw: float
    x_raw: float
    s_raw: float
    e_log: float
    i_log: float
    x_log: float
    s_log: float
    p_e: float
    p_i: float
    p_x: float
    p_s: float
    score: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="生成成熟热度榜（Excel .xlsx）")
    parser.add_argument("--input", default="topics.csv", help="输入 CSV 文件路径")
    parser.add_argument("--output", default="mature_heat_ranking.xlsx", help="输出 Excel 文件路径（.xlsx）")
    parser.add_argument("--min-age-days", type=float, default=14.0, help="成熟榜最小贴龄 H（天）")
    parser.add_argument("--k-smoothing", type=float, default=100.0, help="E 指标平滑项 k")
    parser.add_argument("--min-group-size", type=int, default=20, help="分位比较最小样本，月样本不足则扩窗")
    parser.add_argument("--top-n", type=int, default=100, help="TopN 预览条数")
    parser.add_argument(
        "--sheet-mode",
        choices=("default", "year"),
        default="default",
        help="工作表模式：default=固定4个sheet；year=按发布时间年份拆分榜单sheet",
    )
    parser.add_argument(
        "--as-of",
        default="",
        help="分析基准时刻（ISO8601，如 2026-02-20T23:59:59+08:00），留空则用数据中最大 last_post_at_topic",
    )
    return parser.parse_args()


def parse_iso_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        return None


def clean_text(text: Optional[str]) -> str:
    if text is None:
        return ""
    value = CONTROL_CHAR_RE.sub("", str(text))
    return value.replace("\r\n", "\n").replace("\r", "\n")


def choose_reply_count(non_op_posts: int, replies_listed: int) -> Tuple[int, str]:
    if non_op_posts >= 0:
        return non_op_posts, "non_op_posts"
    if replies_listed >= 0:
        return replies_listed, "replies_listed"
    return 0, "fallback_zero"


def percentile_rank(sorted_values: List[float], value: float) -> float:
    n = len(sorted_values)
    if n <= 1:
        return 0.5
    lo = bisect_left(sorted_values, value)
    hi = bisect_right(sorted_values, value) - 1
    avg_rank = (lo + hi) / 2.0
    return avg_rank / (n - 1)


def month_key(dt: datetime) -> str:
    return dt.strftime("%Y-%m")


def build_cohorts(month_to_indices: Dict[str, List[int]], min_group_size: int) -> Dict[str, Dict[str, object]]:
    months = sorted(month_to_indices.keys())
    pos_map = {m: i for i, m in enumerate(months)}
    total = len(months)
    cohorts: Dict[str, Dict[str, object]] = {}

    for m in months:
        center = pos_map[m]
        selected_pos = {center}
        merged_indices = list(month_to_indices[m])
        radius = 0
        while len(merged_indices) < min_group_size and len(selected_pos) < total:
            radius += 1
            left = center - radius
            right = center + radius
            if left >= 0 and left not in selected_pos:
                selected_pos.add(left)
                merged_indices.extend(month_to_indices[months[left]])
            if len(merged_indices) >= min_group_size:
                break
            if right < total and right not in selected_pos:
                selected_pos.add(right)
                merged_indices.extend(month_to_indices[months[right]])

        low = min(selected_pos)
        high = max(selected_pos)
        window = months[low] if low == high else f"{months[low]}~{months[high]}"
        cohorts[m] = {
            "indices": merged_indices,
            "window": window,
            "size": len(merged_indices),
        }
    return cohorts


def load_topics(input_path: Path) -> Tuple[List[TopicRow], List[Dict[str, str]]]:
    required = {
        "topic_id",
        "title",
        "url",
        "views",
        "replies_listed",
        "non_op_posts",
        "created_at",
        "last_post_at_topic",
    }

    topics: List[TopicRow] = []
    excluded: List[Dict[str, str]] = []

    with input_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = set(reader.fieldnames or [])
        missing = sorted(required - headers)
        if missing:
            raise ValueError(f"输入 CSV 缺少必要字段: {', '.join(missing)}")

        for row in reader:
            topic_id = clean_text(row.get("topic_id"))
            title = clean_text(row.get("title"))
            url = clean_text(row.get("url"))
            op_user = clean_text(row.get("op_user"))

            created_at = parse_iso_datetime(row.get("created_at", ""))
            last_at = parse_iso_datetime(row.get("last_post_at_topic", ""))
            views = parse_int(row.get("views"))
            replies_listed = parse_int(row.get("replies_listed"))
            non_op_posts = parse_int(row.get("non_op_posts"))

            if created_at is None:
                excluded.append({"topic_id": topic_id, "title": title, "reason": "bad_created_at", "detail": row.get("created_at", "")})
                continue
            if last_at is None:
                excluded.append({"topic_id": topic_id, "title": title, "reason": "bad_last_post_at_topic", "detail": row.get("last_post_at_topic", "")})
                continue
            if views is None:
                excluded.append({"topic_id": topic_id, "title": title, "reason": "bad_views", "detail": row.get("views", "")})
                continue
            if replies_listed is None:
                replies_listed = -1
            if non_op_posts is None:
                non_op_posts = -1

            topics.append(
                TopicRow(
                    topic_id=topic_id,
                    title=title,
                    url=url,
                    op_user=op_user,
                    views=max(views, 0),
                    replies_listed=replies_listed,
                    non_op_posts=non_op_posts,
                    created_at=created_at,
                    last_post_at_topic=last_at,
                    source_row=row,
                )
            )
    return topics, excluded


def score_topics(
    topics: List[TopicRow],
    min_age_days: float,
    k_smoothing: float,
    min_group_size: int,
    as_of: datetime,
) -> Tuple[List[ScoredTopic], List[Dict[str, str]], Dict[str, Dict[str, object]]]:
    excluded: List[Dict[str, str]] = []
    scored: List[ScoredTopic] = []

    prelim: List[Dict[str, object]] = []
    for topic in topics:
        age_days = (as_of - topic.created_at).total_seconds() / 86400.0
        if age_days < min_age_days:
            excluded.append(
                {
                    "topic_id": topic.topic_id,
                    "title": topic.title,
                    "reason": "not_mature",
                    "detail": f"age_days={age_days:.4f}",
                }
            )
            continue
        if age_days < 0:
            excluded.append(
                {
                    "topic_id": topic.topic_id,
                    "title": topic.title,
                    "reason": "future_created_at",
                    "detail": topic.created_at.isoformat(),
                }
            )
            continue

        lifecycle_days = (topic.last_post_at_topic - topic.created_at).total_seconds() / 86400.0
        lifecycle_days = min(max(lifecycle_days, 0.0), age_days)

        replies_used, replies_source = choose_reply_count(topic.non_op_posts, topic.replies_listed)
        views = max(topic.views, 0)
        replies_used = max(replies_used, 0)

        e_raw = replies_used / (views + k_smoothing)
        i_raw = replies_used / (age_days + 1.0)
        x_raw = views / (age_days + 1.0)
        s_raw = (lifecycle_days + 1.0) / (age_days + 1.0)

        prelim.append(
            {
                "topic": topic,
                "group_month": month_key(topic.created_at),
                "age_days": age_days,
                "lifecycle_days": lifecycle_days,
                "replies_used": replies_used,
                "replies_source": replies_source,
                "e_raw": e_raw,
                "i_raw": i_raw,
                "x_raw": x_raw,
                "s_raw": s_raw,
                "e_log": math.log1p(e_raw),
                "i_log": math.log1p(i_raw),
                "x_log": math.log1p(x_raw),
                "s_log": math.log1p(s_raw),
            }
        )

    month_to_indices: Dict[str, List[int]] = defaultdict(list)
    for idx, item in enumerate(prelim):
        month_to_indices[item["group_month"]].append(idx)

    cohorts = build_cohorts(month_to_indices, min_group_size=min_group_size)

    metric_cache: Dict[Tuple[str, str], List[float]] = {}
    metrics = ("e_log", "i_log", "x_log", "s_log")
    for m, cohort in cohorts.items():
        indices = cohort["indices"]
        for metric in metrics:
            arr = sorted(float(prelim[i][metric]) for i in indices)
            metric_cache[(m, metric)] = arr

    for item in prelim:
        m = item["group_month"]
        p_e = percentile_rank(metric_cache[(m, "e_log")], float(item["e_log"]))
        p_i = percentile_rank(metric_cache[(m, "i_log")], float(item["i_log"]))
        p_x = percentile_rank(metric_cache[(m, "x_log")], float(item["x_log"]))
        p_s = percentile_rank(metric_cache[(m, "s_log")], float(item["s_log"]))
        score = 0.50 * p_e + 0.25 * p_i + 0.20 * p_x + 0.05 * p_s

        scored.append(
            ScoredTopic(
                topic=item["topic"],
                group_month=m,
                cohort_window=str(cohorts[m]["window"]),
                cohort_size=int(cohorts[m]["size"]),
                age_days=float(item["age_days"]),
                lifecycle_days=float(item["lifecycle_days"]),
                replies_used=int(item["replies_used"]),
                replies_source=str(item["replies_source"]),
                e_raw=float(item["e_raw"]),
                i_raw=float(item["i_raw"]),
                x_raw=float(item["x_raw"]),
                s_raw=float(item["s_raw"]),
                e_log=float(item["e_log"]),
                i_log=float(item["i_log"]),
                x_log=float(item["x_log"]),
                s_log=float(item["s_log"]),
                p_e=p_e,
                p_i=p_i,
                p_x=p_x,
                p_s=p_s,
                score=score,
            )
        )

    scored.sort(
        key=lambda x: (
            x.score,
            x.p_e,
            x.p_i,
            x.p_x,
            x.p_s,
            x.replies_used,
            x.topic.views,
        ),
        reverse=True,
    )
    return scored, excluded, cohorts


def column_name(col_index_1based: int) -> str:
    name = []
    idx = col_index_1based
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        name.append(chr(ord("A") + rem))
    return "".join(reversed(name))


def sanitize_sheet_name(name: str) -> str:
    cleaned = re.sub(r"[\[\]:*?/\\]", "_", clean_text(name)).strip()
    if not cleaned:
        cleaned = "Sheet"
    return cleaned[:31]


def is_number_cell(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return math.isfinite(float(value))
    return False


def cell_xml(row_idx: int, col_idx: int, value: object) -> str:
    ref = f"{column_name(col_idx)}{row_idx}"
    if value is None:
        return f'<c r="{ref}"/>'
    if is_number_cell(value):
        if isinstance(value, int):
            number = str(value)
        else:
            number = f"{float(value):.15g}"
        return f'<c r="{ref}"><v>{number}</v></c>'
    text = escape(clean_text(value).replace("\n", " "))
    return f'<c r="{ref}" t="inlineStr"><is><t xml:space="preserve">{text}</t></is></c>'


def worksheet_xml(headers: List[str], rows: List[List[object]]) -> str:
    lines = ['<?xml version="1.0" encoding="UTF-8" standalone="yes"?>']
    lines.append('<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">')
    lines.append("<sheetData>")

    header_cells = [cell_xml(1, cidx, header) for cidx, header in enumerate(headers, start=1)]
    lines.append(f'<row r="1">{"".join(header_cells)}</row>')

    for ridx, row in enumerate(rows, start=2):
        cells = [cell_xml(ridx, cidx, value) for cidx, value in enumerate(row, start=1)]
        lines.append(f'<row r="{ridx}">{"".join(cells)}</row>')

    lines.append("</sheetData>")
    lines.append("</worksheet>")
    return "".join(lines)


def write_xlsx(path: Path, sheets: List[Tuple[str, List[str], List[List[object]]]]) -> None:
    if not sheets:
        raise ValueError("没有可写入的 sheet。")

    normalized: List[Tuple[str, List[str], List[List[object]]]] = []
    used = set()
    for raw_name, headers, rows in sheets:
        base = sanitize_sheet_name(raw_name)
        name = base
        suffix = 1
        while name in used:
            suffix += 1
            tail = f"_{suffix}"
            name = (base[: max(1, 31 - len(tail))] + tail)[:31]
        used.add(name)
        normalized.append((name, headers, rows))

    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        sheet_overrides = []
        sheet_entries = []
        rel_entries = []

        for idx, (sheet_name, headers, rows) in enumerate(normalized, start=1):
            sheet_file = f"xl/worksheets/sheet{idx}.xml"
            zf.writestr(sheet_file, worksheet_xml(headers, rows))
            sheet_overrides.append(
                f'<Override PartName="/xl/worksheets/sheet{idx}.xml" '
                f'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            )
            sheet_entries.append(
                f'<sheet name="{escape(sheet_name)}" sheetId="{idx}" r:id="rId{idx}"/>'
            )
            rel_entries.append(
                f'<Relationship Id="rId{idx}" '
                f'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
                f'Target="worksheets/sheet{idx}.xml"/>'
            )

        zf.writestr(
            "[Content_Types].xml",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                '<Default Extension="xml" ContentType="application/xml"/>'
                '<Override PartName="/xl/workbook.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                '<Override PartName="/xl/styles.xml" '
                'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
                + "".join(sheet_overrides)
                + "</Types>"
            ),
        )

        zf.writestr(
            "_rels/.rels",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                '<Relationship Id="rId1" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
                'Target="xl/workbook.xml"/>'
                "</Relationships>"
            ),
        )

        zf.writestr(
            "xl/workbook.xml",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
                'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
                "<sheets>"
                + "".join(sheet_entries)
                + "</sheets>"
                "</workbook>"
            ),
        )

        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                + "".join(rel_entries)
                + '<Relationship Id="rIdStyles" '
                'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
                'Target="styles.xml"/>'
                "</Relationships>"
            ),
        )

        zf.writestr(
            "xl/styles.xml",
            (
                '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
                '<fonts count="1"><font><sz val="11"/><name val="Calibri"/><family val="2"/></font></fonts>'
                '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
                '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
                '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
                '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
                '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
                "</styleSheet>"
            ),
        )


def build_workbook_sheets(
    scored: List[ScoredTopic],
    excluded: List[Dict[str, str]],
    all_excluded: List[Dict[str, str]],
    cohorts: Dict[str, Dict[str, object]],
    args: argparse.Namespace,
    as_of: datetime,
) -> List[Tuple[str, List[str], List[List[object]]]]:
    top_n = min(args.top_n, len(scored))

    summary_headers = ["key", "value"]
    cohort_sizes = sorted(int(v["size"]) for v in cohorts.values()) if cohorts else [0]
    reason_counter = Counter([d.get("reason", "unknown") for d in (all_excluded + excluded)])
    summary_rows: List[List[object]] = [
        ["input", args.input],
        ["output", args.output],
        ["min_age_days(H)", args.min_age_days],
        ["k_smoothing", args.k_smoothing],
        ["min_group_size", args.min_group_size],
        ["sheet_mode", args.sheet_mode],
        ["as_of", as_of.isoformat()],
        ["mature_rows", len(scored)],
        ["excluded_rows", len(all_excluded) + len(excluded)],
        ["cohorts", len(cohorts)],
        ["cohort_size_min", min(cohort_sizes)],
        ["cohort_size_max", max(cohort_sizes)],
        ["excluded_reasons", ", ".join([f"{k}={v}" for k, v in sorted(reason_counter.items())]) if reason_counter else "none"],
    ]

    top_headers = [
        "rank",
        "topic_id",
        "score",
        "发布时间",
        "最后回复时间",
        "pE",
        "pI",
        "pX",
        "pS",
        "views",
        "replies_used",
        "replies_source",
        "op_user",
        "title",
        "url",
    ]
    top_rows: List[List[object]] = []
    for i, s in enumerate(scored[:top_n], start=1):
        top_rows.append(
            [
                i,
                s.topic.topic_id,
                round(s.score, 6),
                s.topic.created_at.isoformat(),
                s.topic.last_post_at_topic.isoformat(),
                round(s.p_e, 6),
                round(s.p_i, 6),
                round(s.p_x, 6),
                round(s.p_s, 6),
                s.topic.views,
                s.replies_used,
                s.replies_source,
                s.topic.op_user,
                s.topic.title,
                s.topic.url,
            ]
        )

    full_headers = [
        "rank",
        "topic_id",
        "score",
        "group_month",
        "cohort_window",
        "cohort_size",
        "pE",
        "pI",
        "pX",
        "pS",
        "E",
        "I",
        "X",
        "S",
        "views",
        "replies_used",
        "replies_source",
        "op_user",
        "replies_listed",
        "non_op_posts",
        "age_days",
        "lifecycle_days",
        "发布时间",
        "最后回复时间",
        "title",
        "url",
    ]
    full_rows: List[List[object]] = []
    for i, s in enumerate(scored, start=1):
        full_rows.append(
            [
                i,
                s.topic.topic_id,
                round(s.score, 6),
                s.group_month,
                s.cohort_window,
                s.cohort_size,
                round(s.p_e, 6),
                round(s.p_i, 6),
                round(s.p_x, 6),
                round(s.p_s, 6),
                round(s.e_raw, 8),
                round(s.i_raw, 8),
                round(s.x_raw, 8),
                round(s.s_raw, 8),
                s.topic.views,
                s.replies_used,
                s.replies_source,
                s.topic.op_user,
                s.topic.replies_listed,
                s.topic.non_op_posts,
                round(s.age_days, 4),
                round(s.lifecycle_days, 4),
                s.topic.created_at.isoformat(),
                s.topic.last_post_at_topic.isoformat(),
                s.topic.title,
                s.topic.url,
            ]
        )

    excluded_headers = ["topic_id", "title", "reason", "detail"]
    excluded_rows = [
        [d.get("topic_id", ""), d.get("title", ""), d.get("reason", ""), d.get("detail", "")]
        for d in (all_excluded + excluded)
    ][:1000]

    sheets: List[Tuple[str, List[str], List[List[object]]]] = [
        ("Summary", summary_headers, summary_rows),
        ("TopRanking", top_headers, top_rows),
    ]

    if args.sheet_mode == "year":
        year_rows: Dict[str, List[List[object]]] = defaultdict(list)
        for row in full_rows:
            # full_rows[rank, topic_id, score, group_month, ...]
            group_month = str(row[3])
            year = group_month.split("-")[0] if "-" in group_month else "Unknown"
            year_rows[year].append(row)
        for year in sorted(year_rows.keys(), reverse=True):
            sheets.append((f"Rank_{year}", full_headers, year_rows[year]))
    else:
        sheets.append(("MatureRanking", full_headers, full_rows))

    sheets.append(("Excluded", excluded_headers, excluded_rows))
    return sheets


def main() -> None:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise SystemExit(f"输入文件不存在: {input_path}")

    topics, bad_rows = load_topics(input_path)
    if not topics:
        raise SystemExit("没有可用的主题数据。")

    if args.as_of:
        as_of = parse_iso_datetime(args.as_of)
        if as_of is None:
            raise SystemExit(f"--as-of 不是合法 ISO8601 时间: {args.as_of}")
    else:
        as_of = max(t.last_post_at_topic for t in topics)

    scored, filtered_rows, cohorts = score_topics(
        topics=topics,
        min_age_days=args.min_age_days,
        k_smoothing=args.k_smoothing,
        min_group_size=args.min_group_size,
        as_of=as_of,
    )
    if not scored:
        raise SystemExit("成熟榜为空，请调小 --min-age-days 或检查输入数据。")

    sheets = build_workbook_sheets(
        scored=scored,
        excluded=filtered_rows,
        all_excluded=bad_rows,
        cohorts=cohorts,
        args=args,
        as_of=as_of,
    )
    if output_path.suffix.lower() != ".xlsx":
        print(f"[warn] 输出后缀为 {output_path.suffix}，建议使用 .xlsx。")
    write_xlsx(output_path, sheets)

    print(f"[ok] 输出完成: {output_path}")
    print(f"[ok] 成熟贴数量: {len(scored)}")
    print(f"[ok] 过滤数量: {len(bad_rows) + len(filtered_rows)}")
    print(f"[ok] as_of: {as_of.isoformat()}")


if __name__ == "__main__":
    main()
