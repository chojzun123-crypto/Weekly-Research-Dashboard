"""
generate_dashboard.py — 주간 투자 리서치 대시보드 생성기

두 프로젝트의 JSON 데이터를 읽어 단일 HTML 대시보드를 생성합니다.

사용법:
  python generate_dashboard.py                    # dashboard.html 생성 후 브라우저 실행
  python generate_dashboard.py --no-open          # 브라우저 실행 없이 생성만
  python generate_dashboard.py --output foo.html  # 출력 파일명 지정
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import math
import os
import re
import time
import webbrowser
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("dashboard")

# ── 상수 ─────────────────────────────────────────────────────────
# 기술분석
RSI_PERIOD = 14
BB_PERIOD = 20
EMA_LONG_PERIOD = 224
CANDLE_HISTORY_DAYS = 1250          # 약 5년 거래일
PIVOT_MIN_DISTANCE = 5
TRENDLINE_PIVOT_COUNT = 5
VOL_SURGE_RATIO = 1.5               # 평균 대비 150%
VOL_MODERATE_RATIO = 1.0
BREAKOUT_VOL_RATIO = 1.3
SIMILARITY_THRESHOLD = 0.7

# 투자의견 매핑
BUY_OPINIONS = {"BUY", "매수", "적극매수", "OVERWEIGHT", "OUTPERFORM"}
OVERWEIGHT_OPINIONS = {"Overweight", "overweight", "비중확대"}
INDUSTRY_OPINION_MAP = {
    "overweight": "Overweight", "비중확대": "Overweight",
    "neutral": "Neutral", "중립": "Neutral",
    "underweight": "Underweight", "비중축소": "Underweight",
}
TOP10_OPINION_MAP = {
    "buy": "BUY", "매수": "BUY", "적극매수": "BUY",
    "hold": "HOLD", "중립": "HOLD", "보유": "HOLD",
    "neutral": "HOLD", "시장중립": "HOLD",
    "sell": "SELL", "매도": "SELL",
    "outperform": "BUY", "overweight": "BUY",
    "underperform": "SELL", "underweight": "SELL",
    "not rated": "NR", "nr": "NR",
}
TOP10_CHANGE_MAP = {
    "신규": "신규", "변동없음": "변동없음",
    "이전대비상향": "이전대비상향", "이전대비하향": "이전대비하향",
    "상향": "이전대비상향", "하향": "이전대비하향", "유지": "변동없음",
}
MOMENTUM_KEYWORDS = [
    "서프라이즈", "서프", "최대 실적", "역대", "이동",
    "가이던스", "초과", "최고", "사상최대", "최대치",
    "기대치", "컨센서스 상회", "어닝서프라이즈", "깜짝",
]

# 스크래핑
SCRAPE_DELAY_SEC = 1.5
CLAUDE_MODEL = "claude-sonnet-4-20250514"

# HTTP 헤더
NAVER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://finance.naver.com',
}
WISEREPORT_BASE_URL = "https://comp.wisereport.co.kr/wiseReport/summary/ReportSummary.aspx"
WISEREPORT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://comp.wisereport.co.kr/",
}

# ── 경로 설정 ────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
TOP5_DIR     = SCRIPT_DIR / "output_top5"
INDUSTRY_DIR = SCRIPT_DIR / "output_industry"
_LEGACY_TOP5_DIR     = SCRIPT_DIR.parent / "wisereport_top5"    / "output"
_LEGACY_INDUSTRY_DIR = SCRIPT_DIR.parent / "wisereport_industry" / "output"
TEMPLATE     = SCRIPT_DIR / "template.html"
DEFAULT_OUT  = SCRIPT_DIR / "dashboard.html"

_DATE_RE = re.compile(r"(\d{8})")
_DATE_COL_RE = re.compile(r'^\d{4}\.\d{2}$')


# ── 네이버 금융 재무지표 스크래핑 ────────────────────────────────
_fin_cache: dict[str, dict] = {}


def _safe_float(text: str, multiplier: float = 1.0):
    try:
        cleaned = text.replace(',', '').replace('%', '').replace('배', '').replace('원', '').strip()
        if not cleaned or cleaned in ('-', 'N/A', 'n/a'):
            return None
        return round(float(cleaned) * multiplier, 2)
    except (ValueError, AttributeError):
        return None


def _fetch_naver_main(code: str, result: dict):
    url  = f'https://finance.naver.com/item/main.naver?code={code}'
    resp = requests.get(url, headers=NAVER_HEADERS, timeout=10)
    if resp.encoding and resp.encoding.lower() in ('euc-kr', 'euc_kr'):
        resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text, 'html.parser')

    id_map = {'_per': 'per', '_eps': 'eps', '_pbr': 'pbr', '_dvr': 'div_yield'}
    for em_id, key in id_map.items():
        el = soup.select_one(f'em#{em_id}')
        if el:
            result[key] = _safe_float(el.get_text(strip=True))

    h4 = soup.select_one('h4.sub_tit7')
    if h4:
        a_tag = h4.select_one('a')
        if a_tag:
            result['industry'] = a_tag.get_text(strip=True)
        em_tag = h4.select_one('em')
        if em_tag:
            m = re.search(r'(\d{4}\.\d{2})\s*분기', em_tag.get_text(strip=True))
            if m:
                result['fiscal_quarter'] = m.group(1) + ' 분기'

    for link in soup.select('a.link_site'):
        if 'PER' in link.get_text(strip=True):
            td = link.parent.find_next('td')
            if td:
                em = td.select_one('em')
                result['industry_per'] = _safe_float(em.get_text(strip=True)) if em else None
            break

    _parse_fin_table_from_soup(soup, result)


def _find_latest_annual_col_idx(rows) -> int:
    for row in rows:
        ths = row.select('th')
        tds = row.select('td')
        if not ths or tds:
            continue
        date_cols = [(i, t.get_text(strip=True)) for i, t in enumerate(ths)
                     if _DATE_COL_RE.match(t.get_text(strip=True))]
        if not date_cols:
            continue
        confirmed = [(i, dt) for i, dt in date_cols if '(E)' not in dt]
        if confirmed:
            return confirmed[-1][0] - 1
    return -1


def _parse_fin_table_from_soup(soup, result: dict):
    target_labels = {'ROE': 'roe', '부채비율': 'debt_ratio'}
    for tbl in soup.select('table'):
        rows = tbl.select('tr')
        ths_texts = [r.select_one('th').get_text(strip=True)
                     for r in rows if r.select_one('th')]
        if not any('ROE' in t for t in ths_texts):
            continue
        col_idx = _find_latest_annual_col_idx(rows)
        for row in rows:
            th = row.select_one('th')
            if not th:
                continue
            label = th.get_text(strip=True)
            for keyword, key in target_labels.items():
                if keyword in label and result.get(key) is None:
                    tds = [td.get_text(strip=True) for td in row.select('td')]
                    if not tds:
                        continue
                    v = None
                    if 0 <= col_idx < len(tds):
                        v = _safe_float(tds[col_idx])
                    if v is None:
                        for val in reversed(tds):
                            v = _safe_float(val)
                            if v is not None:
                                break
                    if v is not None:
                        result[key] = v


def _fetch_naver_finance_analysis(code: str, result: dict):
    if result.get('debt_ratio') is not None and result.get('roe') is not None:
        return
    url  = f'https://finance.naver.com/item/coinfo.naver?code={code}&target=finsum_more'
    try:
        resp = requests.get(url, headers=NAVER_HEADERS, timeout=10)
        if resp.encoding and resp.encoding.lower() in ('euc-kr', 'euc_kr'):
            resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'html.parser')
        _parse_fin_table_from_soup(soup, result)
    except Exception:
        pass


def get_fin_metrics(code: str) -> dict:
    if code in _fin_cache:
        return _fin_cache[code]
    result = {k: None for k in ('per', 'pbr', 'debt_ratio', 'roe', 'eps', 'div_yield', 'industry', 'industry_per', 'fiscal_quarter')}
    try:
        _fetch_naver_main(code, result)
        _fetch_naver_finance_analysis(code, result)
        print(f"  [fin] {code}: {result}")
    except Exception as e:
        print(f"  [fin] {code} 수집 실패: {e}")
    _fin_cache[code] = result
    return result


# ── bullet 후처리 (통합 — 단일 소스) ────────────────────────────
_BROKEN_START_RE = re.compile(r'^[\d.,%)~+\-]')
_BROKEN_END_RE   = re.compile(r'[\d.,+\-~(▶]\s*$')
_ARROW_SPLIT_RE  = re.compile(r'\s*[→▶►⇒➜▷➤]\s*')

_FACT_PATTERNS = re.compile(
    r'\d+[%％]'
    r'|\d+조|\d+억|\d+만'
    r'|공시|발표|실적|매출|영업이익|순이익'
    r'|전년[대비동기]|전분기|YoY|QoQ'
    r'|\d+분기|\d{4}년'
)
_OPINION_PATTERNS = re.compile(
    r'전망|예상|기대|추정|전략|추천'
    r'|전망|긍정적|부정적|우려|리스크'
    r'|모멘텀|상승|하락|회복|성장'
)


def _fix_truncated_bullets(bullets: list[str]) -> list[str]:
    """잘린 bullet 항목을 합치고 정리하는 후처리."""
    if not bullets:
        return bullets
    merged = []
    for b in bullets:
        b = b.strip()
        if not b:
            continue
        if merged and _BROKEN_START_RE.match(b):
            merged[-1] = merged[-1] + b
        else:
            merged.append(b)
    result = []
    skip_next = False
    for i, b in enumerate(merged):
        if skip_next:
            skip_next = False
            continue
        if _BROKEN_END_RE.search(b) and i + 1 < len(merged):
            result.append(b + merged[i + 1])
            skip_next = True
        else:
            result.append(b)
    return [b for b in result if len(b) >= 10]


def _split_and_trim_bullets(bullets: list[str], max_items: int = 5, max_chars: int = 50) -> list[str]:
    split_all: list[str] = []
    for b in bullets:
        parts = _ARROW_SPLIT_RE.split(b)
        for p in parts:
            p = p.strip()
            if len(p) >= 8:
                split_all.append(p)
    trimmed: list[str] = []
    for s in split_all:
        if len(s) <= max_chars:
            trimmed.append(s)
            continue
        cut = s[:max_chars]
        last_break = max(cut.rfind(','), cut.rfind('.'), cut.rfind(' '), cut.rfind(')'))
        if last_break > max_chars * 0.5:
            cut = cut[:last_break + 1].rstrip(',. ')
        trimmed.append(cut)
    trimmed = [t for t in trimmed if len(t) >= 10]
    return trimmed[:max_items]


def _sentences_similar(s1: str, s2: str, threshold: float = SIMILARITY_THRESHOLD) -> bool:
    if difflib.SequenceMatcher(None, s1, s2).ratio() >= threshold:
        return True
    words1 = set(re.findall(r"[가-힣a-zA-Z]{2,}", s1))
    words2 = set(re.findall(r"[가-힣a-zA-Z]{2,}", s2))
    if not words1 or not words2:
        return False
    return len(words1 & words2) / len(words1 | words2) >= 0.4


def _dedup_bullets(bullets: list[str]) -> list[str]:
    unique: list[str] = []
    for s in bullets:
        s = s.strip()
        if s and not any(_sentences_similar(s, kept) for kept in unique):
            unique.append(s)
    return unique


def _classify_sentence(sentence: str) -> str:
    if _FACT_PATTERNS.search(sentence):
        return "[Fact]"
    if _OPINION_PATTERNS.search(sentence):
        return "[Opinion]"
    return "[Fact]"


def _fix_truncated_keyword(keyword: str) -> str:
    if not keyword:
        return ""
    k = keyword.strip()
    if _BROKEN_END_RE.search(k):
        return ""
    if len(k) < 5:
        return ""
    return k


def _extract_bullets(text: str, n: int = 4) -> list[str]:
    """titles_summary 텍스트에서 핵심 bullet을 regex 기반 추출.

    TOP10/산업 모두 이 함수를 사용합니다. n=3(산업), n=4(TOP10).
    """
    if not text:
        return []
    sentences = re.split(r"[.\n;|·•→]", text)
    seen = set()
    bullets = []
    for s in sentences:
        s = s.strip()
        if len(s) < 20:
            continue
        if len(s) > 200:
            cut = s[:200]
            for delim in ['▶', '. ', ', ', ' ']:
                idx = cut.rfind(delim)
                if idx >= 80:
                    s = cut[:idx].rstrip()
                    break
            else:
                s = cut.rstrip()
        key = s[:30]
        if key in seen:
            continue
        if any(difflib.SequenceMatcher(None, s, existing).ratio() >= 0.7 for existing in bullets):
            continue
        seen.add(key)
        bullets.append(s)
        if len(bullets) >= n:
            break
    if len(bullets) < 2:
        words = text[:200].split()
        chunk = " ".join(words[:20]) if words else text[:60]
        if chunk and chunk not in seen:
            bullets.append(chunk)
    fixed = _fix_truncated_bullets(bullets[:n])
    # 최종 유사/포함 중복 제거
    unique = []
    for b in fixed:
        is_dup = False
        for u in unique:
            if difflib.SequenceMatcher(None, b, u).ratio() >= 0.6:
                is_dup = True
                break
            short, long = (b, u) if len(b) <= len(u) else (u, b)
            if short[:40] in long:
                is_dup = True
                break
        if not is_dup:
            unique.append(b)
    return unique


def refine_industry_bullets(industry_list: list) -> list:
    refined = []
    for sector in industry_list:
        bullets = sector.get("bullets") or []
        fixed = _fix_truncated_bullets(bullets)
        split = _split_and_trim_bullets(fixed, max_items=5, max_chars=50)
        deduped = _dedup_bullets(split)
        labeled = []
        for s in deduped:
            if s.startswith("[Fact]") or s.startswith("[Opinion]"):
                labeled.append(s)
            else:
                labeled.append(f"{_classify_sentence(s)} {s}")
        new_sector = dict(sector)
        new_sector["bullets"] = labeled[:5]
        refined.append(new_sector)
    return refined


# ── JSON 로드 & 후처리 ──────────────────────────────────────────

def _extract_date(path: Path) -> str | None:
    m = _DATE_RE.search(path.stem)
    return m.group(1) if m else None


def _date_to_label(date_str: str) -> str:
    try:
        d = datetime.strptime(date_str, "%Y%m%d")
        return d.strftime("%Y.%m.%d") + " 주"
    except ValueError:
        return date_str


def _postprocess_top10_bullets(data: dict):
    stock_list = None
    if isinstance(data, dict):
        stock_list = data.get("top5")
    if not isinstance(stock_list, list):
        return
    for item in stock_list:
        if isinstance(item, dict) and "bullets" in item:
            fixed   = _fix_truncated_bullets(item["bullets"])
            split   = _split_and_trim_bullets(fixed, max_items=5, max_chars=50)
            deduped = _dedup_bullets(split)
            item["bullets"] = deduped[:5]


def _fix_opinion_from_ratio(pct: int) -> str:
    if pct >= 30:
        return "Overweight"
    if pct >= 10:
        return "Neutral"
    return ""


def _postprocess_industry_data(data):
    industries = None
    if isinstance(data, dict):
        industries = data.get("industries")
    elif isinstance(data, list):
        industries = data
    if not isinstance(industries, list):
        return
    for item in industries:
        if not isinstance(item, dict):
            continue
        if "bullets" in item:
            item["bullets"] = _fix_truncated_bullets(item["bullets"])
        if "keyword" in item:
            item["keyword"] = _fix_truncated_keyword(item.get("keyword", ""))
        if "overweight_ratio_pct" in item:
            pct = int(item.get("overweight_ratio_pct", 0))
            correct_opinion = _fix_opinion_from_ratio(pct)
            old_opinion = item.get("opinion", "")
            if old_opinion != correct_opinion:
                item["opinion"] = correct_opinion


def load_all_json() -> list[dict]:
    weeks: dict[str, dict] = {}

    _top5_paths = list(TOP5_DIR.glob("유망기업_TOP5_*.json"))
    if _LEGACY_TOP5_DIR.exists():
        _top5_paths += list(_LEGACY_TOP5_DIR.glob("유망기업_TOP5_*.json"))
    for p in sorted(_top5_paths):
        d = _extract_date(p)
        if not d:
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            _postprocess_top10_bullets(data)
            weeks.setdefault(d, {"date": d, "label": _date_to_label(d), "top5": None, "industry": None})
            weeks[d]["top5"] = data
        except Exception as e:
            print(f"[경고] TOP5 JSON 읽기 실패 {p.name}: {e}")

    _ind_paths = list(INDUSTRY_DIR.glob("산업동향_*.json"))
    if _LEGACY_INDUSTRY_DIR.exists():
        _ind_paths += list(_LEGACY_INDUSTRY_DIR.glob("산업동향_*.json"))
    for p in sorted(_ind_paths):
        d = _extract_date(p)
        if not d:
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            _postprocess_industry_data(data)
            if isinstance(data, list) and data and isinstance(data[0], dict) and "bullets" in data[0]:
                data = refine_industry_bullets(data)
            elif isinstance(data, dict):
                for key, val in data.items():
                    if isinstance(val, list) and val and isinstance(val[0], dict) and "bullets" in val[0]:
                        data[key] = refine_industry_bullets(val)
                        break
            weeks.setdefault(d, {"date": d, "label": _date_to_label(d), "top5": None, "industry": None})
            weeks[d]["industry"] = data
        except Exception as e:
            print(f"[경고] 산업 JSON 읽기 실패 {p.name}: {e}")

    return sorted(weeks.values(), key=lambda w: w["date"], reverse=True)


# ── 기술분석: 지표 ──────────────────────────────────────────────

def _rolling_mean(arr: list, window: int) -> list:
    result = [None] * len(arr)
    for i in range(window - 1, len(arr)):
        result[i] = sum(arr[i - window + 1 : i + 1]) / window
    return result


def _calc_rsi(closes: list, period: int = RSI_PERIOD) -> float | None:
    if len(closes) < period + 1:
        return None
    diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = diffs[-period:]
    avg_gain = sum(max(d, 0) for d in recent) / period
    avg_loss = sum(abs(min(d, 0)) for d in recent) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_gain / avg_loss), 1)


def _calc_rsi_series(closes: list, period: int = RSI_PERIOD) -> list:
    n   = len(closes)
    rsi = [None] * n
    if n < period + 1:
        return rsi
    diffs  = [closes[i] - closes[i - 1] for i in range(1, n)]
    gains  = [max(d, 0)   for d in diffs]
    losses = [abs(min(d, 0)) for d in diffs]
    avg_g  = sum(gains[:period])  / period
    avg_l  = sum(losses[:period]) / period
    rsi[period] = 100.0 if avg_l == 0 else round(100 - 100 / (1 + avg_g / avg_l), 1)
    for i in range(period + 1, n):
        avg_g = (avg_g * (period - 1) + gains[i - 1])  / period
        avg_l = (avg_l * (period - 1) + losses[i - 1]) / period
        rsi[i] = 100.0 if avg_l == 0 else round(100 - 100 / (1 + avg_g / avg_l), 1)
    return rsi


def _calc_ema_series(closes: list[float], period: int) -> list[float | None]:
    n = len(closes)
    result: list[float | None] = [None] * n
    if n < period:
        return result
    result[period - 1] = sum(closes[:period]) / period
    k = 2.0 / (period + 1)
    for i in range(period, n):
        result[i] = closes[i] * k + result[i - 1] * (1 - k)
    return result


def _find_pivots(arr: list, min_dist: int = PIVOT_MIN_DISTANCE):
    peaks, troughs = [], []
    for i in range(min_dist, len(arr) - min_dist):
        window = arr[i - min_dist : i + min_dist + 1]
        if arr[i] == max(window):
            peaks.append(i)
        if arr[i] == min(window):
            troughs.append(i)
    return peaks, troughs


def _linreg(x: list, y: list) -> tuple[float, float]:
    n = len(x)
    if n < 2:
        return 0.0, float(y[0]) if y else 0.0
    sx  = sum(x);  sy  = sum(y)
    sxy = sum(xi * yi for xi, yi in zip(x, y))
    sxx = sum(xi * xi for xi in x)
    denom = n * sxx - sx * sx
    if denom == 0:
        return 0.0, sy / n
    slope     = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n
    return slope, intercept


def _avg_vol(volumes: list, i: int, period: int = 20) -> float:
    start = max(0, i - period)
    vals = [volumes[j] for j in range(start, i) if volumes[j] > 0]
    return sum(vals) / len(vals) if vals else 1


# ── 기술분석: 패턴 감지 ─────────────────────────────────────────

def detect_candle_markers(df, trendlines: list | None = None) -> list[dict]:
    if len(df) < 3:
        return []

    opens   = df["Open"].tolist()
    highs   = df["High"].tolist()
    lows    = df["Low"].tolist()
    closes  = df["Close"].tolist()
    volumes = df["Volume"].tolist()
    times   = [str(idx.date()) for idx in df.index]
    n = len(closes)

    markers: list[dict] = []

    for i in range(2, n):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        po, ph, pl, pc = opens[i-1], highs[i-1], lows[i-1], closes[i-1]
        ppo, pph, ppl, ppc = opens[i-2], highs[i-2], lows[i-2], closes[i-2]
        vol  = volumes[i]
        pvol = volumes[i-1]
        avol = _avg_vol(volumes, i)
        body      = abs(c - o)
        full_range = h - l if h != l else 1
        lower_wick = min(o, c) - l
        upper_wick = h - max(o, c)

        vol_surge    = vol >= avol * VOL_SURGE_RATIO
        vol_moderate = vol >= avol * VOL_MODERATE_RATIO

        # 상승반전
        if (lower_wick >= body * 2 and upper_wick <= body * 0.5
                and pc < ppc and vol_moderate):
            markers.append({"time": times[i], "position": "belowBar", "color": "#2196F3", "shape": "arrowUp", "text": "망치형"})
        elif (pc < po and ppc > ppo and c > po and o < pc and vol_surge):
            markers.append({"time": times[i], "position": "belowBar", "color": "#26a69a", "shape": "arrowUp", "text": "상승장악형"})
        elif (ppc > ppo and abs(pc - po) / full_range < 0.3
                and c > o and c > (ppo + ppc) / 2 and vol_surge):
            markers.append({"time": times[i], "position": "belowBar", "color": "#00bcd4", "shape": "arrowUp", "text": "샛별형"})
        # 하락반전
        elif (upper_wick >= body * 2 and lower_wick <= body * 0.5
                and pc > ppc and vol_moderate):
            markers.append({"time": times[i], "position": "aboveBar", "color": "#ef5350", "shape": "arrowDown", "text": "유성형"})
        elif (pc > po and ppc < ppo and c < po and o > pc and vol_surge):
            markers.append({"time": times[i], "position": "aboveBar", "color": "#e53935", "shape": "arrowDown", "text": "하락장악형"})
        elif (ppo < ppc and abs(pc - po) / full_range < 0.3
                and c < o and c < (ppo + ppc) / 2 and vol_surge):
            markers.append({"time": times[i], "position": "aboveBar", "color": "#ff7043", "shape": "arrowDown", "text": "석별형"})
        # VPA
        elif (c > o and c > pc and vol >= pvol * 1.5 and vol_surge):
            markers.append({"time": times[i], "position": "belowBar", "color": "#ff9800", "shape": "circle", "text": "강력돌파"})
        elif (abs(c - o) / max(o, 1) < 0.005 and vol_surge):
            markers.append({"time": times[i], "position": "aboveBar", "color": "#9e9e9e", "shape": "circle", "text": "물량소화"})

    # BB Squeeze
    bb_widths: list[float | None] = []
    for i in range(n):
        if i < BB_PERIOD - 1:
            bb_widths.append(None)
            continue
        sl   = closes[i - BB_PERIOD + 1 : i + 1]
        mean = sum(sl) / BB_PERIOD
        std  = (sum((x - mean) ** 2 for x in sl) / BB_PERIOD) ** 0.5
        bb_widths.append(4.0 * std)
    valid_w  = [w if w is not None else 0.0 for w in bb_widths]
    width_ma = _rolling_mean(valid_w, BB_PERIOD)
    marked_times  = {m["time"] for m in markers}
    prev_squeezed = False
    for i in range(BB_PERIOD * 2, n):
        w, wm = bb_widths[i], width_ma[i]
        if w is None or not wm:
            prev_squeezed = False
            continue
        squeezed = w < wm * 0.5
        if squeezed and not prev_squeezed and times[i] not in marked_times:
            markers.append({"time": times[i], "position": "aboveBar", "color": "#ce93d8", "shape": "circle", "text": "변동성 수축"})
            marked_times.add(times[i])
        prev_squeezed = squeezed

    # 지지/가짜 돌파 필터
    if trendlines:
        time_to_idx = {t: idx for idx, t in enumerate(times)}
        for tl in trendlines:
            if tl["type"] != "resistance":
                continue
            p1_t = tl["p1"]["time"]; p1_v = tl["p1"]["value"]
            p2_t = tl["p2"]["time"]; p2_v = tl["p2"]["value"]
            idx1  = time_to_idx.get(p1_t)
            idx2  = time_to_idx.get(p2_t)
            if idx1 is None or idx2 is None or idx2 <= idx1:
                continue
            slope = (p2_v - p1_v) / (idx2 - idx1)
            for i in range(idx2 + 1, min(idx2 + 20, n)):
                res_val  = p2_v + slope * (i - idx2)
                prev_c   = closes[i - 1]
                cur_c    = closes[i]
                if not (prev_c <= res_val and cur_c > res_val):
                    continue
                avg20 = _avg_vol(volumes, i, 20)
                if volumes[i] >= avg20 * VOL_SURGE_RATIO:
                    markers.append({"time": times[i], "position": "belowBar", "color": "#2ea043", "shape": "arrowUp", "text": "강력 돌파(Confirmed)"})
                else:
                    markers.append({"time": times[i], "position": "belowBar", "color": "rgba(218,54,51,0.5)", "shape": "arrowUp", "text": "약한 돌파(False)"})
                break

    # RSI 상승 다이버전스
    rsi_series = _calc_rsi_series(closes)
    rsi_valid  = [r if r is not None else 50.0 for r in rsi_series]
    try:
        from scipy.signal import argrelextrema
        import numpy as np
        price_arr = np.array(lows, dtype=float)
        rsi_arr   = np.array(rsi_valid, dtype=float)
        order     = max(3, n // 40)
        price_troughs = argrelextrema(price_arr, np.less_equal, order=order)[0].tolist()
        rsi_troughs   = argrelextrema(rsi_arr,   np.less_equal, order=order)[0].tolist()
    except ImportError:
        _, price_troughs = _find_pivots(lows, min_dist=5)
        _, rsi_troughs   = _find_pivots(rsi_valid, min_dist=5)

    if len(price_troughs) >= 2 and len(rsi_troughs) >= 2:
        pt1, pt2 = price_troughs[-2], price_troughs[-1]
        def _nearest(pool: list, idx: int) -> int:
            return min(pool, key=lambda x: abs(x - idx))
        rt1 = _nearest(rsi_troughs, pt1)
        rt2 = _nearest(rsi_troughs, pt2)
        r1, r2 = rsi_valid[rt1], rsi_valid[rt2]
        if (lows[pt2] < lows[pt1] and r2 > r1 and pt2 > pt1
                and times[pt2] not in {m["time"] for m in markers}):
            markers.append({"time": times[pt2], "position": "belowBar", "color": "#f0b429", "shape": "arrowUp", "text": "반전 신호(Divergence)"})

    markers.sort(key=lambda m: m["time"])
    return markers


def detect_trendlines(df, ohlcv: list, n_pivots: int = TRENDLINE_PIVOT_COUNT) -> list[dict]:
    if len(ohlcv) < 30:
        return []
    highs = df["High"].tolist()
    lows  = df["Low"].tolist()
    times = [c["time"] for c in ohlcv]

    peaks,   _  = _find_pivots(highs, min_dist=5)
    _,  troughs = _find_pivots(lows,  min_dist=5)
    peaks   = peaks[-n_pivots:]
    troughs = troughs[-n_pivots:]

    trend_lines: list[dict] = []
    res_slope = sup_slope = None

    if len(peaks) >= 2:
        slope, intercept = _linreg(peaks, [highs[i] for i in peaks])
        res_slope = slope
        i1, i2 = peaks[0], peaks[-1]
        trend_lines.append({
            "type": "resistance", "pattern": "",
            "p1": {"time": times[i1], "value": round(float(highs[i1]), 2)},
            "p2": {"time": times[i2], "value": round(float(slope * i2 + intercept), 2)},
        })

    if len(troughs) >= 2:
        slope, intercept = _linreg(troughs, [lows[i] for i in troughs])
        sup_slope = slope
        i1, i2 = troughs[0], troughs[-1]
        trend_lines.append({
            "type": "support", "pattern": "",
            "p1": {"time": times[i1], "value": round(float(lows[i1]), 2)},
            "p2": {"time": times[i2], "value": round(float(slope * i2 + intercept), 2)},
        })

    pattern = ""
    if res_slope is not None and sup_slope is not None:
        avg_price = sum(highs) / len(highs)
        res_n = res_slope / avg_price * 100
        sup_n = sup_slope / avg_price * 100
        if res_n < -0.05 and sup_n > 0.05:
            pattern = "triangle"
        elif abs(res_n - sup_n) < 0.03:
            pattern = "channel"
        elif res_n < 0 and sup_n < 0 and res_n < sup_n:
            pattern = "falling_wedge"
        for tl in trend_lines:
            tl["pattern"] = pattern

    return trend_lines


def detect_patterns(df) -> list[dict]:
    closes = df["Close"].tolist()
    n = len(closes)
    patterns = []
    if n < 30:
        return patterns

    ma5  = _rolling_mean(closes, 5)
    ma20 = _rolling_mean(closes, 20)

    for i in range(max(1, n - 15), n):
        if None in (ma5[i], ma20[i], ma5[i - 1], ma20[i - 1]):
            continue
        if ma5[i - 1] <= ma20[i - 1] and ma5[i] > ma20[i]:
            patterns.append({"name": "골든크로스", "type": "bullish"})
            break
        if ma5[i - 1] >= ma20[i - 1] and ma5[i] < ma20[i]:
            patterns.append({"name": "데드크로스", "type": "bearish"})
            break

    cur_close = closes[-1]
    cur_ma20  = ma20[-1]
    prev_ma20 = next((v for v in reversed(ma20[max(0, n - 6) : n - 1]) if v is not None), None)
    if cur_ma20 and prev_ma20:
        slope = (cur_ma20 - prev_ma20) / prev_ma20
        if cur_close > cur_ma20 and slope > 0.002:
            patterns.append({"name": "상승추세", "type": "bullish"})
        elif cur_close < cur_ma20 and slope < -0.002:
            patterns.append({"name": "하락추세", "type": "bearish"})

    rsi = _calc_rsi(closes)
    if rsi is not None:
        if rsi > 70:
            patterns.append({"name": f"과매수 RSI{rsi:.0f}", "type": "warning"})
        elif rsi < 30:
            patterns.append({"name": f"과매도 RSI{rsi:.0f}", "type": "bullish"})

    recent = closes[max(0, n - 60):]
    peaks, troughs = _find_pivots(recent)
    if len(peaks) >= 2:
        p1, p2 = recent[peaks[-2]], recent[peaks[-1]]
        if abs(p1 - p2) / max(p1, p2) < 0.05:
            patterns.append({"name": "쌍봉", "type": "bearish"})
    if len(troughs) >= 2:
        t1, t2 = recent[troughs[-2]], recent[troughs[-1]]
        if abs(t1 - t2) / max(t1, t2) < 0.05:
            patterns.append({"name": "쌍바닥", "type": "bullish"})

    return patterns


# ── 4구간 매매 전략 ──────────────────────────────────────────────

def detect_four_phases(df) -> dict:
    empty = {"current_phase": None, "phase_label": "미감지", "phase_boundaries": []}
    closes = df["Close"].tolist()
    lows = df["Low"].tolist()
    volumes = df["Volume"].tolist()
    times = [str(idx.date()) for idx in df.index]
    n = len(closes)

    if n < 250:
        return empty

    ema224 = _calc_ema_series(closes, EMA_LONG_PERIOD)
    ema_start = next((i for i in range(n) if ema224[i] is not None), n)
    if n - ema_start < 30:
        return empty

    # MA224 하향 돌파 지점
    cross_below_idx = None
    for i in range(n - 1, ema_start, -1):
        if ema224[i] is None or ema224[i - 1] is None:
            continue
        if closes[i] < ema224[i] and closes[i - 1] >= ema224[i - 1]:
            cross_below_idx = i
            break

    if cross_below_idx is None:
        cur_ema = ema224[-1]
        if cur_ema and closes[-1] > cur_ema:
            return {
                "current_phase": 4, "phase_label": "상승 분출",
                "phase1_duration": None, "phase2_duration": None,
                "accumulation_sufficient": None,
                "ma224_breakout_date": None, "breakout_volume_confirmed": None,
                "pullback_detected": None, "support_held": None,
                "ma224_value": round(cur_ema, 1), "phase_boundaries": [],
            }
        return empty

    # 구간1: 하락
    search_end = min(cross_below_idx + 200, n)
    phase1_trough_idx = cross_below_idx
    for i in range(cross_below_idx, search_end):
        if lows[i] < lows[phase1_trough_idx]:
            phase1_trough_idx = i
        if lows[phase1_trough_idx] > 0 and closes[i] > lows[phase1_trough_idx] * 1.05:
            break

    phase1_start = cross_below_idx
    phase1_end = phase1_trough_idx
    phase1_duration = phase1_end - phase1_start + 1

    # 구간2: 기간 조정
    cross_above_idx = None
    for i in range(phase1_end + 1, n):
        if ema224[i] is None or ema224[i - 1] is None:
            continue
        if closes[i - 1] < ema224[i - 1] and closes[i] > ema224[i]:
            cross_above_idx = i
            break

    if cross_above_idx is None:
        if phase1_end == n - 1 or (n - 1 - phase1_end) < 5:
            cur_ema = ema224[-1]
            return {
                "current_phase": 1, "phase_label": "가격 조정",
                "phase1_start": times[phase1_start], "phase1_end": None,
                "phase1_duration": n - phase1_start,
                "phase2_duration": None, "accumulation_sufficient": None,
                "ma224_breakout_date": None, "breakout_volume_confirmed": None,
                "pullback_detected": None, "support_held": None,
                "ma224_value": round(cur_ema, 1) if cur_ema else None,
                "phase_boundaries": [{"start": times[phase1_start], "end": times[-1], "phase": 1}],
            }
        phase2_duration = n - 1 - phase1_end
        accum_ok = phase2_duration > phase1_duration
        cur_ema = ema224[-1]
        return {
            "current_phase": 2, "phase_label": "기간 조정",
            "phase1_start": times[phase1_start], "phase1_end": times[phase1_end],
            "phase1_duration": phase1_duration,
            "phase2_start": times[phase1_end + 1] if phase1_end + 1 < n else None,
            "phase2_end": None, "phase2_duration": phase2_duration,
            "accumulation_sufficient": accum_ok,
            "ma224_breakout_date": None, "breakout_volume_confirmed": None,
            "pullback_detected": None, "support_held": None,
            "ma224_value": round(cur_ema, 1) if cur_ema else None,
            "phase_boundaries": [
                {"start": times[phase1_start], "end": times[phase1_end], "phase": 1},
                {"start": times[min(phase1_end + 1, n - 1)], "end": times[-1], "phase": 2},
            ],
        }

    phase2_start = phase1_end + 1 if phase1_end + 1 < n else phase1_end
    phase2_end = cross_above_idx - 1 if cross_above_idx > 0 else cross_above_idx
    phase2_duration = cross_above_idx - phase1_end
    accum_ok = phase2_duration > phase1_duration

    # 구간3: 돌파 + 눌림목
    breakout_vol_ok = volumes[cross_above_idx] >= _avg_vol(volumes, cross_above_idx) * BREAKOUT_VOL_RATIO

    pullback_idx = None
    support_held = False
    for i in range(cross_above_idx + 1, min(cross_above_idx + 60, n)):
        if ema224[i] is None:
            continue
        if closes[i] <= ema224[i] * 1.02:
            pullback_idx = i
            break

    if pullback_idx is not None:
        for i in range(pullback_idx + 1, min(pullback_idx + 20, n)):
            if ema224[i] is None:
                continue
            if closes[i] > ema224[i] * 1.02 and closes[i] > closes[pullback_idx]:
                support_held = True
                break

    cur_ema = ema224[-1]
    is_above_ma = cur_ema and closes[-1] > cur_ema

    if pullback_idx and support_held and is_above_ma:
        current_phase = 4
        phase_label = "상승 분출"
        phase3_end_idx = pullback_idx + 5 if pullback_idx + 5 < n else n - 1
    elif cross_above_idx and is_above_ma:
        current_phase = 3
        phase_label = "추세 전환"
        phase3_end_idx = n - 1
    elif cross_above_idx and not is_above_ma:
        current_phase = 2
        phase_label = "기간 조정"
        phase3_end_idx = None
    else:
        current_phase = 3
        phase_label = "추세 전환"
        phase3_end_idx = n - 1

    boundaries = [{"start": times[phase1_start], "end": times[phase1_end], "phase": 1}]
    if phase2_start < n:
        boundaries.append({"start": times[phase2_start], "end": times[min(phase2_end, n - 1)], "phase": 2})
    if cross_above_idx < n:
        p3_end = phase3_end_idx if phase3_end_idx else min(cross_above_idx + 30, n - 1)
        boundaries.append({"start": times[cross_above_idx], "end": times[min(p3_end, n - 1)], "phase": 3})
    if current_phase == 4 and phase3_end_idx and phase3_end_idx < n - 1:
        boundaries.append({"start": times[phase3_end_idx], "end": times[-1], "phase": 4})

    return {
        "current_phase": current_phase, "phase_label": phase_label,
        "phase1_start": times[phase1_start], "phase1_end": times[phase1_end],
        "phase1_duration": phase1_duration,
        "phase2_start": times[phase2_start] if phase2_start < n else None,
        "phase2_end": times[min(phase2_end, n - 1)],
        "phase2_duration": phase2_duration,
        "accumulation_sufficient": accum_ok,
        "ma224_breakout_date": times[cross_above_idx],
        "breakout_volume_confirmed": breakout_vol_ok,
        "pullback_detected": pullback_idx is not None,
        "pullback_date": times[pullback_idx] if pullback_idx else None,
        "support_held": support_held,
        "ma224_value": round(cur_ema, 1) if cur_ema else None,
        "phase_boundaries": boundaries,
    }


# ── 캔들/성과 데이터 조립 ───────────────────────────────────────

def fetch_candle_data(weeks: list[dict]) -> dict:
    try:
        import FinanceDataReader as fdr
    except ImportError:
        print("[경고] FinanceDataReader 미설치 → 캔들 데이터 생략")
        return {}

    week_stocks: dict[str, list] = {}
    all_codes: set[str] = set()
    for week in weeks:
        if not week.get("top5") or not week["top5"].get("top5"):
            continue
        week_stocks[week["date"]] = week["top5"]["top5"]
        for s in week["top5"]["top5"]:
            code = s.get("stock_code", "").strip()
            if code:
                all_codes.add(code)

    if not all_codes:
        return {}

    ohlcv_cache:      dict[str, list] = {}
    pattern_cache:    dict[str, list] = {}
    trendline_cache:  dict[str, list] = {}
    marker_cache:     dict[str, list] = {}
    fourphase_cache:  dict[str, dict] = {}
    for code in sorted(all_codes):
        print(f"  캔들 조회: {code}...")
        try:
            df_full = fdr.DataReader(code)
            df_full = df_full.dropna(subset=["Open", "High", "Low", "Close"])
            df = df_full.tail(CANDLE_HISTORY_DAYS)
            fourphase_cache[code] = detect_four_phases(df)
            ohlcv = []
            for idx, row in df.iterrows():
                ohlcv.append({
                    "time":   str(idx.date()),
                    "open":   float(row["Open"]),
                    "high":   float(row["High"]),
                    "low":    float(row["Low"]),
                    "close":  float(row["Close"]),
                    "volume": int(row["Volume"]) if row["Volume"] == row["Volume"] else 0,
                })
            ohlcv_cache[code]     = ohlcv
            pattern_cache[code]   = detect_patterns(df)
            tl = detect_trendlines(df, ohlcv)
            trendline_cache[code] = tl
            marker_cache[code]    = detect_candle_markers(df, tl)
        except Exception as e:
            print(f"  [경고] {code} 캔들 실패: {e}")

    result: dict[str, dict] = {}
    for date_key, stocks in week_stocks.items():
        week_candles: dict[str, dict] = {}
        for s in stocks:
            code = s.get("stock_code", "").strip()
            if code not in ohlcv_cache:
                continue
            rec_price = s.get("current_price")
            tgt_price = s.get("avg_target_price")
            stop_loss = round(rec_price * 0.9) if rec_price else None
            week_candles[code] = {
                "company_name": s.get("company_name", ""),
                "rank":         s.get("rank"),
                "ohlcv":        ohlcv_cache[code],
                "rec_price":    rec_price,
                "target_price": tgt_price,
                "stop_loss":    stop_loss,
                "patterns":     pattern_cache[code],
                "trend_lines":  trendline_cache.get(code, []),
                "markers":      marker_cache.get(code, []),
                "fin_data":     get_fin_metrics(code),
                "four_phases":  fourphase_cache.get(code, {}),
            }
        if week_candles:
            result[date_key] = week_candles

    return result


def fetch_performance_data(weeks: list[dict]) -> list[dict]:
    try:
        import FinanceDataReader as fdr
        fdr_available = True
    except ImportError:
        print("[경고] FinanceDataReader 미설치")
        fdr_available = False

    price_cache: dict[str, float | None] = {}
    records = []

    for week in weeks:
        if not week.get("top5") or not week["top5"].get("top5"):
            continue
        for stock in week["top5"]["top5"]:
            code      = stock.get("stock_code", "").strip()
            name      = stock.get("company_name", "")
            rec_price = stock.get("current_price")
            tgt_price = stock.get("avg_target_price")
            if not code or not rec_price:
                continue

            if code not in price_cache:
                if fdr_available:
                    try:
                        df = fdr.DataReader(code)
                        price_cache[code] = float(df["Close"].iloc[-1]) if not df.empty else None
                    except Exception as e:
                        print(f"  [경고] {name}({code}) 조회 실패: {e}")
                        price_cache[code] = None
                else:
                    price_cache[code] = None

            cur = price_cache[code]
            if cur is not None:
                return_pct     = round((cur - rec_price) / rec_price * 100, 2)
                target_achieved = bool(cur >= tgt_price) if tgt_price else None
            else:
                return_pct      = None
                target_achieved = None

            records.append({
                "week": week["label"], "date": week["date"],
                "rank": stock.get("rank"), "company_name": name,
                "stock_code": code, "rec_price": rec_price,
                "target_price": tgt_price, "current_price": cur,
                "return_pct": return_pct, "target_achieved": target_achieved,
            })

    records.sort(key=lambda r: (r["date"], r["rank"] or 99), reverse=False)
    records.sort(key=lambda r: r["date"], reverse=True)
    return records


def build_dashboard_data(weeks: list[dict], performance: list[dict], candles: dict) -> str:
    payload = {
        "weeks": weeks,
        "performance": performance,
        "perf_updated_at": datetime.now().isoformat(timespec="seconds"),
        "candles": candles,
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def render_dashboard(data_json: str) -> str:
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"템플릿 파일을 찾을 수 없습니다: {TEMPLATE}")
    html = TEMPLATE.read_text(encoding="utf-8")
    html = html.replace("/*__DASHBOARD_DATA__*/null", data_json, 1)
    return html


# ── 와이즈리포트 스크래퍼: 산업 ─────────────────────────────────

def _ind_normalize_opinion(text: str) -> str:
    if not text:
        return ""
    return INDUSTRY_OPINION_MAP.get(text.strip().lower(), text.strip())


def _ind_parse_html_table(html: str, report_date: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    tables = soup.find_all("table")
    if not tables:
        logger.warning(f"[{report_date}] 테이블 없음")
        return []
    best_table = max(tables, key=lambda t: sum(1 for tr in t.find_all("tr") if len(tr.find_all(["td", "th"])) == 6))
    trs = best_table.find_all("tr")
    HEADER_KEYS = {"기관명", "작성자", "투자의견", "이전의견", "리포트", "요약", "변화", "산업명"}
    for tr in trs:
        tds = tr.find_all(["td", "th"])
        if len(tds) != 6:
            continue
        def cell(i: int) -> str:
            t = tds[i].get_text(separator=" ", strip=True)
            return "" if t in ("&nbsp", "\xa0") else t
        c0 = cell(0)
        if any(k in c0 for k in HEADER_KEYS) or c0 in ("변화", ""):
            continue
        industry_name = c0.strip()
        if not industry_name:
            continue
        inst_raw = cell(1)
        m_inst = re.match(r"^(.+?)\s*\[", inst_raw)
        institution = m_inst.group(1).strip() if m_inst else inst_raw
        m_auth = re.search(r"\]\s*(.+)$", inst_raw)
        author = m_auth.group(1).strip() if m_auth else ""
        opinion = _ind_normalize_opinion(cell(2))
        prev_opinion = _ind_normalize_opinion(cell(3))
        title = re.sub(r"^[►▶•·\s]+", "", cell(4)).strip()
        summary = re.sub(r"^[►▶•·\s]+", "", cell(5)).strip()
        rows.append({
            "report_date": report_date, "industry_name": industry_name,
            "institution": institution, "author": author,
            "opinion": opinion, "prev_opinion": prev_opinion,
            "title": title, "summary": summary,
        })
    logger.info(f"[{report_date}] 파싱된 산업 리포트: {len(rows)}건")
    return rows


def _ind_fetch_with_requests(date_str: str) -> Optional[str]:
    session = requests.Session()
    session.headers.update(WISEREPORT_HEADERS)
    params = {"cn": "", "fmt": "2", "ee": date_str, "sortcol": "IND_NM_KOR", "sorttyp": "asc", "typ": "0", "searchKeyWord": ""}
    try:
        init_resp = session.get(WISEREPORT_BASE_URL, params=params, timeout=20)
        init_resp.raise_for_status()
        soup = BeautifulSoup(init_resp.text, "lxml")
        viewstate = soup.find("input", {"name": "__VIEWSTATE"})
        eventval = soup.find("input", {"name": "__EVENTVALIDATION"})
        viewstate_gen = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        data_rows = [r for r in soup.find_all("tr") if len(r.find_all("td")) >= 6]
        if len(data_rows) > 3:
            return init_resp.text
        if viewstate:
            post_data = {
                "__VIEWSTATE": viewstate.get("value", ""),
                "__EVENTVALIDATION": eventval.get("value", "") if eventval else "",
                "__VIEWSTATEGENERATOR": viewstate_gen.get("value", "") if viewstate_gen else "",
                "__EVENTTARGET": "", "__EVENTARGUMENT": "",
                "cn": "", "fmt": "2", "ee": date_str,
                "sortcol": "IND_NM_KOR", "sorttyp": "asc", "typ": "0", "searchKeyWord": "",
            }
            post_resp = session.post(WISEREPORT_BASE_URL, data=post_data, timeout=20)
            post_resp.raise_for_status()
            soup2 = BeautifulSoup(post_resp.text, "lxml")
            rows2 = [r for r in soup2.find_all("tr") if len(r.find_all("td")) >= 6]
            if len(rows2) > 3:
                return post_resp.text
        return init_resp.text
    except requests.RequestException as e:
        logger.warning(f"[{date_str}] requests 실패: {e}")
        return None


def _ind_fetch_with_playwright(date_str: str) -> Optional[str]:
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    except ImportError:
        logger.error("playwright 미설치")
        return None
    url = f"{WISEREPORT_BASE_URL}?cn=&fmt=2&ee={date_str}&sortcol=IND_NM_KOR&sorttyp=asc&typ=0&searchKeyWord="
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(locale="ko-KR", extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9"})
            page = ctx.new_page()
            page.goto(url, wait_until="networkidle", timeout=30_000)
            try:
                page.wait_for_selector("table tr td", timeout=10_000)
            except PwTimeout:
                pass
            html = page.content()
            browser.close()
        return html
    except Exception as e:
        logger.error(f"[{date_str}] Playwright 실패: {e}")
        return None


def fetch_industry_one_day(date_str: str) -> list[dict]:
    html = _ind_fetch_with_requests(date_str)
    if html:
        soup = BeautifulSoup(html, "lxml")
        data_rows = [r for r in soup.find_all("tr") if len(r.find_all("td")) >= 6]
        if len(data_rows) <= 3:
            html = _ind_fetch_with_playwright(date_str)
    else:
        html = _ind_fetch_with_playwright(date_str)
    if not html:
        return []
    return _ind_parse_html_table(html, date_str)


def fetch_industry_week(start_date: date, end_date: date) -> pd.DataFrame:
    all_rows: list[dict] = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            date_str = current.strftime("%Y%m%d")
            logger.info(f"수집 중: {date_str} ({current.strftime('%a')})")
            all_rows.extend(fetch_industry_one_day(date_str))
            time.sleep(SCRAPE_DELAY_SEC)
        current += timedelta(days=1)
    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    logger.info(f"총 수집: {len(df)}건 / 산업 수: {df['industry_name'].nunique()}")
    return df


# ── 산업 가공 ────────────────────────────────────────────────────

def ind_extract_keyword(titles_summary: str) -> str:
    if not titles_summary:
        return ""
    sentences = re.split(r"[.\n;|·•→]", titles_summary)
    for s in sentences:
        s = s.strip()
        if 15 <= len(s) <= 60:
            return _fix_truncated_keyword(s[:40])
    return _fix_truncated_keyword(titles_summary.strip()[:40])


def generate_industry_bullets(titles_summary: str, industry_name: str, report_count: int = 1) -> list[str]:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _extract_bullets(titles_summary, n=3)
    try:
        import anthropic
    except ImportError:
        return _extract_bullets(titles_summary, n=3)
    if not titles_summary or not titles_summary.strip():
        return _extract_bullets(titles_summary, n=3)
    text_input = titles_summary[:2000]
    prompt = (
        f"아래는 {industry_name} 산업에 대한 이번 주 증권사 리포트 제목과 요약입니다.\n"
        f"핵심 트렌드, 주요 이슈, 주목할 포인트를 중심으로\n"
        f"30~50자 한국어 bullet 3줄로 요약해줘.\n"
        f"수치나 키워드가 있으면 반드시 포함해줘.\n\n"
        f"[리포트 텍스트]\n{text_input}\n\n"
        f"- 각 bullet은 30~50자 이내 한국어\n"
        f"- 앞에 '•', '-', '→' 같은 기호 없이 텍스트만 출력\n"
        f"- 줄바꿈으로만 구분 (번호 매기지 말 것)\n"
        f"- 정확히 3줄"
    )
    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(model=CLAUDE_MODEL, max_tokens=600, messages=[{"role": "user", "content": prompt}])
        text = next((b.text for b in response.content if b.type == "text"), "").strip()
        bullets = [line.strip().lstrip("•-→· ").strip() for line in text.splitlines() if line.strip() and len(line.strip()) > 10]
        bullets = [b for b in bullets if len(b) >= 15][:3]
        bullets = _fix_truncated_bullets(bullets)
        if bullets:
            return bullets
        return _extract_bullets(titles_summary, n=3)
    except Exception as e:
        logger.warning(f"[{industry_name}] API 호출 실패: {e} → regex 폴백")
        return _extract_bullets(titles_summary, n=3)


def aggregate_by_industry(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["is_overweight"] = df["opinion"].isin(OVERWEIGHT_OPINIONS)
    df["text_combined"] = (df["title"].fillna("") + " " + df["summary"].fillna("")).str.strip()
    grp = df.groupby("industry_name", sort=False)
    agg = grp.agg(
        report_count=("industry_name", "count"),
        overweight_ratio=("is_overweight", "mean"),
        institution_list=("institution", lambda x: list(x.unique())),
        titles_summary=("text_combined", lambda x: " ".join(x.dropna())),
    ).reset_index()
    agg["main_institutions"] = agg["institution_list"].apply(lambda lst: ", ".join(lst[:3]))
    agg["overweight_ratio_pct"] = (agg["overweight_ratio"] * 100).round(0).astype(int)
    agg["opinion"] = agg["overweight_ratio_pct"].apply(_fix_opinion_from_ratio)
    agg = agg.drop(columns=["institution_list"])
    agg = agg.sort_values("report_count", ascending=False).reset_index(drop=True)
    return agg


def enrich_with_bullets(agg: pd.DataFrame) -> pd.DataFrame:
    if agg.empty:
        return agg
    df = agg.copy()
    df["bullets"] = df.apply(
        lambda r: generate_industry_bullets(r["titles_summary"], r["industry_name"], r["report_count"]), axis=1)
    df["keyword"] = df["titles_summary"].apply(ind_extract_keyword)
    return df


def compute_industry_stats(df_raw: pd.DataFrame) -> dict:
    if df_raw.empty:
        return {"total_reports": 0, "covered_industries": 0, "overweight_industries": 0}
    overweight_mask = df_raw["opinion"].isin(OVERWEIGHT_OPINIONS)
    return {
        "total_reports": len(df_raw),
        "covered_industries": df_raw["industry_name"].nunique(),
        "overweight_industries": df_raw[overweight_mask]["industry_name"].nunique(),
    }


def save_industry_json(agg, stats, start_date, end_date, analysis_days, output_dir="output") -> str:
    def _safe(val):
        if val is None: return None
        if isinstance(val, float) and math.isnan(val): return None
        if hasattr(val, "item"): return val.item()
        return val
    industries = []
    for _, row in agg.iterrows():
        industries.append({
            "industry_name": str(row["industry_name"]),
            "report_count": int(row["report_count"]),
            "overweight_ratio": _safe(row.get("overweight_ratio")),
            "overweight_ratio_pct": int(row.get("overweight_ratio_pct", 0)),
            "opinion": str(row.get("opinion", "")),
            "main_institutions": str(row.get("main_institutions", "")),
            "bullets": list(row["bullets"]) if isinstance(row.get("bullets"), (list, tuple)) else [],
            "keyword": str(row.get("keyword", "")),
        })
    data = {
        "meta": {"report_type": "industry", "start_date": str(start_date), "end_date": str(end_date),
                 "generated_at": datetime.now().isoformat(timespec="seconds"), "analysis_days": analysis_days},
        "stats": stats, "industries": industries,
    }
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    date_str = end_date.strftime("%Y%m%d") if hasattr(end_date, "strftime") else str(end_date).replace("-", "")
    out_path = Path(output_dir) / f"산업동향_{date_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"JSON 저장 완료: {out_path}")
    return str(out_path)


# ── 와이즈리포트 스크래퍼: TOP10 ────────────────────────────────

def _t5_parse_number(text: str) -> Optional[float]:
    if not text:
        return None
    cleaned = re.sub(r"[^\d.]", "", text.strip())
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def _t5_normalize_opinion(text: str) -> str:
    if not text:
        return "NR"
    return TOP10_OPINION_MAP.get(text.strip().lower(), text.strip().upper())


def _t5_normalize_change(text: str) -> str:
    if not text:
        return "변동없음"
    t = text.strip()
    for k, v in TOP10_CHANGE_MAP.items():
        if k in t:
            return v
    return t


def _t5_parse_html_table(html: str, report_date: str) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    tables = soup.find_all("table")
    if not tables:
        logger.warning(f"[{report_date}] 테이블 없음")
        return []
    best_table = max(tables, key=lambda t: sum(1 for tr in t.find_all("tr") if len(tr.find_all(["td", "th"])) == 7))
    trs = best_table.find_all("tr")
    HEADER_KEYS = {"기관명", "작성자", "투자의견", "목표주가", "전일종가", "리포트", "요약", "변화"}
    for tr in trs:
        tds = tr.find_all(["td", "th"])
        if len(tds) != 7:
            continue
        def cell(i: int) -> str:
            t = tds[i].get_text(separator=" ", strip=True)
            return "" if t in ("&nbsp", "\xa0") else t
        c0 = cell(0)
        if any(k in c0 for k in HEADER_KEYS) or c0 in ("변화", ""):
            continue
        company_raw = c0
        m_code = re.search(r"\((\d{6})\)", company_raw)
        stock_code = m_code.group(1) if m_code else ""
        company_name = re.sub(r"\s*\(\d{6}\)", "", company_raw).strip()
        if not company_name:
            continue
        inst_raw = cell(1)
        m_inst = re.match(r"^(.+?)\s*\[", inst_raw)
        institution = m_inst.group(1).strip() if m_inst else inst_raw
        m_auth = re.search(r"\]\s*(.+)$", inst_raw)
        author = m_auth.group(1).strip() if m_auth else ""
        opinion_raw = cell(2)
        opinion = _t5_normalize_opinion(opinion_raw) if opinion_raw else "NR"
        if opinion_raw.strip().lower() in ("신규", "new", "initiate", "initiation"):
            opinion_change = "신규"
        elif opinion_raw.strip() == "":
            opinion_change = "변동없음"
        else:
            opinion_change = "변동없음"
        tp_raw = cell(3)
        target_price = _t5_parse_number(tp_raw) if tp_raw not in ("", "&nbsp", "nbsp") else None
        cp_raw = cell(4)
        current_price = _t5_parse_number(cp_raw)
        upside = None
        if target_price and current_price and current_price > 0:
            upside = (target_price - current_price) / current_price * 100
        title = re.sub(r"^[►▶•·\s]+", "", cell(5)).strip()
        summary = re.sub(r"^[►▶•·\s]+", "", cell(6)).strip()
        rows.append({
            "report_date": report_date, "company_name": company_name,
            "stock_code": stock_code, "institution": institution, "author": author,
            "opinion": opinion, "opinion_change": opinion_change,
            "target_price": target_price, "current_price": current_price,
            "upside": upside, "title": title, "summary": summary,
        })
    logger.info(f"[{report_date}] 파싱된 리포트: {len(rows)}건")
    return rows


def _t5_fetch_with_requests(date_str: str) -> Optional[str]:
    session = requests.Session()
    session.headers.update(WISEREPORT_HEADERS)
    params = {"cn": "", "fmt": "1", "ee": date_str, "sortcol": "CMP_NM_KOR", "sorttyp": "asc", "typ": "0", "searchKeyWord": ""}
    try:
        init_resp = session.get(WISEREPORT_BASE_URL, params=params, timeout=20)
        init_resp.raise_for_status()
        soup = BeautifulSoup(init_resp.text, "lxml")
        viewstate = soup.find("input", {"name": "__VIEWSTATE"})
        eventval = soup.find("input", {"name": "__EVENTVALIDATION"})
        viewstate_gen = soup.find("input", {"name": "__VIEWSTATEGENERATOR"})
        data_rows = [r for r in soup.find_all("tr") if len(r.find_all("td")) >= 6]
        if len(data_rows) > 3:
            return init_resp.text
        if viewstate:
            post_data = {
                "__VIEWSTATE": viewstate.get("value", ""),
                "__EVENTVALIDATION": eventval.get("value", "") if eventval else "",
                "__VIEWSTATEGENERATOR": viewstate_gen.get("value", "") if viewstate_gen else "",
                "__EVENTTARGET": "", "__EVENTARGUMENT": "",
                "cn": "", "fmt": "1", "ee": date_str,
                "sortcol": "CMP_NM_KOR", "sorttyp": "asc", "typ": "0", "searchKeyWord": "",
            }
            post_resp = session.post(WISEREPORT_BASE_URL, data=post_data, timeout=20)
            post_resp.raise_for_status()
            soup2 = BeautifulSoup(post_resp.text, "lxml")
            rows2 = [r for r in soup2.find_all("tr") if len(r.find_all("td")) >= 6]
            if len(rows2) > 3:
                return post_resp.text
        return init_resp.text
    except requests.RequestException as e:
        logger.warning(f"[{date_str}] requests 실패: {e}")
        return None


def _t5_fetch_with_playwright(date_str: str) -> Optional[str]:
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PwTimeout
    except ImportError:
        logger.error("playwright 미설치")
        return None
    url = f"{WISEREPORT_BASE_URL}?cn=&fmt=1&ee={date_str}&sortcol=CMP_NM_KOR&sorttyp=asc&typ=0&searchKeyWord="
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            ctx = browser.new_context(locale="ko-KR", extra_http_headers={"Accept-Language": "ko-KR,ko;q=0.9"})
            page = ctx.new_page()
            page.goto(url, wait_until="networkidle", timeout=30_000)
            try:
                page.wait_for_selector("table tr td", timeout=10_000)
            except PwTimeout:
                pass
            html = page.content()
            browser.close()
        return html
    except Exception as e:
        logger.error(f"[{date_str}] Playwright 실패: {e}")
        return None


def fetch_top5_one_day(date_str: str) -> list[dict]:
    html = _t5_fetch_with_requests(date_str)
    if html:
        soup = BeautifulSoup(html, "lxml")
        data_rows = [r for r in soup.find_all("tr") if len(r.find_all("td")) >= 6]
        if len(data_rows) <= 3:
            html = _t5_fetch_with_playwright(date_str)
    else:
        html = _t5_fetch_with_playwright(date_str)
    if not html:
        return []
    return _t5_parse_html_table(html, date_str)


def fetch_top5_week(start_date: date, end_date: date) -> pd.DataFrame:
    all_rows: list[dict] = []
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            date_str = current.strftime("%Y%m%d")
            logger.info(f"수집 중: {date_str} ({current.strftime('%a')})")
            all_rows.extend(fetch_top5_one_day(date_str))
            time.sleep(SCRAPE_DELAY_SEC)
        current += timedelta(days=1)
    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows)
    logger.info(f"총 수집: {len(df)}건 / 종목 수: {df['company_name'].nunique()}")
    return df


# ── TOP10 가공/스코어링 ─────────────────────────────────────────

def _t5_momentum_score(text: str, report_count: int) -> float:
    if not text or report_count == 0:
        return 0.0
    count = sum(1 for kw in MOMENTUM_KEYWORDS if kw in text)
    return min(count / max(report_count, 1), 1.0)


def _t5_norm(series: pd.Series) -> pd.Series:
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series([0.5] * len(series), index=series.index)
    return (series - mn) / (mx - mn)


def generate_bullets_with_claude(titles_summary: str, company_name: str, report_count: int = 1) -> list[str]:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return _extract_bullets(titles_summary)
    try:
        import anthropic
    except ImportError:
        return _extract_bullets(titles_summary)
    if not titles_summary or not titles_summary.strip():
        return _extract_bullets(titles_summary)
    text_input = titles_summary[:2000]
    prompt = (
        f"다음은 증권사 애널리스트들이 {company_name}에 대해 작성한 "
        f"리포트 제목과 요약을 모은 텍스트입니다 (총 {report_count}건).\n\n"
        f"[리포트 텍스트]\n{text_input}\n\n"
        "위 내용을 바탕으로 투자자가 주목해야 할 핵심 투자 포인트를 "
        "3~4줄 bullet 형식으로 간결하게 작성해주세요.\n"
        "- 각 bullet은 30~60자 이내로 한국어로 작성\n"
        "- 구체적 수치(%, 원, 배율 등)가 있으면 포함\n"
        "- 앞에 '•', '-', '→' 같은 기호 없이 텍스트만 출력\n"
        "- 줄바꿈으로만 구분 (번호 매기지 말 것)\n"
        "- 4줄을 넘지 말 것"
    )
    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(model=CLAUDE_MODEL, max_tokens=800, messages=[{"role": "user", "content": prompt}])
        if response.stop_reason == "max_tokens":
            logger.warning(f"[{company_name}] Claude 응답이 잘림")
        text = next((b.text for b in response.content if b.type == "text"), "").strip()
        bullets = [line.strip().lstrip("•-→· ").strip() for line in text.splitlines() if line.strip() and len(line.strip()) > 10]
        bullets = [b for b in bullets if len(b) >= 15][:4]
        unique = []
        for b in bullets:
            is_dup = False
            for u in unique:
                if difflib.SequenceMatcher(None, b, u).ratio() >= 0.6:
                    is_dup = True; break
                short, long = (b, u) if len(b) <= len(u) else (u, b)
                if short[:40] in long:
                    is_dup = True; break
            if not is_dup:
                unique.append(b)
        bullets = _fix_truncated_bullets(unique)
        if bullets:
            return bullets
        return _extract_bullets(titles_summary)
    except Exception as e:
        logger.warning(f"[{company_name}] API 호출 실패: {e} → regex 폴백")
        return _extract_bullets(titles_summary)


def aggregate_weekly(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["target_price"] = pd.to_numeric(df["target_price"], errors="coerce")
    df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")
    df["is_buy"] = df["opinion"].str.upper().isin(BUY_OPINIONS)
    df["is_up"] = df["opinion_change"].isin(["이전대비상향", "신규"])
    df["text_combined"] = (df["title"].fillna("") + " " + df["summary"].fillna("")).str.strip()
    grp = df.groupby("company_name", sort=False)
    agg = grp.agg(
        stock_code=("stock_code", "first"),
        institution_list=("institution", lambda x: ", ".join(x.unique()[:5])),
        report_count=("company_name", "count"),
        buy_ratio=("is_buy", "mean"),
        target_up_ratio=("is_up", "mean"),
        avg_target_price=("target_price", "mean"),
        max_target_price=("target_price", "max"),
        current_price=("current_price", "last"),
        titles_summary=("text_combined", lambda x: " ".join(x.dropna())),
    ).reset_index()
    agg["avg_upside"] = (
        (agg["avg_target_price"] - agg["current_price"])
        / agg["current_price"].replace(0, float("nan")) * 100
    ).round(1)
    agg = agg[agg["current_price"] > 0].copy()
    agg = agg[agg["avg_target_price"].notna()].copy()
    return agg


def score_and_select_top5(agg: pd.DataFrame) -> pd.DataFrame:
    if agg.empty:
        return pd.DataFrame()
    df = agg.copy()
    df["momentum_score"] = df.apply(lambda r: _t5_momentum_score(r["titles_summary"], r["report_count"]), axis=1)
    df = df[df["report_count"] >= 1].copy()
    if df.empty:
        return pd.DataFrame()
    df["score"] = (
        _t5_norm(df["report_count"])    * 0.25
        + _t5_norm(df["buy_ratio"])     * 0.25
        + _t5_norm(df["target_up_ratio"]) * 0.20
        + _t5_norm(df["avg_upside"].fillna(0)) * 0.20
        + _t5_norm(df["momentum_score"]) * 0.10
    )
    top5 = df.nlargest(10, "score").reset_index(drop=True)
    top5["rank"] = top5.index + 1
    top5["bullets"] = top5.apply(
        lambda r: generate_bullets_with_claude(r["titles_summary"], r["company_name"], r["report_count"]), axis=1)
    top5["current_price_fmt"] = top5["current_price"].apply(lambda x: f"{x:,.0f}원" if pd.notna(x) else "N/A")
    top5["avg_target_price_fmt"] = top5["avg_target_price"].apply(lambda x: f"{x:,.0f}원" if pd.notna(x) else "N/A")
    top5["max_target_price_fmt"] = top5["max_target_price"].apply(lambda x: f"{x:,.0f}원" if pd.notna(x) else "N/A")
    top5["avg_upside_fmt"] = top5["avg_upside"].apply(
        lambda x: f"+{x:.1f}%" if pd.notna(x) and x >= 0 else (f"{x:.1f}%" if pd.notna(x) else "N/A"))
    top5["all_buy"] = top5["buy_ratio"] >= 0.9
    top5["target_up_tag"] = top5["target_up_ratio"] >= 0.5
    for _, row in top5.iterrows():
        logger.info(f"  {row['rank']}위 {row['company_name']} (score={row['score']:.3f}, 리포트={row['report_count']}건, 상승여력={row['avg_upside_fmt']})")
    return top5


def compute_top5_stats(df_raw: pd.DataFrame) -> dict:
    if df_raw.empty:
        return {"total_reports": 0, "buy_count": 0, "target_up_count": 0, "total_companies": 0}
    buy_mask = df_raw["opinion"].str.upper().isin(BUY_OPINIONS)
    up_mask = df_raw["opinion_change"].isin(["이전대비상향", "신규"])
    return {
        "total_reports": len(df_raw),
        "buy_count": int(buy_mask.sum()),
        "target_up_count": int(up_mask.sum()),
        "total_companies": df_raw["company_name"].nunique(),
    }


def save_top5_json(top5, stats, start_date, end_date, analysis_days, output_dir="output") -> str:
    def _safe(val):
        if val is None: return None
        if isinstance(val, float) and math.isnan(val): return None
        if hasattr(val, "item"): return val.item()
        return val
    companies = []
    for _, row in top5.iterrows():
        companies.append({
            "rank": int(row["rank"]), "company_name": str(row["company_name"]),
            "stock_code": str(row.get("stock_code", "")),
            "institution_list": str(row.get("institution_list", "")),
            "report_count": int(row["report_count"]),
            "buy_ratio": _safe(row.get("buy_ratio")),
            "target_up_ratio": _safe(row.get("target_up_ratio")),
            "current_price": _safe(row.get("current_price")),
            "avg_target_price": _safe(row.get("avg_target_price")),
            "max_target_price": _safe(row.get("max_target_price")),
            "avg_upside": _safe(row.get("avg_upside")),
            "bullets": list(row["bullets"]) if isinstance(row.get("bullets"), (list, tuple)) else [],
        })
    data = {
        "meta": {"report_type": "top5", "start_date": str(start_date), "end_date": str(end_date),
                 "generated_at": datetime.now().isoformat(timespec="seconds"), "analysis_days": analysis_days},
        "stats": stats, "top5": companies,
    }
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    date_str = end_date.strftime("%Y%m%d") if hasattr(end_date, "strftime") else str(end_date).replace("-", "")
    out_path = Path(output_dir) / f"유망기업_TOP5_{date_str}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"JSON 저장 완료: {out_path}")
    return str(out_path)


# ── 스크래핑 파이프라인 ──────────────────────────────────────────

def get_weekly_dates(reference: Optional[date] = None) -> tuple[date, date]:
    today = reference or date.today()
    monday = today - timedelta(days=today.weekday())
    friday = monday + timedelta(days=4)
    if friday > today:
        monday -= timedelta(weeks=1)
        friday -= timedelta(weeks=1)
    return monday, friday


def _scrape_pipeline(start_date, end_date, save_json: bool = True) -> None:
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(name)s - %(message)s", datefmt="%H:%M:%S")
    print(f"\n[스크랩] 분석 기간: {start_date} ~ {end_date}")

    analysis_days = sum(
        1 for i in range((end_date - start_date).days + 1)
        if (start_date + timedelta(days=i)).weekday() < 5
    )

    print("\n[1/2] TOP5 종목 리포트 스크랩 중...")
    try:
        df_t5 = fetch_top5_week(start_date, end_date)
        if df_t5.empty:
            print("  [경고] TOP5 수집 결과 없음")
        else:
            print(f"  수집: {len(df_t5)}건")
            agg_t5 = aggregate_weekly(df_t5)
            top5 = score_and_select_top5(agg_t5)
            stats_t5 = compute_top5_stats(df_t5)
            if save_json and not top5.empty:
                TOP5_DIR.mkdir(parents=True, exist_ok=True)
                save_top5_json(top5, stats_t5, start_date, end_date, analysis_days, str(TOP5_DIR))
                print(f"  TOP5 JSON 저장: {TOP5_DIR}")
    except Exception as e:
        print(f"  [오류] TOP5 파이프라인 실패: {e}")

    print("\n[2/2] 산업동향 리포트 스크랩 중...")
    try:
        df_ind = fetch_industry_week(start_date, end_date)
        if df_ind.empty:
            print("  [경고] 산업 수집 결과 없음")
        else:
            print(f"  수집: {len(df_ind)}건")
            agg_ind = aggregate_by_industry(df_ind)
            agg_ind = enrich_with_bullets(agg_ind)
            stats_ind = compute_industry_stats(df_ind)
            if save_json and not agg_ind.empty:
                INDUSTRY_DIR.mkdir(parents=True, exist_ok=True)
                save_industry_json(agg_ind, stats_ind, start_date, end_date, analysis_days, str(INDUSTRY_DIR))
                print(f"  산업 JSON 저장: {INDUSTRY_DIR}")
    except Exception as e:
        print(f"  [오류] 산업 파이프라인 실패: {e}")

    print("\n[스크랩] 완료\n")


def _parse_yyyymmdd(s: str):
    return datetime.strptime(s, "%Y%m%d").date()


def main() -> None:
    parser = argparse.ArgumentParser(description="주간 투자 리서치 대시보드 생성")
    parser.add_argument("--output", default=str(DEFAULT_OUT), help="출력 HTML 파일 경로")
    parser.add_argument("--no-open", action="store_true", help="브라우저 자동 실행 비활성화")
    parser.add_argument("--scrape", action="store_true", help="와이즈리포트에서 직접 스크래핑하여 JSON 갱신 후 대시보드 생성")
    parser.add_argument("--start", type=_parse_yyyymmdd, metavar="YYYYMMDD", help="(--scrape 전용) 시작일")
    parser.add_argument("--end", type=_parse_yyyymmdd, metavar="YYYYMMDD", help="(--scrape 전용) 종료일")
    args = parser.parse_args()

    out_path = Path(args.output)

    if args.scrape:
        if args.start and args.end:
            sd, ed = args.start, args.end
        else:
            sd, ed = get_weekly_dates()
            print(f"날짜 자동 설정: {sd} ~ {ed}")
        _scrape_pipeline(sd, ed, save_json=True)

    print("데이터 로딩 중...")
    weeks = load_all_json()

    if not weeks:
        print("[경고] JSON 데이터가 없습니다. --scrape 옵션으로 직접 수집하거나")
        print("       wisereport_top5/run.py 또는 wisereport_industry/run.py를 먼저 실행해 주세요.")
        print("       빈 대시보드를 생성합니다.")

    print(f"  로드된 주차: {len(weeks)}개")
    for w in weeks[:3]:
        top5_ok = "O" if w["top5"] else "X"
        ind_ok   = "O" if w["industry"] else "X"
        print(f"  {w['label']}  TOP5:{top5_ok}  산업:{ind_ok}")
    if len(weeks) > 3:
        print(f"  ... 외 {len(weeks) - 3}주")

    print("현재가 조회 중 (FinanceDataReader)...")
    performance = fetch_performance_data(weeks)
    print(f"  성과 데이터: {len(performance)}개 종목")

    print("캔들 데이터 조회 중...")
    candles = fetch_candle_data(weeks)
    print(f"  캔들 데이터: {len(candles)}개 종목")

    data_json = build_dashboard_data(weeks, performance, candles)
    html = render_dashboard(data_json)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    print(f"\n대시보드 생성 완료: {out_path}")
    print(f"파일 크기: {out_path.stat().st_size / 1024:.1f} KB")

    if not args.no_open:
        url = out_path.resolve().as_uri()
        print(f"브라우저 열기: {url}")
        webbrowser.open(url)


if __name__ == "__main__":
    main()
