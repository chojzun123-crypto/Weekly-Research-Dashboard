"""
generate_dashboard.py — 주간 투자 리서치 대시보드 생성기

두 프로젝트의 JSON 데이터를 읽어 단일 HTML 대시보드를 생성합니다.

사용법:
  python generate_dashboard.py                    # dashboard.html 생성 후 브라우저 오픈
  python generate_dashboard.py --no-open          # 브라우저 오픈 없이 생성만
  python generate_dashboard.py --output foo.html  # 출력 파일명 지정
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import webbrowser
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup


# ── 네이버 금융 재무지표 스크래핑 ───────────────────────────────
_NAVER_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://finance.naver.com',
}
_fin_cache: dict[str, dict] = {}   # 종목당 1회만 조회


def _safe_float(text: str, multiplier: float = 1.0):
    """문자열 → float 변환. 실패 시 None."""
    try:
        cleaned = text.replace(',', '').replace('%', '').replace('배', '').replace('원', '').strip()
        if not cleaned or cleaned in ('-', 'N/A', 'n/a'):
            return None
        return round(float(cleaned) * multiplier, 2)
    except (ValueError, AttributeError):
        return None


def _fetch_naver_main(code: str, result: dict):
    """네이버 금융 메인 페이지 → PER, EPS, PBR, 배당수익률, ROE, 부채비율 수집.
    em#_per / em#_eps / em#_pbr / em#_dvr id로 직접 추출.
    기업실적분석 테이블에서 ROE, 부채비율도 함께 수집.
    """
    url  = f'https://finance.naver.com/item/main.naver?code={code}'
    resp = requests.get(url, headers=_NAVER_HEADERS, timeout=10)
    # 네이버 금융 메인은 UTF-8 인코딩
    if resp.encoding and resp.encoding.lower() in ('euc-kr', 'euc_kr'):
        resp.encoding = 'utf-8'
    soup = BeautifulSoup(resp.text, 'html.parser')

    id_map = {
        '_per': 'per',
        '_eps': 'eps',
        '_pbr': 'pbr',
        '_dvr': 'div_yield',
    }
    for em_id, key in id_map.items():
        el = soup.select_one(f'em#{em_id}')
        if el:
            result[key] = _safe_float(el.get_text(strip=True))

    # 업종명 + 재무 기준 분기 추출 (h4.sub_tit7)
    h4 = soup.select_one('h4.sub_tit7')
    if h4:
        a_tag = h4.select_one('a')
        if a_tag:
            result['industry'] = a_tag.get_text(strip=True)
        em_tag = h4.select_one('em')
        if em_tag:
            import re as _re
            m = _re.search(r'(\d{4}\.\d{2})\s*분기', em_tag.get_text(strip=True))
            if m:
                result['fiscal_quarter'] = m.group(1) + ' 분기'

    # 동일업종 PER (a.link_site 중 'PER' 포함)
    for link in soup.select('a.link_site'):
        if 'PER' in link.get_text(strip=True):
            td = link.parent.find_next('td')
            if td:
                em = td.select_one('em')
                result['industry_per'] = _safe_float(em.get_text(strip=True)) if em else None
            break

    # 기업실적분석 테이블 → ROE, 부채비율
    _parse_fin_table_from_soup(soup, result)


_DATE_COL_RE = re.compile(r'^\d{4}\.\d{2}$')


def _find_latest_annual_col_idx(rows) -> int:
    """테이블 헤더 행에서 '(E)'가 없는 가장 마지막 날짜 열 인덱스 반환.
    찾지 못하면 -1 반환."""
    for row in rows:
        ths = row.select('th')
        tds = row.select('td')
        if not ths or tds:           # 데이터 행(td 있음) 제외
            continue
        date_cols = [(i, t.get_text(strip=True)) for i, t in enumerate(ths)
                     if _DATE_COL_RE.match(t.get_text(strip=True))]
        if not date_cols:
            continue
        # (E) 없는 날짜 중 마지막 = 최신 확정 연간
        confirmed = [(i, dt) for i, dt in date_cols if '(E)' not in dt]
        if confirmed:
            return confirmed[-1][0] - 1   # th 인덱스 → td 인덱스 (row header 제외)
    return -1   # 감지 실패 시 첫 번째 열 사용


def _parse_fin_table_from_soup(soup, result: dict):
    """BeautifulSoup 객체에서 기업실적분석 테이블의 ROE·부채비율 파싱.
    최신 확정 연간 열 값을 우선 사용."""
    target_labels = {
        'ROE': 'roe',
        '부채비율': 'debt_ratio',
    }
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
                    # 최신 확정 연간 열 우선, 실패 시 비어있지 않은 마지막 값
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
    """네이버 금융 coinfo → 부채비율·ROE 보완 수집 (main에서 이미 수집된 경우 스킵)."""
    if result.get('debt_ratio') is not None and result.get('roe') is not None:
        return   # 이미 수집 완료

    url  = f'https://finance.naver.com/item/coinfo.naver?code={code}&target=finsum_more'
    try:
        resp = requests.get(url, headers=_NAVER_HEADERS, timeout=10)
        # 인코딩 자동 감지 (UTF-8 우선)
        if resp.encoding and resp.encoding.lower() in ('euc-kr', 'euc_kr'):
            resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'html.parser')
        _parse_fin_table_from_soup(soup, result)
    except Exception:
        pass


def get_fin_metrics(code: str) -> dict:
    """네이버 금융에서 재무지표 수집 (종목당 1회 캐싱)."""
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

# ── 경로 설정 ───────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
TOP5_DIR     = SCRIPT_DIR.parent / "wisereport_top5"    / "output"
INDUSTRY_DIR = SCRIPT_DIR.parent / "wisereport_industry" / "output"
TEMPLATE     = SCRIPT_DIR / "template.html"
DEFAULT_OUT  = SCRIPT_DIR / "dashboard.html"

_DATE_RE = re.compile(r"(\d{8})")


def _extract_date(path: Path) -> str | None:
    m = _DATE_RE.search(path.stem)
    return m.group(1) if m else None


def _date_to_label(date_str: str) -> str:
    try:
        d = datetime.strptime(date_str, "%Y%m%d")
        return d.strftime("%Y.%m.%d") + " 주"
    except ValueError:
        return date_str


_BROKEN_START_RE = re.compile(r'^[\d.,%)~+\-]')
_BROKEN_END_RE   = re.compile(r'[\d.,+\-~(▶]\s*$')


_ARROW_SPLIT_RE = re.compile(r'\s*[▶►→▷➤]\s*')


def _fix_truncated_bullets(bullets: list[str]) -> list[str]:
    """잘린 bullet 항목을 합치고 정리하는 후처리 (대시보드 로딩 시 적용)."""
    if not bullets:
        return bullets

    # 1단계: 숫자/소수점/%/)로 시작하는 항목은 앞 항목에 합치기
    merged = []
    for b in bullets:
        b = b.strip()
        if not b:
            continue
        if merged and _BROKEN_START_RE.match(b):
            merged[-1] = merged[-1] + b
        else:
            merged.append(b)

    # 2단계: 숫자/불완전 단어로 끝나는 항목은 다음 항목과 합치기
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

    # 3단계: 10자 미만의 의미없는 조각 제거
    return [b for b in result if len(b) >= 10]


def _split_and_trim_bullets(bullets: list[str], max_items: int = 5, max_chars: int = 50) -> list[str]:
    """▶/→ 등 화살표로 이어 붙여진 긴 bullet을 개별 문장으로 분리하고 요약.

    규칙:
      - 하나의 bullet 항목은 하나의 문장만 담을 것
      - ▶ / → 등 화살표로 문장을 이어 붙이지 말 것
      - 한 항목은 최대 max_chars자 이내로 끊을 것
      - 항목 수는 max_items개로 제한할 것
    """
    # 1) 화살표 기호로 분리
    split_all: list[str] = []
    for b in bullets:
        parts = _ARROW_SPLIT_RE.split(b)
        for p in parts:
            p = p.strip()
            if len(p) >= 8:
                split_all.append(p)

    # 2) 긴 문장 자르기 (max_chars 초과 시 자연스러운 끊기점에서 절단)
    trimmed: list[str] = []
    for s in split_all:
        if len(s) <= max_chars:
            trimmed.append(s)
            continue
        # 쉼표, 마침표, 공백 등에서 끊기
        cut = s[:max_chars]
        # 마지막 쉼표/마침표/공백 위치에서 자르기
        last_break = max(cut.rfind(','), cut.rfind('.'), cut.rfind(' '), cut.rfind(')'))
        if last_break > max_chars * 0.5:
            cut = cut[:last_break + 1].rstrip(',. ')
        trimmed.append(cut)

    # 3) 10자 미만 조각 제거
    trimmed = [t for t in trimmed if len(t) >= 10]

    return trimmed[:max_items]


def _fix_truncated_keyword(keyword: str) -> str:
    """잘린 keyword를 검증하고, 불완전하면 빈 문자열로 대체."""
    if not keyword:
        return ""
    k = keyword.strip()
    if _BROKEN_END_RE.search(k):
        return ""
    if len(k) < 5:
        return ""
    return k


_FACT_PATTERNS = re.compile(
    r'\d+[%％]'
    r'|\d+조|\d+억|\d+만'
    r'|공시|발표|실적|매출|영업이익|순이익'
    r'|전년[대비동기]|전분기|YoY|QoQ'
    r'|\d+분기|\d{4}년'
)
_OPINION_PATTERNS = re.compile(
    r'전망|예상|기대|추정|전략|추천'
    r'|유망|긍정적|부정적|우려|리스크'
    r'|모멘텀|상승|하락|회복|성장'
)


def _classify_sentence(sentence: str) -> str:
    """문장을 [Fact] 또는 [Opinion]으로 분류."""
    if _FACT_PATTERNS.search(sentence):
        return "[Fact]"
    if _OPINION_PATTERNS.search(sentence):
        return "[Opinion]"
    return "[Fact]"


def _sentences_similar(s1: str, s2: str, threshold: float = 0.7) -> bool:
    """문자 수준 유사도(SequenceMatcher) + 키워드 자카드 유사도로 중복 판별."""
    if difflib.SequenceMatcher(None, s1, s2).ratio() >= threshold:
        return True
    # 의미적 유사도: 2자 이상 단어 집합 자카드
    words1 = set(re.findall(r"[가-힣a-zA-Z]{2,}", s1))
    words2 = set(re.findall(r"[가-힣a-zA-Z]{2,}", s2))
    if not words1 or not words2:
        return False
    return len(words1 & words2) / len(words1 | words2) >= 0.4


def refine_industry_bullets(industry_list: list) -> list:
    """산업 섹터 리스트의 bullets 정제:
      1) 잘린 텍스트 합치기
      2) ▶/→ 화살표로 분리 + 항목당 50자 절단
      3) 유사도 기반 중복 제거
      4) [Fact] / [Opinion] 레이블 부착
      5) 섹터당 최대 5문장으로 제한
    """
    refined = []
    for sector in industry_list:
        bullets = sector.get("bullets") or []

        # 1. 잘린 텍스트 합치기
        fixed = _fix_truncated_bullets(bullets)

        # 2. 화살표 분리 + 50자 절단 + 5개 제한
        split = _split_and_trim_bullets(fixed, max_items=5, max_chars=50)

        # 3. 중복·유사 문장 제거
        deduped = _dedup_bullets(split)

        # 4. Fact/Opinion 레이블 부착 (이미 붙어 있으면 스킵)
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


def _dedup_bullets(bullets: list[str]) -> list[str]:
    """유사도 기반 중복 bullet 제거 (SequenceMatcher + 자카드)."""
    unique: list[str] = []
    for s in bullets:
        s = s.strip()
        if s and not any(_sentences_similar(s, kept) for kept in unique):
            unique.append(s)
    return unique


def _postprocess_top10_bullets(data: dict):
    """TOP10 JSON 내 모든 종목의 bullets를 후처리.

    파이프라인:
      1) 잘린 텍스트 합치기 (_fix_truncated_bullets)
      2) ▶/→ 화살표로 이어진 문장 분리 + 50자 이내 절단 (_split_and_trim_bullets)
      3) 유사도 기반 중복 제거 (_dedup_bullets)
      4) 최종 3~5개 항목으로 제한
    """
    stock_list = None
    if isinstance(data, dict):
        stock_list = data.get("top5")   # JSON 키명은 "top5"로 유지 (하위 호환)
    if not isinstance(stock_list, list):
        return
    for item in stock_list:
        if isinstance(item, dict) and "bullets" in item:
            fixed   = _fix_truncated_bullets(item["bullets"])
            split   = _split_and_trim_bullets(fixed, max_items=5, max_chars=50)
            deduped = _dedup_bullets(split)
            item["bullets"] = deduped[:5]


def _fix_opinion_from_ratio(pct: int) -> str:
    """overweight_ratio_pct 기준으로 opinion을 자동 결정."""
    if pct >= 30:
        return "Overweight"
    if pct >= 10:
        return "Neutral"
    return ""


def _postprocess_industry_data(data):
    """산업 JSON 내 모든 섹터의 bullets + keyword + opinion 일관성 후처리."""
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
        # opinion ↔ overweight_ratio_pct 일관성 보정
        if "overweight_ratio_pct" in item:
            pct = int(item.get("overweight_ratio_pct", 0))
            correct_opinion = _fix_opinion_from_ratio(pct)
            old_opinion = item.get("opinion", "")
            if old_opinion != correct_opinion:
                item["opinion"] = correct_opinion


def load_all_json() -> list[dict]:
    """
    두 output 폴더에서 JSON 파일을 읽어 날짜별로 병합.
    반환: 날짜 내림차순 정렬된 week dict 리스트.
      [{"label": "2026.03.27 주", "date": "20260327",
        "top5": {...} | None, "industry": {...} | None}, ...]
    """
    weeks: dict[str, dict] = {}

    # TOP10 JSON
    for p in sorted(TOP5_DIR.glob("유망기업_TOP5_*.json")):
        d = _extract_date(p)
        if not d:
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            # TOP10 bullets 후처리: 잘린 텍스트 합치기 + 중복 제거
            _postprocess_top10_bullets(data)
            weeks.setdefault(d, {"date": d, "label": _date_to_label(d), "top5": None, "industry": None})
            weeks[d]["top5"] = data
        except Exception as e:
            print(f"[경고] TOP5 JSON 읽기 실패 {p.name}: {e}")

    # 산업 JSON
    for p in sorted(INDUSTRY_DIR.glob("산업동향_*.json")):
        d = _extract_date(p)
        if not d:
            continue
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            # 산업 bullets + keyword 후처리
            _postprocess_industry_data(data)
            # bullets 정제: 최상위 리스트 또는 dict 내부 리스트 탐색
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

    # 날짜 내림차순
    return sorted(weeks.values(), key=lambda w: w["date"], reverse=True)


# ── 패턴 감지 헬퍼 ──────────────────────────────────────────
def _rolling_mean(arr: list, window: int) -> list:
    result = [None] * len(arr)
    for i in range(window - 1, len(arr)):
        result[i] = sum(arr[i - window + 1 : i + 1]) / window
    return result


def _calc_rsi(closes: list, period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    diffs = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = diffs[-period:]
    avg_gain = sum(max(d, 0) for d in recent) / period
    avg_loss = sum(abs(min(d, 0)) for d in recent) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - 100 / (1 + avg_gain / avg_loss), 1)


def _calc_rsi_series(closes: list, period: int = 14) -> list:
    """전체 구간 RSI 시리즈 반환 (길이 = closes, 초기 미계산 구간은 None)."""
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


def _find_pivots(arr: list, min_dist: int = 5):
    peaks, troughs = [], []
    for i in range(min_dist, len(arr) - min_dist):
        window = arr[i - min_dist : i + min_dist + 1]
        if arr[i] == max(window):
            peaks.append(i)
        if arr[i] == min(window):
            troughs.append(i)
    return peaks, troughs


def detect_candle_markers(df, trendlines: list | None = None) -> list[dict]:
    """캔들 패턴 + 거래량 분석 + 고급 알고리즘 마커 생성.

    감지 패턴:
      상승반전: 망치형, 상승장악형, 샛별형
      하락반전: 유성형, 하락장악형, 석별형
      VPA: 강력돌파, 물량소화
      고급: BB Squeeze, 가짜 돌파(Low Vol), RSI 상승 다이버전스
    거래량 동반 조건 필수 적용 (노이즈 최소화).
    """
    if len(df) < 3:
        return []

    opens   = df["Open"].tolist()
    highs   = df["High"].tolist()
    lows    = df["Low"].tolist()
    closes  = df["Close"].tolist()
    volumes = df["Volume"].tolist()
    times   = [str(idx.date()) for idx in df.index]
    n = len(closes)

    # 20일 평균 거래량 (기준선)
    def avg_vol(i: int, period: int = 20) -> float:
        start = max(0, i - period)
        vals = [volumes[j] for j in range(start, i) if volumes[j] > 0]
        return sum(vals) / len(vals) if vals else 1

    markers: list[dict] = []

    for i in range(2, n):
        o, h, l, c = opens[i], highs[i], lows[i], closes[i]
        po, ph, pl, pc = opens[i-1], highs[i-1], lows[i-1], closes[i-1]
        ppo, pph, ppl, ppc = opens[i-2], highs[i-2], lows[i-2], closes[i-2]
        vol  = volumes[i]
        pvol = volumes[i-1]
        avol = avg_vol(i)
        body      = abs(c - o)
        full_range = h - l if h != l else 1
        lower_wick = min(o, c) - l
        upper_wick = h - max(o, c)

        # ── 거래량 조건 ─────────────────────────────────────
        vol_surge    = vol >= avol * 1.5          # 평균 대비 150%+
        vol_moderate = vol >= avol * 1.0          # 평균 이상 (패턴 최소 조건)

        # ── 상승반전 ──────────────────────────────────────
        # 망치형: 하단 꼬리 ≥ 몸통 2배, 상단 꼬리 작음, 이전 하락 추세
        if (lower_wick >= body * 2 and upper_wick <= body * 0.5
                and pc < ppc and vol_moderate):
            markers.append({
                "time": times[i], "position": "belowBar",
                "color": "#2196F3", "shape": "arrowUp", "text": "망치형",
            })

        # 상승장악형: 이전 음봉 완전 포함하는 양봉 + 거래량 급증
        elif (pc < po and ppc > ppo and c > po and o < pc and vol_surge):
            markers.append({
                "time": times[i], "position": "belowBar",
                "color": "#26a69a", "shape": "arrowUp", "text": "상승장악형",
            })

        # 샛별형: 이전이전 큰 음봉 → 이전 소형봉(갭다운) → 당일 큰 양봉
        elif (ppc > ppo and abs(pc - po) / full_range < 0.3
                and c > o and c > (ppo + ppc) / 2 and vol_surge):
            markers.append({
                "time": times[i], "position": "belowBar",
                "color": "#00bcd4", "shape": "arrowUp", "text": "샛별형",
            })

        # ── 하락반전 ──────────────────────────────────────
        # 유성형: 상단 꼬리 ≥ 몸통 2배, 하단 꼬리 작음, 이전 상승 추세
        elif (upper_wick >= body * 2 and lower_wick <= body * 0.5
                and pc > ppc and vol_moderate):
            markers.append({
                "time": times[i], "position": "aboveBar",
                "color": "#ef5350", "shape": "arrowDown", "text": "유성형",
            })

        # 하락장악형: 이전 양봉 완전 포함하는 음봉 + 거래량 급증
        elif (pc > po and ppc < ppo and c < po and o > pc and vol_surge):
            markers.append({
                "time": times[i], "position": "aboveBar",
                "color": "#e53935", "shape": "arrowDown", "text": "하락장악형",
            })

        # 석별형: 이전이전 큰 양봉 → 이전 소형봉(갭업) → 당일 큰 음봉
        elif (ppo < ppc and abs(pc - po) / full_range < 0.3
                and c < o and c < (ppo + ppc) / 2 and vol_surge):
            markers.append({
                "time": times[i], "position": "aboveBar",
                "color": "#ff7043", "shape": "arrowDown", "text": "석별형",
            })

        # ── VPA ───────────────────────────────────────────
        # 강력 돌파: 가격 상승 + 거래량 전일 대비 150%+ 및 평균 150%+
        elif (c > o and c > pc and vol >= pvol * 1.5 and vol_surge):
            markers.append({
                "time": times[i], "position": "belowBar",
                "color": "#ff9800", "shape": "circle", "text": "강력돌파",
            })

        # 물량 소화: 가격 변동 < 0.5% but 거래량 급증
        elif (abs(c - o) / max(o, 1) < 0.005 and vol_surge):
            markers.append({
                "time": times[i], "position": "aboveBar",
                "color": "#9e9e9e", "shape": "circle", "text": "물량소화",
            })

    # ── 알고리즘 1: BB Squeeze (변동성 응축) ─────────────────
    BB_PERIOD = 20
    bb_widths: list[float | None] = []
    for i in range(n):
        if i < BB_PERIOD - 1:
            bb_widths.append(None)
            continue
        sl   = closes[i - BB_PERIOD + 1 : i + 1]
        mean = sum(sl) / BB_PERIOD
        std  = (sum((x - mean) ** 2 for x in sl) / BB_PERIOD) ** 0.5
        bb_widths.append(4.0 * std)            # (upper - lower) = 2 * mult * std, mult=2
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
        # 응축 구간 진입 첫 봉에만 마커 1개
        if squeezed and not prev_squeezed and times[i] not in marked_times:
            markers.append({
                "time": times[i], "position": "aboveBar",
                "color": "#ce93d8", "shape": "circle", "text": "변동성 응축",
            })
            marked_times.add(times[i])
        prev_squeezed = squeezed

    # ── 알고리즘 2: 진짜/가짜 돌파 필터 (저항선 기준, 20일 평균 거래량) ──
    if trendlines:
        time_to_idx = {t: idx for idx, t in enumerate(times)}
        for tl in trendlines:
            if tl["type"] != "resistance":
                continue
            # 추세선 연장값: p1→p2 기울기로 각 바의 저항가 계산
            p1_t = tl["p1"]["time"]; p1_v = tl["p1"]["value"]
            p2_t = tl["p2"]["time"]; p2_v = tl["p2"]["value"]
            idx1  = time_to_idx.get(p1_t)
            idx2  = time_to_idx.get(p2_t)
            if idx1 is None or idx2 is None or idx2 <= idx1:
                continue
            slope = (p2_v - p1_v) / (idx2 - idx1)

            for i in range(idx2 + 1, min(idx2 + 20, n)):
                res_val  = p2_v + slope * (i - idx2)   # 추세선 연장값
                prev_c   = closes[i - 1]
                cur_c    = closes[i]
                if not (prev_c <= res_val and cur_c > res_val):
                    continue
                # 20일 평균 거래량 기준 150% 판별
                v20  = [volumes[j] for j in range(max(0, i - 20), i) if volumes[j] > 0]
                avg20 = sum(v20) / len(v20) if v20 else 1
                if volumes[i] >= avg20 * 1.5:
                    markers.append({
                        "time": times[i], "position": "belowBar",
                        "color": "#2ea043", "shape": "arrowUp",
                        "text": "강력 돌파(Confirmed)",
                    })
                else:
                    markers.append({
                        "time": times[i], "position": "belowBar",
                        "color": "rgba(218,54,51,0.5)", "shape": "arrowUp",
                        "text": "약한 돌파(False)",
                    })
                break  # 추세선당 최초 돌파봉 1개만

    # ── 알고리즘 3: RSI 상승 다이버전스 (scipy argrelextrema) ──
    rsi_series = _calc_rsi_series(closes)
    rsi_valid  = [r if r is not None else 50.0 for r in rsi_series]
    try:
        from scipy.signal import argrelextrema
        import numpy as np
        price_arr = np.array(lows, dtype=float)
        rsi_arr   = np.array(rsi_valid, dtype=float)
        order     = max(3, n // 40)          # 데이터 길이에 비례한 감도
        price_troughs = argrelextrema(price_arr, np.less_equal, order=order)[0].tolist()
        rsi_troughs   = argrelextrema(rsi_arr,   np.less_equal, order=order)[0].tolist()
    except ImportError:
        # scipy 없으면 자체 피벗으로 폴백
        _, price_troughs = _find_pivots(lows, min_dist=5)
        _, rsi_troughs   = _find_pivots(rsi_valid, min_dist=5)

    # 가격/RSI 저점을 각각 최근 2개씩 비교
    if len(price_troughs) >= 2 and len(rsi_troughs) >= 2:
        pt1, pt2 = price_troughs[-2], price_troughs[-1]
        # RSI 저점 중 pt1, pt2 에 가장 가까운 것 매칭
        def _nearest(pool: list, idx: int) -> int:
            return min(pool, key=lambda x: abs(x - idx))
        rt1 = _nearest(rsi_troughs, pt1)
        rt2 = _nearest(rsi_troughs, pt2)
        r1, r2 = rsi_valid[rt1], rsi_valid[rt2]
        if (lows[pt2] < lows[pt1]                         # 가격 Lower Low
                and r2 > r1                                # RSI Higher Low
                and pt2 > pt1                              # 시간순
                and times[pt2] not in {m["time"] for m in markers}):
            markers.append({
                "time": times[pt2], "position": "belowBar",
                "color": "#f0b429", "shape": "arrowUp",
                "text": "반전 신호(Divergence)",
            })

    # 시간순 정렬
    markers.sort(key=lambda m: m["time"])
    return markers


def _linreg(x: list, y: list) -> tuple[float, float]:
    """최소제곱 선형 회귀 → (slope, intercept)."""
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


def detect_trendlines(df, ohlcv: list, n_pivots: int = 5) -> list[dict]:
    """저항선·지지선 계산 및 수렴(삼각)/평행(채널) 판별.
    반환: trend_lines 리스트
      [{"type": "resistance"|"support", "pattern": str,
        "p1": {"time": str, "value": float},
        "p2": {"time": str, "value": float}}, ...]
    """
    if len(ohlcv) < 30:
        return []

    highs = df["High"].tolist()
    lows  = df["Low"].tolist()
    times = [c["time"] for c in ohlcv]

    # 고점 피벗(highs 기준), 저점 피벗(lows 기준)
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
            "type":    "resistance",
            "pattern": "",
            "p1": {"time": times[i1], "value": round(float(highs[i1]), 2)},
            "p2": {"time": times[i2], "value": round(float(slope * i2 + intercept), 2)},
        })

    if len(troughs) >= 2:
        slope, intercept = _linreg(troughs, [lows[i] for i in troughs])
        sup_slope = slope
        i1, i2 = troughs[0], troughs[-1]
        trend_lines.append({
            "type":    "support",
            "pattern": "",
            "p1": {"time": times[i1], "value": round(float(lows[i1]), 2)},
            "p2": {"time": times[i2], "value": round(float(slope * i2 + intercept), 2)},
        })

    # 수렴/평행 판별 (정규화된 기울기 비교)
    pattern = ""
    if res_slope is not None and sup_slope is not None:
        avg_price = sum(highs) / len(highs)
        res_n = res_slope / avg_price * 100   # % 변화/bar
        sup_n = sup_slope / avg_price * 100
        if res_n < -0.05 and sup_n > 0.05:
            pattern = "triangle"              # 수렴 삼각형
        elif abs(res_n - sup_n) < 0.03:
            pattern = "channel"               # 평행 채널

        # ── 하락 쐐기형(Falling Wedge) ─────────────────────
        # 조건: 두 기울기 모두 음수, 저항선 기울기 < 지지선 기울기 (저항이 더 가파르게 하락 = 수렴)
        elif res_n < 0 and sup_n < 0 and res_n < sup_n:
            pattern = "falling_wedge"

        for tl in trend_lines:
            tl["pattern"] = pattern

    return trend_lines


def detect_patterns(df) -> list[dict]:
    """기술적 패턴 감지 (골든크로스, 데드크로스, 추세, RSI, 쌍봉/쌍바닥)."""
    closes = df["Close"].tolist()
    n = len(closes)
    patterns = []
    if n < 30:
        return patterns

    ma5  = _rolling_mean(closes, 5)
    ma20 = _rolling_mean(closes, 20)

    # 골든크로스 / 데드크로스 (최근 15일 이내)
    for i in range(max(1, n - 15), n):
        if None in (ma5[i], ma20[i], ma5[i - 1], ma20[i - 1]):
            continue
        if ma5[i - 1] <= ma20[i - 1] and ma5[i] > ma20[i]:
            patterns.append({"name": "골든크로스", "type": "bullish"})
            break
        if ma5[i - 1] >= ma20[i - 1] and ma5[i] < ma20[i]:
            patterns.append({"name": "데드크로스", "type": "bearish"})
            break

    # 추세 (MA20 기울기 기반)
    cur_close = closes[-1]
    cur_ma20  = ma20[-1]
    prev_ma20 = next((v for v in reversed(ma20[max(0, n - 6) : n - 1]) if v is not None), None)
    if cur_ma20 and prev_ma20:
        slope = (cur_ma20 - prev_ma20) / prev_ma20
        if cur_close > cur_ma20 and slope > 0.002:
            patterns.append({"name": "상승추세", "type": "bullish"})
        elif cur_close < cur_ma20 and slope < -0.002:
            patterns.append({"name": "하락추세", "type": "bearish"})

    # RSI 과매수/과매도
    rsi = _calc_rsi(closes)
    if rsi is not None:
        if rsi > 70:
            patterns.append({"name": f"과매수 RSI{rsi:.0f}", "type": "warning"})
        elif rsi < 30:
            patterns.append({"name": f"과매도 RSI{rsi:.0f}", "type": "bullish"})

    # 쌍봉 / 쌍바닥 (최근 60일)
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


# ── 4구간 매매 전략 감지 ──────────────────────────────────────────
def _calc_ema_series(closes: list[float], period: int) -> list[float | None]:
    """EMA 시리즈 계산 (closes와 같은 길이, 초기 구간은 None)."""
    n = len(closes)
    result: list[float | None] = [None] * n
    if n < period:
        return result
    # SMA로 시드
    result[period - 1] = sum(closes[:period]) / period
    k = 2.0 / (period + 1)
    for i in range(period, n):
        result[i] = closes[i] * k + result[i - 1] * (1 - k)
    return result


def detect_four_phases(df) -> dict:
    """4구간 매매 전략 분석.

    구간 1 – 가격 조정: MA224 하향 돌파 후 하락 구간
    구간 2 – 기간 조정: 더 이상 저점 갱신 없이 횡보 (기간 > 구간1)
    구간 3 – 추세 전환: MA224 상향 돌파 + 눌림목 안착
    구간 4 – 시세 분출: 안착 이후 강한 상승
    """
    empty = {"current_phase": None, "phase_label": "미감지", "phase_boundaries": []}

    closes = df["Close"].tolist()
    highs = df["High"].tolist()
    lows = df["Low"].tolist()
    volumes = df["Volume"].tolist()
    times = [str(idx.date()) for idx in df.index]
    n = len(closes)

    if n < 250:
        return empty

    ema224 = _calc_ema_series(closes, 224)

    # 유효한 EMA 시작 인덱스
    ema_start = next((i for i in range(n) if ema224[i] is not None), n)
    if n - ema_start < 30:
        return empty

    # ── 1) MA224 하향 돌파 지점 찾기 (최근부터 역방향 탐색) ───────
    cross_below_idx = None
    for i in range(n - 1, ema_start, -1):
        if ema224[i] is None or ema224[i - 1] is None:
            continue
        if closes[i] < ema224[i] and closes[i - 1] >= ema224[i - 1]:
            cross_below_idx = i
            break

    if cross_below_idx is None:
        # MA224 아래로 간 적이 없음 → 이미 상승 추세 중
        cur_ema = ema224[-1]
        if cur_ema and closes[-1] > cur_ema:
            return {
                "current_phase": 4,
                "phase_label": "시세 분출",
                "phase1_duration": None, "phase2_duration": None,
                "accumulation_sufficient": None,
                "ma224_breakout_date": None, "breakout_volume_confirmed": None,
                "pullback_detected": None, "support_held": None,
                "ma224_value": round(cur_ema, 1),
                "phase_boundaries": [],
            }
        return empty

    # ── 2) 구간1: 하락 구간 (cross_below → 저점) ─────────────────
    # 저점 찾기: cross_below 이후 가장 낮은 lows
    search_end = min(cross_below_idx + 200, n)  # 최대 200일 탐색
    phase1_trough_idx = cross_below_idx
    for i in range(cross_below_idx, search_end):
        if lows[i] < lows[phase1_trough_idx]:
            phase1_trough_idx = i
        # 저점에서 5% 이상 반등하면 하락 종료로 간주
        if lows[phase1_trough_idx] > 0 and closes[i] > lows[phase1_trough_idx] * 1.05:
            break

    phase1_start = cross_below_idx
    phase1_end = phase1_trough_idx
    phase1_duration = phase1_end - phase1_start + 1
    phase1_low = lows[phase1_trough_idx]

    # ── 3) 구간2: 기간 조정 (저점 이후 횡보, MA224 상향 돌파 전) ──
    cross_above_idx = None
    for i in range(phase1_end + 1, n):
        if ema224[i] is None:
            continue
        # MA224 상향 돌파: 이전 종가 < EMA, 현재 종가 > EMA, 0.5%+ 돌파
        if i > 0 and ema224[i - 1] is not None:
            if closes[i - 1] < ema224[i - 1] and closes[i] > ema224[i]:
                cross_above_idx = i
                break

    if cross_above_idx is None:
        # 아직 MA224 돌파 전 → 구간2 진행 중이거나 구간1 진행 중
        if phase1_end == n - 1 or (n - 1 - phase1_end) < 5:
            cur_ema = ema224[-1]
            return {
                "current_phase": 1,
                "phase_label": "가격 조정",
                "phase1_start": times[phase1_start],
                "phase1_end": None,
                "phase1_duration": n - phase1_start,
                "phase2_duration": None,
                "accumulation_sufficient": None,
                "ma224_breakout_date": None, "breakout_volume_confirmed": None,
                "pullback_detected": None, "support_held": None,
                "ma224_value": round(cur_ema, 1) if cur_ema else None,
                "phase_boundaries": [
                    {"start": times[phase1_start], "end": times[-1], "phase": 1},
                ],
            }
        # 구간2 진행 중
        phase2_duration = n - 1 - phase1_end
        accum_ok = phase2_duration > phase1_duration
        cur_ema = ema224[-1]
        return {
            "current_phase": 2,
            "phase_label": "기간 조정",
            "phase1_start": times[phase1_start],
            "phase1_end": times[phase1_end],
            "phase1_duration": phase1_duration,
            "phase2_start": times[phase1_end + 1] if phase1_end + 1 < n else None,
            "phase2_end": None,
            "phase2_duration": phase2_duration,
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

    # ── 4) 구간3: 돌파 거래량 확인 + 눌림목 ─────────────────────
    def _avg_vol(idx: int, period: int = 20) -> float:
        s = max(0, idx - period)
        vals = [volumes[j] for j in range(s, idx) if volumes[j] > 0]
        return sum(vals) / len(vals) if vals else 1

    breakout_vol_ok = volumes[cross_above_idx] >= _avg_vol(cross_above_idx) * 1.3

    # 눌림목 탐색: 돌파 후 MA224 근처까지 내려오는지 (2% 이내)
    pullback_idx = None
    support_held = False
    for i in range(cross_above_idx + 1, min(cross_above_idx + 60, n)):
        if ema224[i] is None:
            continue
        # 종가가 EMA224의 2% 이내로 접근
        if closes[i] <= ema224[i] * 1.02:
            pullback_idx = i
            break

    if pullback_idx is not None:
        # 풀백 이후 반등 확인
        for i in range(pullback_idx + 1, min(pullback_idx + 20, n)):
            if ema224[i] is None:
                continue
            if closes[i] > ema224[i] * 1.02 and closes[i] > closes[pullback_idx]:
                support_held = True
                break

    # ── 현재 구간 판별 ───────────────────────────────────────────
    cur_ema = ema224[-1]
    is_above_ma = cur_ema and closes[-1] > cur_ema

    if pullback_idx and support_held and is_above_ma:
        current_phase = 4
        phase_label = "시세 분출"
        phase3_end_idx = pullback_idx + 5 if pullback_idx + 5 < n else n - 1
    elif cross_above_idx and is_above_ma:
        current_phase = 3
        phase_label = "추세 전환"
        phase3_end_idx = n - 1
    elif cross_above_idx and not is_above_ma:
        # 돌파 후 다시 이탈 → 실패, 구간2 재진입
        current_phase = 2
        phase_label = "기간 조정"
        phase3_end_idx = None
    else:
        current_phase = 3
        phase_label = "추세 전환"
        phase3_end_idx = n - 1

    # 구간 경계 조립
    boundaries = [
        {"start": times[phase1_start], "end": times[phase1_end], "phase": 1},
    ]
    if phase2_start < n:
        boundaries.append({"start": times[phase2_start], "end": times[min(phase2_end, n - 1)], "phase": 2})
    if cross_above_idx < n:
        p3_end = phase3_end_idx if phase3_end_idx else min(cross_above_idx + 30, n - 1)
        boundaries.append({"start": times[cross_above_idx], "end": times[min(p3_end, n - 1)], "phase": 3})
    if current_phase == 4 and phase3_end_idx and phase3_end_idx < n - 1:
        boundaries.append({"start": times[phase3_end_idx], "end": times[-1], "phase": 4})

    return {
        "current_phase": current_phase,
        "phase_label": phase_label,
        "phase1_start": times[phase1_start],
        "phase1_end": times[phase1_end],
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


def fetch_candle_data(weeks: list[dict]) -> dict:
    """각 주차별 TOP5 종목의 캔들 OHLCV 데이터 + 패턴 분석.
    반환 구조: {date: {stock_code: {...}}}
    OHLCV는 종목당 1회 조회 후 캐싱, rec_price/target_price는 주차별로 분리.
    """
    try:
        import FinanceDataReader as fdr
    except ImportError:
        print("[경고] FinanceDataReader 미설치 → 캔들 데이터 생략")
        return {}

    # 주차별 종목 수집 + 전체 고유 종목코드 모음
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

    # OHLCV + 패턴 + 추세선 + 마커 + 4구간 캐시 (코드당 1회만 조회)
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
            # 4구간 분석 + 차트 표시 모두 5년(1250거래일)
            df = df_full.tail(1250)
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

    # 주차별 결과 조립 (rec_price/target_price는 주차별 추천 시점 값 사용)
    result: dict[str, dict] = {}
    for date, stocks in week_stocks.items():
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
            result[date] = week_candles

    return result


def fetch_performance_data(weeks: list[dict]) -> list[dict]:
    """각 주차 TOP5 추천 종목의 현재가를 조회하여 수익률 계산."""
    try:
        import FinanceDataReader as fdr
        fdr_available = True
    except ImportError:
        print("[경고] FinanceDataReader 미설치 → pip install finance-datareader")
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
                "week":            week["label"],
                "date":            week["date"],
                "rank":            stock.get("rank"),
                "company_name":    name,
                "stock_code":      code,
                "rec_price":       rec_price,
                "target_price":    tgt_price,
                "current_price":   cur,
                "return_pct":      return_pct,
                "target_achieved": target_achieved,
            })

    # 최신 주차 먼저, 같은 주차 내에서는 순위순
    records.sort(key=lambda r: (r["date"], r["rank"] or 99), reverse=False)
    records.sort(key=lambda r: r["date"], reverse=True)
    return records


def build_dashboard_data(weeks: list[dict], performance: list[dict], candles: dict) -> str:
    """weeks + 성과 + 캔들 데이터를 JS에 embed할 JSON 문자열로 변환."""
    payload = {
        "weeks": weeks,
        "performance": performance,
        "perf_updated_at": datetime.now().isoformat(timespec="seconds"),
        "candles": candles,
    }
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def render_dashboard(data_json: str) -> str:
    """template.html의 placeholder를 JSON 데이터로 치환."""
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"템플릿 파일을 찾을 수 없습니다: {TEMPLATE}")
    html = TEMPLATE.read_text(encoding="utf-8")
    html = html.replace("/*__DASHBOARD_DATA__*/null", data_json, 1)
    return html


def main() -> None:
    parser = argparse.ArgumentParser(description="주간 투자 리서치 대시보드 생성")
    parser.add_argument("--output", default=str(DEFAULT_OUT), help="출력 HTML 파일 경로")
    parser.add_argument("--no-open", action="store_true", help="브라우저 자동 오픈 비활성화")
    args = parser.parse_args()

    out_path = Path(args.output)

    print("데이터 로딩 중...")
    weeks = load_all_json()

    if not weeks:
        print("[경고] JSON 데이터가 없습니다. 먼저 wisereport_top5/run.py 또는 wisereport_industry/run.py를 실행해 주세요.")
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
