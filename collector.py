#!/usr/bin/env python3
"""공시 수집기: DART(한국) · SEC EDGAR(미국) · EDINET(일본) -> JSON Lines

실행 예:  python collector.py --config config.json
          python collector.py --config config.json --sources dart,edgar
결과: <out_dir>/feed_YYYYMMDD_HHMM.jsonl (새 공시가 있을 때만), _status.json, _state.json
"""
import argparse
import hashlib
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import requests

KST = timezone(timedelta(hours=9))
JST = KST  # 같은 UTC+9
ATOM = {"a": "http://www.w3.org/2005/Atom"}
SEEN_TTL_DAYS = 14


# ---------- 공통 ----------
def now_kst():
    return datetime.now(KST)


def http_get(url, params=None, headers=None, timeout=20, retries=3):
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                last = RuntimeError(f"HTTP {r.status_code}")
                time.sleep(2 ** i)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            last = e
            time.sleep(2 ** i)
    raise last


def make_id(*parts):
    return hashlib.sha1("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()[:16]


def load_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return default


def save_json_atomic(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_config(path):
    cfg = load_json(path, None)
    if cfg is None:
        sys.exit(f"설정 파일을 읽을 수 없습니다: {path}")
    # 환경변수가 있으면 설정 파일보다 우선 (키를 파일에 적지 않아도 됨)
    for env, key in (("DART_API_KEY", "dart_api_key"),
                     ("EDINET_API_KEY", "edinet_api_key"),
                     ("SEC_USER_AGENT", "sec_user_agent")):
        if os.environ.get(env):
            cfg[key] = os.environ[env]
    cfg.setdefault("out_dir", "market-feed")
    cfg.setdefault("lookback_days", 1)
    cfg.setdefault("dart_corp_cls", ["Y", "K"])
    cfg.setdefault("dart_max_pages", 5)
    cfg.setdefault("edgar_forms", ["8-K", "10-Q", "10-K", "6-K", "20-F"])
    cfg.setdefault("keywords", {})
    return cfg


# ---------- 보유·관심 종목 ----------
def load_watchlist(path):
    """holdings.md 의 표를 읽는다. 예시 행과 빈 행은 건너뛴다."""
    rows = []
    if not path or not os.path.exists(path):
        return rows
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.startswith("|"):
                continue
            cells = [c.strip() for c in line.strip().strip("|").split("|")]
            if len(cells) < 4:
                continue
            name, code, market, kind = cells[:4]
            if not name or not code or re.fullmatch(r"-+", name) or name == "종목명":
                continue
            if name.startswith("(예시)"):
                continue
            rows.append({"name": name, "code": code.upper(), "market": market.upper(), "kind": kind})
    return rows


def make_flags(item, watch, keywords):
    flags = []
    company = item.get("company", "")
    code = (item.get("code") or "").upper()
    market = item["market"]
    for w in watch:
        if w["market"] != market:
            continue
        hit = False
        if market == "KR":
            hit = code == w["code"] or w["name"] in company
        elif market == "JP":
            hit = bool(code) and code.startswith(w["code"]) or w["name"] in company
        else:  # US: 공시 피드에 티커가 없어 회사명으로만 대조
            hit = w["name"].lower() in company.lower()
        if hit:
            flags.append(f"watchlist:{w['name']}({w['kind']})")
    for kw in keywords.get(market, []):
        if kw.lower() in item.get("title", "").lower():
            flags.append(f"keyword:{kw}")
    return flags


# ---------- DART (한국) ----------
def fetch_dart(cfg, collected_at):
    key = cfg.get("dart_api_key")
    if not key:
        raise RuntimeError("DART 인증키가 없습니다 (DART_API_KEY)")
    end = collected_at.date()
    bgn = end - timedelta(days=int(cfg["lookback_days"]))
    items, page = [], 1
    while page <= int(cfg["dart_max_pages"]):
        r = http_get("https://opendart.fss.or.kr/api/list.json", params={
            "crtfc_key": key, "bgn_de": bgn.strftime("%Y%m%d"), "end_de": end.strftime("%Y%m%d"),
            "page_no": page, "page_count": 100})
        j = r.json()
        status = j.get("status")
        if status == "013":  # 조회된 데이터 없음
            break
        if status != "000":
            raise RuntimeError(f"DART status {status}: {j.get('message')}")
        for x in j.get("list", []):
            if x.get("corp_cls") not in cfg["dart_corp_cls"]:
                continue
            d = x["rcept_dt"]  # YYYYMMDD (시각 정보 없음)
            posted = datetime(int(d[:4]), int(d[4:6]), int(d[6:]), tzinfo=KST)
            title = x.get("report_nm", "").strip()
            items.append({
                "id": make_id("dart", x["rcept_no"]),
                "source_type": "dart", "source_name": "DART", "market": "KR",
                "company": x.get("corp_name", ""), "code": x.get("stock_code", ""),
                "title": title,
                "posted_at": posted.isoformat(), "posted_precision": "date",
                "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={x['rcept_no']}",
                "text": f"{x.get('corp_name', '')} {title} {x.get('rm', '')}".strip(),
                "doc_id": x["rcept_no"],
            })
        if page >= int(j.get("total_page", 1)):
            break
        page += 1
    return items


# ---------- SEC EDGAR (미국) ----------
TITLE_RE = re.compile(r"^(?P<form>.+?) - (?P<co>.+?) \((?P<cik>\d+)\) \((?P<role>[^)]+)\)$")


def parse_edgar_atom(content, form_hint=""):
    root = ET.fromstring(content)
    out = []
    for e in root.findall("a:entry", ATOM):
        title = (e.findtext("a:title", default="", namespaces=ATOM) or "").strip()
        updated = (e.findtext("a:updated", default="", namespaces=ATOM) or "").strip()
        eid = (e.findtext("a:id", default="", namespaces=ATOM) or "").strip()
        link_el = e.find("a:link", ATOM)
        link = link_el.get("href") if link_el is not None else ""
        m = TITLE_RE.match(title)
        form, company, cik = (m.group("form"), m.group("co"), m.group("cik")) if m else (form_hint, title, "")
        try:
            posted = datetime.fromisoformat(updated)
        except ValueError:
            continue
        out.append({
            "id": make_id("edgar", eid, cik),
            "source_type": "edgar", "source_name": "SEC EDGAR", "market": "US",
            "company": company, "code": cik, "title": f"{form} {company}".strip(),
            "form": form,
            "posted_at": posted.astimezone(KST).isoformat(), "posted_precision": "minute",
            "url": link, "text": f"{company} {form}", "doc_id": eid,
        })
    return out


def fetch_edgar(cfg, collected_at):
    ua = cfg.get("sec_user_agent", "")
    if "@" not in ua:
        raise RuntimeError("SEC_USER_AGENT 에 이름과 이메일을 적어야 합니다 (예: 'Hong Gildong hong@example.com')")
    items = []
    for form in cfg["edgar_forms"]:
        r = http_get("https://www.sec.gov/cgi-bin/browse-edgar", params={
            "action": "getcurrent", "type": form, "company": "", "dateb": "",
            "owner": "include", "start": 0, "count": 100, "output": "atom"},
            headers={"User-Agent": ua, "Accept": "application/atom+xml"})
        items += parse_edgar_atom(r.content, form)
        time.sleep(0.3)  # SEC 권장 호출 한도(초당 10회)보다 훨씬 느리게
    return items


# ---------- EDINET (일본) ----------
def fetch_edinet(cfg, collected_at):
    key = cfg.get("edinet_api_key")
    if not key:
        raise RuntimeError("EDINET 인증키가 없습니다 (EDINET_API_KEY)")
    items = []
    for i in range(int(cfg["lookback_days"]) + 1):
        d = collected_at.date() - timedelta(days=i)
        r = http_get("https://api.edinet-fsa.go.jp/api/v2/documents.json",
                     params={"date": d.isoformat(), "type": 2, "Subscription-Key": key})
        for x in r.json().get("results", []):
            sdt = x.get("submitDateTime")
            if not sdt:
                continue
            posted = datetime.strptime(sdt, "%Y-%m-%d %H:%M").replace(tzinfo=JST)
            title = (x.get("docDescription") or "").strip()
            items.append({
                "id": make_id("edinet", x["docID"]),
                "source_type": "edinet", "source_name": "EDINET", "market": "JP",
                "company": x.get("filerName", ""), "code": x.get("secCode") or "",
                "title": title,
                "posted_at": posted.isoformat(), "posted_precision": "minute",
                "url": f"https://api.edinet-fsa.go.jp/api/v2/documents/{x['docID']}",  # 열람에는 인증키 필요
                "text": f"{x.get('filerName', '')} {title}".strip(), "doc_id": x["docID"],
            })
    return items


SOURCES = {"dart": fetch_dart, "edgar": fetch_edgar, "edinet": fetch_edinet}


# ---------- 실행 ----------
def finalize(item, collected_at, watch, keywords):
    item["collected_at"] = collected_at.isoformat()
    item["forwarded_from"] = None
    item["views"] = None
    item["linked_urls"] = [item["url"]] if item.get("url") else []
    if item["posted_precision"] == "minute":
        age = (collected_at - datetime.fromisoformat(item["posted_at"])).total_seconds() / 3600
        item["age_hours_at_collection"] = round(max(age, 0), 2)
    else:
        item["age_hours_at_collection"] = None  # 날짜만 제공되어 경과 시간을 알 수 없음
    item["flags"] = make_flags(item, watch, keywords)
    return item


def run(cfg, only=None, now=None, fetchers=None):
    fetchers = fetchers or SOURCES
    collected_at = now or now_kst()
    out_dir = cfg["out_dir"]
    os.makedirs(out_dir, exist_ok=True)
    state_path = os.path.join(out_dir, "_state.json")
    status_path = os.path.join(out_dir, "_status.json")
    state = load_json(state_path, {"seen": {}})
    prev = load_json(status_path, {"sources": {}})
    watch = load_watchlist(cfg.get("holdings_file"))

    # 오래된 id 정리
    cutoff = collected_at - timedelta(days=SEEN_TTL_DAYS)
    state["seen"] = {k: v for k, v in state["seen"].items() if datetime.fromisoformat(v) > cutoff}

    new_items, status = [], {"run_at": collected_at.isoformat(), "sources": {}}
    enabled = [n for n in fetchers if not only or n in only]
    for name in enabled:
        old = prev.get("sources", {}).get(name, {})
        try:
            items = fetchers[name](cfg, collected_at)
            fresh, batch_ids = [], set()
            for it in items:
                if it["id"] in state["seen"] or it["id"] in batch_ids:
                    continue
                batch_ids.add(it["id"])
                fresh.append(finalize(it, collected_at, watch, cfg["keywords"]))
            new_items += fresh
            status["sources"][name] = {"ok": True, "fetched": len(items), "new": len(fresh),
                                       "last_success": collected_at.isoformat(), "error": None}
        except Exception as e:  # 한 소스의 실패가 다른 소스를 막지 않게 한다
            status["sources"][name] = {"ok": False, "fetched": 0, "new": 0,
                                       "last_success": old.get("last_success"), "error": re.sub(r"(crtfc_key|Subscription-Key)=[^&\s]+", r"\1=***", str(e))[:300]}

    # 파일을 먼저 쓰고, 성공한 뒤에만 '본 공시' 기록을 갱신한다 (유실 방지)
    if new_items:
        new_items.sort(key=lambda x: x["posted_at"])
        name = f"feed_{collected_at.strftime('%Y%m%d_%H%M')}.jsonl"
        path = os.path.join(out_dir, name)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for it in new_items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")
        os.replace(tmp, path)
        for it in new_items:
            state["seen"][it["id"]] = collected_at.isoformat()
        status["file"] = name
    save_json_atomic(state_path, state)

    # 전체 last_success: 켜 둔 소스 중 가장 오래된 성공 시각 (하나라도 실패 이력이 없으면 None)
    times = [status["sources"][n]["last_success"] for n in enabled]
    status["last_success"] = None if (not times or any(t is None for t in times)) else min(times)
    status["new_total"] = len(new_items)
    save_json_atomic(status_path, status)
    return status


def prune_old_feeds(out_dir, days=14, now=None):
    """feed_*.jsonl 중 파일명 날짜가 오래된 것을 지운다 (저장소가 계속 커지지 않게)."""
    now = now or now_kst()
    cutoff = (now - timedelta(days=days)).strftime("%Y%m%d")
    removed = []
    for fn in os.listdir(out_dir):
        m = re.match(r"feed_.*?(\d{8})_\d{4}\.jsonl$", fn)
        if m and m.group(1) < cutoff:
            os.remove(os.path.join(out_dir, fn))
            removed.append(fn)
    return removed


def main():
    ap = argparse.ArgumentParser(description="공시 수집기")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--sources", default="", help="쉼표 구분: dart,edgar,edinet (기본: 모두)")
    args = ap.parse_args()
    cfg = load_config(args.config)
    only = [s.strip() for s in args.sources.split(",") if s.strip()]
    status = run(cfg, only or None)
    prune_old_feeds(cfg["out_dir"])
    print(json.dumps(status, ensure_ascii=False, indent=2))
    if any(not s["ok"] for s in status["sources"].values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
