#!/usr/bin/env python3
"""텔레그램 공개 미리보기(t.me/s/채널) 수집기.

로그인 없이 공개 채널의 최근 게시물, 게시 시각, 조회수를 읽는다. 비공개 채널은 읽을 수 없다.
결과: <out_dir>/feed_tg_YYYYMMDD_HHMM.jsonl (새 게시물만), telegram_snapshot.json (최근 48시간 게시물의 현재 조회수),
      _status_telegram.json, _state_telegram.json
사이트 이용약관과 접속 빈도를 지키는 개인용 수집 용도로만 쓴다.
"""
import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

import collector as c

UA = "Mozilla/5.0 (compatible; personal-feed-reader/1.0)"


def parse_views(s):
    s = (s or "").strip().upper().replace(",", "")
    m = re.fullmatch(r"([\d.]+)([KM]?)", s)
    if not m:
        return None
    return int(float(m.group(1)) * {"": 1, "K": 1000, "M": 1000000}[m.group(2)])


def parse_channel_html(html, channel):
    soup = BeautifulSoup(html, "html.parser")
    posts = []
    for el in soup.select("div.tgme_widget_message[data-post]"):
        data_post = el["data-post"]  # 예: channel/12345
        date_a = el.select_one("a.tgme_widget_message_date")
        time_el = date_a.select_one("time") if date_a else None
        if not time_el or not time_el.get("datetime"):
            continue
        try:
            posted = datetime.fromisoformat(time_el["datetime"]).astimezone(c.KST)
        except ValueError:
            continue
        text_el = el.select_one(".tgme_widget_message_text")
        text = text_el.get_text("\n", strip=True) if text_el else ""
        views_el = el.select_one(".tgme_widget_message_views")
        fwd = el.select_one(".tgme_widget_message_forwarded_from_name")
        links = [a["href"] for a in (text_el.select("a[href]") if text_el else [])
                 if a.get("href", "").startswith("http")]
        posts.append({
            "channel": channel, "post_id": data_post.split("/")[-1],
            "posted_at": posted.isoformat(),
            "url": date_a.get("href") or f"https://t.me/{data_post}",
            "text": text, "views": parse_views(views_el.get_text() if views_el else ""),
            "forwarded_from": fwd.get_text(strip=True) if fwd else None,
            "linked_urls": links,
        })
    return posts


def fetch_channel(channel):
    r = requests.get(f"https://t.me/s/{channel}", headers={"User-Agent": UA}, timeout=20)
    r.raise_for_status()
    posts = parse_channel_html(r.text, channel)
    if not posts:
        raise RuntimeError("게시물을 읽지 못했습니다(비공개이거나 미리보기가 막힌 채널일 수 있음)")
    return posts


def run(cfg, fetch=None, now=None, sleep=time.sleep):
    fetch = fetch or fetch_channel
    now = now or c.now_kst()
    out_dir = cfg["out_dir"]
    os.makedirs(out_dir, exist_ok=True)
    state_path = os.path.join(out_dir, "_state_telegram.json")
    state = c.load_json(state_path, {"seen": {}})
    watch = c.load_watchlist(cfg.get("holdings_file"))
    lookback = timedelta(hours=int(cfg.get("telegram_lookback_hours", 48)))
    cutoff = now - lookback
    state["seen"] = {k: v for k, v in state["seen"].items()
                     if datetime.fromisoformat(v) > now - timedelta(days=c.SEEN_TTL_DAYS)}

    new_items, snapshot, status = [], [], {"run_at": now.isoformat(), "channels": {}}
    prev = c.load_json(os.path.join(out_dir, "_status_telegram.json"), {"channels": {}})
    channels = cfg.get("telegram_channels", [])
    for i, ch in enumerate(channels):
        cid, name = ch["id"], ch.get("name", ch["id"])
        old = prev.get("channels", {}).get(cid, {})
        try:
            posts = fetch(cid)
            fresh = 0
            for p in posts:
                posted = datetime.fromisoformat(p["posted_at"])
                if posted < cutoff:
                    continue
                snapshot.append({"channel": cid, "name": name, "post_id": p["post_id"],
                                 "posted_at": p["posted_at"], "views": p["views"],
                                 "url": p["url"], "text": p["text"][:300]})
                pid = c.make_id("telegram", cid, p["post_id"])
                if pid in state["seen"]:
                    continue
                age = round(max((now - posted).total_seconds() / 3600, 0), 2)
                flags = [f"watchlist:{w['name']}({w['kind']})" for w in watch
                         if w["name"] in p["text"] or (w["market"] == "KR" and w["code"] in p["text"])]
                new_items.append({
                    "id": pid, "source_type": "telegram", "source_name": name, "channel": cid,
                    "posted_at": p["posted_at"], "posted_precision": "minute",
                    "collected_at": now.isoformat(), "age_hours_at_collection": age,
                    "url": p["url"], "forwarded_from": p["forwarded_from"], "views": p["views"],
                    "text": p["text"], "linked_urls": p["linked_urls"], "flags": flags,
                    "source_grade": "D",
                })
                fresh += 1
            status["channels"][cid] = {"ok": True, "posts": len(posts), "new": fresh,
                                       "last_success": now.isoformat(), "error": None}
        except Exception as e:  # 한 채널의 실패가 다른 채널을 막지 않게 한다
            status["channels"][cid] = {"ok": False, "posts": 0, "new": 0,
                                       "last_success": old.get("last_success"), "error": str(e)[:200]}
        if i < len(channels) - 1:
            sleep(2)  # 천천히 접속한다

    if new_items:
        new_items.sort(key=lambda x: x["posted_at"])
        fname = f"feed_tg_{now.strftime('%Y%m%d_%H%M')}.jsonl"
        path = os.path.join(out_dir, fname)
        with open(path + ".tmp", "w", encoding="utf-8") as f:
            for it in new_items:
                f.write(json.dumps(it, ensure_ascii=False) + "\n")
        os.replace(path + ".tmp", path)
        for it in new_items:
            state["seen"][it["id"]] = now.isoformat()
        status["file"] = fname
    c.save_json_atomic(state_path, state)
    snapshot.sort(key=lambda x: x["posted_at"], reverse=True)
    c.save_json_atomic(os.path.join(out_dir, "telegram_snapshot.json"),
                       {"collected_at": now.isoformat(), "posts": snapshot})
    times = [v["last_success"] for v in status["channels"].values()]
    status["last_success"] = None if (not times or any(t is None for t in times)) else min(times)
    status["new_total"] = len(new_items)
    c.save_json_atomic(os.path.join(out_dir, "_status_telegram.json"), status)
    return status


def main():
    ap = argparse.ArgumentParser(description="텔레그램 공개 미리보기 수집기")
    ap.add_argument("--config", default="config.json")
    args = ap.parse_args()
    cfg = c.load_config(args.config)
    status = run(cfg)
    c.prune_old_feeds(cfg["out_dir"])
    print(json.dumps({k: v for k, v in status.items() if k != "channels"}, ensure_ascii=False))
    bad = {k: v["error"] for k, v in status["channels"].items() if not v["ok"]}
    if bad:
        print("실패한 채널:", json.dumps(bad, ensure_ascii=False))
        sys.exit(1)


if __name__ == "__main__":
    main()
