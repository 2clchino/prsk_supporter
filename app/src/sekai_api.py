import re
import requests
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Union, Optional

BASE_URL = "https://api.sekai.best"
REGION = "jp"

_GH_BASE = "https://raw.githubusercontent.com/Sekai-World/sekai-master-db-diff/main/"
_CDN_BASE = "https://cdn.jsdelivr.net/gh/Sekai-World/sekai-master-db-diff@main/"

def _gh_urls(filename: str) -> list[str]:
    return [_GH_BASE + filename, _CDN_BASE + filename]

JST = timezone(timedelta(hours=9))

_cache: dict[str, tuple[float, Any]] = {}
_CACHE_TTL = 3600  # 1時間

def _fetch_json_cached(filename: str) -> Any:
    now = time.monotonic()
    if filename in _cache:
        ts, data = _cache[filename]
        if now - ts < _CACHE_TTL:
            return data

    last_exc = None
    for url in _gh_urls(filename):
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            _cache[filename] = (now, data)
            return data
        except Exception as e:
            last_exc = e
    raise last_exc

def fetch_event_list():
    data = _fetch_json_cached("events.json")
    if not isinstance(data, list):
        raise ValueError("events.json の中身がリストではありません。")
    return data

def list_event_names():
    events = fetch_event_list()
    event_names = [e["name"] for e in events if isinstance(e, dict) and "name" in e]
    print("\n".join(event_names))
    
def get_event_info_by_name(event_name):
    events = fetch_event_list()
    for evt in events:
        if not isinstance(evt, dict):
            continue
        if evt.get("name") == event_name:
            return evt
    raise ValueError(f"イベント名【{event_name}】が見つかりませんでした。")

def get_event_info_by_id(event_id: int):
    events = fetch_event_list()
    for evt in events:
        if not isinstance(evt, dict):
            continue
        if evt.get("id") == event_id:
            return evt
    raise ValueError(f"EventID [{event_id}] が見つかりませんでした。")

def filter_event_info(evt):
    return evt.get("id"), datetime.fromtimestamp(evt.get("startAt") / 1000, tz=JST), datetime.fromtimestamp(evt.get("aggregateAt") / 1000, tz=JST)

def get_event_time(event_id):
    url = f"{BASE_URL}/event/{event_id}/rankings/time"
    params = {
        "region": REGION
    }
    try:
        resp = requests.get(url, params=params, timeout=100)
        print("Status:", resp.status_code)
        print("URL:", resp.url)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", [])
        return data if isinstance(data, list) else []
    except Exception as e:
        print("Error:", e)
        return []
    
_CHARA_ID_TO_NAME = {
    1: "Ichika", 2: "Saki", 3: "Honami", 4: "Shiho",
    5: "Minori", 6: "Haruka", 7: "Airi", 8: "Shizuku",
    9: "Kohane", 10: "An", 11: "Akito", 12: "Toya",
    13: "Tsukasa", 14: "Emu", 15: "Nene", 16: "Rui",
    17: "Kanade", 18: "Mafuyu", 19: "Ena", 20: "Mizuki",
    21: "Miku", 22: "Rin", 23: "Len", 24: "Luka",
    25: "MEIKO", 26: "KAITO",
}

_CHARA_ID_TO_NAME_JA = {
    1: "一歌", 2: "咲希", 3: "穂波", 4: "志歩",
    5: "みのり", 6: "遥", 7: "愛莉", 8: "雫",
    9: "こはね", 10: "杏", 11: "冬弥", 12: "つかさ",
    13: "司", 14: "えむ", 15: "寧々", 16: "類",
    17: "奏", 18: "まふゆ", 19: "絵名", 20: "瑞希",
    21: "ミク", 22: "リン", 23: "レン", 24: "ルカ",
    25: "MEIKO", 26: "KAITO",
}

def _parse_sekai_ing_response(data: dict, chara_id: int = None) -> list:
    """Parse /api/events/now/leaderboard JSON into the common entry format."""
    if chara_id is None:
        entries = data.get("rankings") or []
    else:
        # Find chapter index by character_id in event.chapters
        chapters = (data.get("event") or {}).get("chapters") or []
        chapter_no = next(
            (c.get("num") for c in chapters if c.get("character_id") == chara_id),
            None,
        )
        if chapter_no is not None:
            chapter_list = data.get("chapter_rankings") or []
            idx = chapter_no - 1
            entries = chapter_list[idx] if idx < len(chapter_list) else []
        else:
            entries = []

    result = []
    for s in entries:
        rank = s.get("rank")
        name = s.get("name") or s.get("userName")
        user_hash = s.get("hash") or s.get("userId") or s.get("id")
        score_raw = s.get("score") or s.get("points")
        try:
            score = int(str(score_raw).replace(",", ""))
        except (TypeError, ValueError):
            score = None
        if rank is not None and name:
            result.append({"rank": rank, "score": score, "userName": name, "userId": user_hash})
    return result


def _get_leaderboard_sekai_ing(chara_id: int = None) -> list:
    # Playwright: wait for the XHR leaderboard response (includes user hashes)
    extract_js = """(bodyId) => {
        const tbody = document.querySelector('#' + bodyId);
        if (!tbody) return [];
        const results = [];
        let rowIndex = 0;
        for (const tr of tbody.querySelectorAll('tr')) {
            const tds = tr.querySelectorAll('td');
            if (tds.length < 3) continue;
            rowIndex++;
            const nameEl = tds[1].querySelector('.username-label');
            const name = nameEl ? nameEl.innerText.trim() : tds[1].innerText.trim();
            const score = parseInt(tds[2].innerText.trim().replace(/,/g, ''));
            if (name && !isNaN(score)) results.push({rank: rowIndex, name, score});
        }
        return results;
    }"""

    char_name_ja = _CHARA_ID_TO_NAME_JA.get(chara_id) if chara_id else None

    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # expect_response must wrap the navigation so it catches the deferred XHR
        try:
            with page.expect_response(
                lambda r: "/api/events/now/leaderboard" in r.url,
                timeout=25000,
            ) as response_info:
                page.goto("https://jp.seka.ing/", wait_until="domcontentloaded", timeout=30000)
            data = response_info.value.json()
            browser.close()
            return _parse_sekai_ing_response(data, chara_id)
        except Exception:
            pass

        # Last resort: DOM scraping (hash unavailable)
        body_id = "eventTableOverallBody"
        if char_name_ja:
            chapter_idx = page.evaluate(f"""() => {{
                const charName = {repr(char_name_ja)};
                for (const btn of document.querySelectorAll('button[data-bs-target]')) {{
                    if (btn.innerText.trim() === charName) {{
                        btn.click();
                        const m = (btn.getAttribute('data-bs-target') || '').match(/chapter[-]?(\\d+)/i);
                        return m ? m[1] : null;
                    }}
                }}
                return null;
            }}""")
            if chapter_idx:
                body_id = f"eventTableChapter{chapter_idx}Body"
                try:
                    page.wait_for_selector(f"#{body_id} tr", timeout=8000)
                except Exception:
                    page.wait_for_timeout(3000)

        rankings = page.evaluate(extract_js, body_id)
        browser.close()

    return [
        {"rank": e["rank"], "score": e["score"], "userName": e["name"], "userId": None}
        for e in rankings
    ]

def get_event_rankings(event_id, ts):
    url = f"{BASE_URL}/event/{event_id}/rankings"
    params = {"timestamp": ts, "region": REGION}
    try:
        resp = requests.get(url, params=params, timeout=100)
        print("Status:", resp.status_code)
        print("URL:", resp.url)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data")
        rankings = data.get("eventRankings")
        if not rankings:
            raise ValueError("empty eventRankings")
        return rankings
    except Exception as e:
        print("Error:", e)
    try:
        return _get_leaderboard_sekai_ing(chara_id=None)
    except Exception as e:
        print("Fallback Error:", e)
        return []

def fetch_world_bloom():
    data = _fetch_json_cached("worldBlooms.json")
    if not isinstance(data, list):
        raise ValueError("worldBlooms.json の中身がリストではありません。")
    return data

def get_chapter_info(event_id, chapter_no):
    items = fetch_world_bloom()
    return next(
        (o for o in items if o.get("eventId") == event_id and o.get("chapterNo") == chapter_no),
        None
    )
    
def filter_chapter_info(evt):
    return evt.get("gameCharacterId"), datetime.fromtimestamp(evt.get("chapterStartAt") / 1000, tz=JST), datetime.fromtimestamp(evt.get("aggregateAt") / 1000, tz=JST)
    
def get_chapter_time(event_id, chara_id):
    url = f"{BASE_URL}/event/{event_id}/chapter_rankings/time"
    params = {"charaId": chara_id, "region": REGION}
    try:
        resp = requests.get(url, params=params, timeout=100)
        print("Status:", resp.status_code)
        print("URL:", resp.url)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data", [])
        return data if isinstance(data, list) else []
    except Exception as e:
        print("Error:", e)
        return []
    
def get_chapter_rankings(event_id, chara_id, ts):
    url = f"{BASE_URL}/event/{event_id}/chapter_rankings"
    params = {"charaId": chara_id, "timestamp": ts, "region": REGION}
    try:
        resp = requests.get(url, params=params, timeout=100)
        print("Status:", resp.status_code)
        print("URL:", resp.url)
        resp.raise_for_status()
        payload = resp.json()
        data = payload.get("data")
        rankings = data.get("eventRankings")
        if not rankings:
            raise ValueError("empty eventRankings")
        return rankings
    except Exception as e:
        print("Error:", e)
    try:
        return _get_leaderboard_sekai_ing(chara_id=chara_id)
    except Exception as e:
        print("Fallback Error:", e)
        return []

def _entry_user_id(entry: Dict[str, Any]) -> Optional[str]:
    uid = entry.get("userId") or (entry.get("userProfile") or {}).get("userId")
    return str(uid) if uid is not None else None

def _entry_twitter_id(entry: Dict[str, Any]) -> Optional[str]:
    tid = (entry.get("userProfile") or {}).get("twitterId")
    return str(tid) if tid else None

def is_twitter_target(t: Any) -> bool:
    return isinstance(t, str) and t.startswith("twitterId:")

def _twitter_id_of(t: str) -> str:
    return t[len("twitterId:"):]

def extract_scores(rankings: List[Dict[str, Any]],
                   targets: List[Union[int, str]]) -> Dict[Union[int, str], Any]:
    result: Dict[Union[int, str], Any] = {}
    def is_userid_like(x: Any) -> bool:
        return isinstance(x, int) and len(str(abs(x))) >= 15
    for entry in rankings:
        rank_val = entry.get("rank")
        user_name = entry.get("userName")
        user_id_str = _entry_user_id(entry)
        twitter_id = _entry_twitter_id(entry)
        score_val = entry.get("score")
        try:
            score_val = int(score_val)
        except (TypeError, ValueError):
            score_val = None
        for t in targets:
            if is_twitter_target(t):
                if twitter_id and twitter_id == _twitter_id_of(t):
                    result[t] = score_val
            elif is_userid_like(t):
                if user_id_str is not None and user_id_str == str(t):
                    result[t] = score_val
            elif isinstance(t, int):
                if rank_val == t:
                    result[t] = score_val
            elif isinstance(t, str):
                if user_name == t:
                    result[t] = score_val
    return result

def extract_ranks(rankings: List[Dict[str, Any]],
                  targets: List[Union[int, str]]) -> Dict[Union[int, str], int]:
    result: Dict[Union[int, str], int] = {}
    def is_userid_like(x: Any) -> bool:
        return isinstance(x, int) and len(str(abs(x))) >= 15
    for entry in rankings:
        rank_val = entry.get("rank")
        user_name = entry.get("userName")
        user_id_str = _entry_user_id(entry)
        twitter_id = _entry_twitter_id(entry)
        for t in targets:
            if t in result:
                continue
            if is_twitter_target(t):
                if twitter_id and twitter_id == _twitter_id_of(t):
                    result[t] = rank_val
            elif is_userid_like(t):
                if user_id_str is not None and user_id_str == str(t):
                    result[t] = rank_val
            elif isinstance(t, int):
                if rank_val == t:
                    result[t] = rank_val
            elif isinstance(t, str):
                if user_name == t:
                    result[t] = rank_val
    return result

def resolve_display_names(rankings: List[Dict[str, Any]],
                          targets: List[Union[int, str]]) -> Dict[str, str]:
    """twitterId:xxx ターゲットを実際のユーザー名に解決する"""
    result: Dict[str, str] = {}
    twitter_targets = {_twitter_id_of(t): t for t in targets if is_twitter_target(t)}
    if not twitter_targets:
        return result
    for entry in rankings:
        tid = _entry_twitter_id(entry)
        if tid and tid in twitter_targets:
            key = twitter_targets[tid]
            result[key] = entry.get("userName", key)
    return result

def pick_hourly(times: List[str]) -> List[str]:
    dt_list = [datetime.fromisoformat(t.replace("Z", "+00:00")) for t in times]
    dt_list.sort()

    result = []
    if not dt_list:
        return result

    start = dt_list[0].replace(minute=0, second=0, microsecond=0)
    end = dt_list[-1]
    target = start

    while target <= end:
        nearest = min(dt_list, key=lambda d: abs((d - target).total_seconds()))
        result.append(nearest.isoformat().replace("+00:00", "Z"))
        target += timedelta(hours=1)

    return sorted(set(result), key=lambda x: x)

_SEKA_ING_URL = "https://jp.seka.ing/"

def parse_timeline(data: dict) -> Dict[datetime, int]:
    """/api/event-stats/<hash> の timeline を {分単位のJST時刻: ポイント} に変換する。
    (ユーザー詳細の Heatmap → Download as CSV で出力されるCSVと同じ元データ)"""
    result: Dict[datetime, int] = {}
    for item in data.get("timeline") or []:
        try:
            ts = datetime.fromisoformat(str(item["timestamp"]).replace("Z", "+00:00")).astimezone(JST)
            score = int(item["score"])
        except (KeyError, TypeError, ValueError):
            continue
        result[ts.replace(second=0, microsecond=0)] = score
    return result

# 並列に投げると 429 (Too many requests) になるため、1件ずつ間隔を空けて取得し、429 の場合は待って再試行する
_FETCH_TIMELINES_JS = """async (hashes) => {
    const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    const results = [];
    for (const h of hashes) {
        let data = null;
        for (let attempt = 0; attempt < 4; attempt++) {
            try {
                const r = await fetch('/api/event-stats/' + h);
                if (r.ok) { data = await r.json(); break; }
                if (r.status !== 429) break;
            } catch (e) {}
            await sleep(3000 * (attempt + 1));
        }
        results.push([h, data]);
        await sleep(500);
    }
    return results;
}"""

def fetch_minute_points(ranks: List[int] = (1, 2, 3, 4, 5), names: List[str] = ()) -> Dict[str, Any]:
    """jp.seka.ing からランキング(100位まで)と、指定順位・指定ユーザー名の1分毎のポイント推移を取得する。
    推移はユーザー詳細の Heatmap → Download as CSV と同じ元データ(/api/event-stats/<hash>)で、
    Cloudflare を通過したページ内から fetch する。ユーザー名はランキング100位以内から検索する
    (同名がいる場合は上位の方)。
    戻り値は {"leaderboard": [{"rank", "name", "hash", "score"}, ...],
              "timelines": {hash: {JST時刻(分単位): ポイント}}}。取得に失敗したユーザーは timelines に含まれない。"""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_context().new_page()
            with page.expect_response(lambda r: "/api/events/now/leaderboard" in r.url, timeout=30000) as resp_info:
                page.goto(_SEKA_ING_URL, wait_until="domcontentloaded", timeout=30000)
            leaderboard = [
                {"rank": e.get("rank"), "name": e.get("name"), "hash": e.get("hash"), "score": e.get("score")}
                for e in (resp_info.value.json().get("rankings") or []) if e.get("hash")
            ]
            rank_hashes = [e["hash"] for e in leaderboard if e["rank"] in ranks]
            name_hashes = [next((e["hash"] for e in leaderboard if e["name"] == n), None) for n in names]
            hashes = list(dict.fromkeys(rank_hashes + [h for h in name_hashes if h]))
            fetched = page.evaluate(_FETCH_TIMELINES_JS, hashes)
        finally:
            browser.close()
    timelines = {h: parse_timeline(d) for h, d in fetched if d}
    for h, d in fetched:
        if not d:
            print(f"[fetch_minute_points] event-stats {h} の取得に失敗しました")
    return {"leaderboard": leaderboard, "timelines": {h: t for h, t in timelines.items() if t}}
