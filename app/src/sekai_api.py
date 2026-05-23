import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Union

BASE_URL = "https://api.sekai.best"
REGION = "jp"
EVENTS_JSON_URL = (
    "https://raw.githubusercontent.com/Sekai-World/sekai-master-db-diff/"
    "main/events.json"
)
WORLD_BLOOM_JSON_URL = (
    "https://raw.githubusercontent.com/Sekai-World/sekai-master-db-diff/"
    "main/worldBlooms.json"
)
JST = timezone(timedelta(hours=9))

def fetch_event_list():
    resp = requests.get(EVENTS_JSON_URL)
    resp.raise_for_status()
    data = resp.json()
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
        slots = data.get("slots") or data.get("rankings") or data.get("leaderboard") or []
    else:
        # Find chapter number by gameCharacterId
        chapters = (data.get("event") or {}).get("chapters") or data.get("chapters") or []
        chapter_no = next(
            (c.get("chapterNo") or c.get("no") for c in chapters
             if c.get("gameCharacterId") == chara_id),
            None,
        )
        if chapter_no is not None:
            slots = (data.get(f"chapter{chapter_no}")
                     or data.get(f"slots_chapter{chapter_no}")
                     or data.get("slots") or [])
        else:
            slots = data.get("slots") or []

    result = []
    for s in slots:
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
    # Playwright: intercept XHR to get full response including hashes
    char_name_ja = _CHARA_ID_TO_NAME_JA.get(chara_id) if chara_id else None
    captured: dict = {}

    def _on_response(response):
        if "/api/events/now/leaderboard" in response.url and "data" not in captured:
            try:
                captured["data"] = response.json()
            except Exception:
                pass

    extract_js = """(bodyId) => {
        const tbody = document.querySelector('#' + bodyId);
        if (!tbody) return [];
        const results = [];
        for (const tr of tbody.querySelectorAll('tr')) {
            const tds = tr.querySelectorAll('td');
            if (tds.length < 3) continue;
            let rankText = tds[0].innerText.trim();
            if (!rankText) {
                const el = tds[0].querySelector('[aria-label]');
                rankText = el ? el.getAttribute('aria-label') : '';
            }
            const rank = parseInt(rankText.replace(/,/g, ''));
            const nameEl = tds[1].querySelector('.username-label');
            const name = nameEl ? nameEl.innerText.trim() : tds[1].innerText.trim();
            const score = parseInt(tds[2].innerText.trim().replace(/,/g, ''));
            if (!isNaN(rank) && name && !isNaN(score)) results.push({rank, name, score});
        }
        return results;
    }"""

    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.on("response", _on_response)
        page.goto("https://sekai.ing/", wait_until="domcontentloaded", timeout=30000)
        try:
            page.wait_for_selector("#eventTableOverallBody tr", timeout=15000)
        except Exception:
            page.wait_for_timeout(8000)

        # If XHR was captured, parse it directly (includes hashes)
        if captured.get("data"):
            browser.close()
            return _parse_sekai_ing_response(captured["data"], chara_id)

        # Last resort: DOM scraping (hash unavailable)
        body_id = "eventTableOverallBody"
        if char_name_ja:
            chapter_idx = page.evaluate(f"""() => {{
                const charName = {repr(char_name_ja)};
                for (const btn of document.querySelectorAll('button[data-bs-target]')) {{
                    if (btn.innerText.trim() === charName) {{
                        btn.click();
                        const m = (btn.getAttribute('data-bs-target') || '').match(/chapter(\\d+)/i);
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
        return data.get("eventRankings")
    except Exception as e:
        print("Error:", e)
    try:
        return _get_leaderboard_sekai_ing(chara_id=None)
    except Exception as e:
        print("Fallback Error:", e)
        return []

def fetch_world_bloom():
    resp = requests.get(WORLD_BLOOM_JSON_URL)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError("events.json の中身がリストではありません。")
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
        return data.get("eventRankings")
    except Exception as e:
        print("Error:", e)
    try:
        return _get_leaderboard_sekai_ing(chara_id=chara_id)
    except Exception as e:
        print("Fallback Error:", e)
        return []

def extract_scores(rankings: List[Dict[str, Any]],
                   targets: List[Union[int, str]]) -> Dict[Union[int, str], Any]:
    result: Dict[Union[int, str], Any] = {}
    def is_userid_like(x: Any) -> bool:
        return isinstance(x, int) and len(str(abs(x))) >= 15
    for entry in rankings:
        rank_val = entry.get("rank")
        user_name = entry.get("userName")
        user_id   = entry.get("userId")
        user_id_str = str(user_id) if user_id is not None else None
        score_val = entry.get("score")
        try:
            score_val = int(score_val)
        except (TypeError, ValueError):
            score_val = None
        for t in targets:
            if is_userid_like(t):
                if user_id_str is not None and user_id_str == str(t):
                    result[t] = score_val
            elif isinstance(t, int):
                if rank_val == t:
                    result[t] = score_val
            elif isinstance(t, str):
                if user_name == t:
                    result[t] = score_val

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