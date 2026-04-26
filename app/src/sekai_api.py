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

def _get_leaderboard_sekai_run(chara_id: int = None) -> list:
    card_title = _CHARA_ID_TO_NAME.get(chara_id) if chara_id else "Overall"
    js = f"""() => {{
        const title = {repr(card_title)};
        const card = [...document.querySelectorAll('.card')].find(
            c => c.querySelector('h3')?.innerText.trim() === title
        );
        if (!card) return [];
        const results = [];
        for (const tr of card.querySelectorAll('tr:has(th.rank)')) {{
            const rank = parseInt(tr.querySelector('th.rank').innerText.trim());
            const tds = tr.querySelectorAll('td');
            const name = tds[1] ? tds[1].innerText.trim() : '';
            const scoreRaw = tds[2] ? tds[2].innerText.trim() : '';
            const score = parseInt(scoreRaw.replace(/,/g, ''));
            if (!isNaN(rank) && name && !isNaN(score)) results.push({{rank, name, score}});
        }}
        return results;
    }}"""
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto('https://sekai.run/', wait_until='domcontentloaded', timeout=30000)
        page.wait_for_timeout(8000)
        rankings = page.evaluate(js)
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
        return _get_leaderboard_sekai_run(chara_id=None)
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
        return _get_leaderboard_sekai_run(chara_id=chara_id)
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