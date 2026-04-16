import json
import os
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

OWNER = os.environ["REPO_OWNER"]
REPO = os.environ["REPO_NAME"]
TOKEN = os.environ["GITHUB_TOKEN"]

API_BASE = f"https://api.github.com/repos/{OWNER}/{REPO}/traffic"
OUT_DIR = Path("traffic_history")
OUT_DIR.mkdir(exist_ok=True)

VIEWS_FILE = OUT_DIR / "views_history.json"
CLONES_FILE = OUT_DIR / "clones_history.json"
SNAPSHOT_FILE = OUT_DIR / "latest_snapshot.json"


from urllib.error import HTTPError

def github_get(url: str):
    req = Request(url)
    req.add_header("Authorization", f"Bearer {TOKEN}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    try:
        with urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        print("HTTP ERROR:", e.code, e.reason)
        print("URL:", url)
        print("X-Accepted-GitHub-Permissions:", e.headers.get("X-Accepted-GitHub-Permissions"))
        print("X-OAuth-Scopes:", e.headers.get("X-OAuth-Scopes"))
        print("X-Accepted-OAuth-Scopes:", e.headers.get("X-Accepted-OAuth-Scopes"))
        body = e.read().decode("utf-8", errors="ignore")
        print("Response body:", body)
        raise


def load_json(path: Path):
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return []


def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def merge_by_timestamp(old_items, new_items):
    merged = {item["timestamp"]: item for item in old_items}
    for item in new_items:
        merged[item["timestamp"]] = item
    return [merged[k] for k in sorted(merged.keys())]


def main():
    views = github_get(f"{API_BASE}/views")
    clones = github_get(f"{API_BASE}/clones")

    old_views = load_json(VIEWS_FILE)
    old_clones = load_json(CLONES_FILE)

    merged_views = merge_by_timestamp(old_views, views.get("views", []))
    merged_clones = merge_by_timestamp(old_clones, clones.get("clones", []))

    save_json(VIEWS_FILE, merged_views)
    save_json(CLONES_FILE, merged_clones)

    snapshot = {
        "saved_at_utc": datetime.now(timezone.utc).isoformat(),
        "repo": f"{OWNER}/{REPO}",
        "latest_14d_views_count": views.get("count", 0),
        "latest_14d_views_uniques": views.get("uniques", 0),
        "latest_14d_clones_count": clones.get("count", 0),
        "latest_14d_clones_uniques": clones.get("uniques", 0),
        "historical_views_total": sum(item.get("count", 0) for item in merged_views),
        "historical_views_uniques_naive_sum": sum(item.get("uniques", 0) for item in merged_views),
        "historical_clones_total": sum(item.get("count", 0) for item in merged_clones),
        "historical_clones_uniques_naive_sum": sum(item.get("uniques", 0) for item in merged_clones),
    }
    save_json(SNAPSHOT_FILE, snapshot)

    print("Traffic data saved.")
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
