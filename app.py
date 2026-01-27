from collections import OrderedDict
from typing import Any, Dict, Optional
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from db import RepoDB

app = FastAPI(title="Git Leaderboard", version="1.0", openapi_url=None, docs_url=None, redoc_url=None)

DB_PATH = "repos.db"
LEADERBOARD_PAGE_SIZE = 100
IS_PROD = True
db = RepoDB(DB_PATH)


class LRUCache:
    def __init__(self, max_size: int):
        self.max_size = max_size
        self.cache: OrderedDict[str, Any] = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

    def set(self, key: str, value: Any):
        self.cache[key] = value
        if len(self.cache) > self.max_size:
            self.cache.popitem(last=False)


response_cache = LRUCache(max_size=10_000)


@app.on_event("shutdown")
def _shutdown() -> None:
    db.close()


if not IS_PROD:
    app.mount("/static", StaticFiles(directory="static"), name="static")

    @app.get("/")
    def home() -> FileResponse:
        return FileResponse("static/index.html")


@app.get("/api/leaderboard")
def leaderboard(
    metric: str = Query("stars", description="stars|forks|watchers|diskUsage"),
    page: int = Query(1, ge=1),
    q: Optional[str] = Query(None, description="Search in name/description"),
    in_description: bool = Query(True),
    language: Optional[str] = None,
    topic: Optional[str] = None,
) -> Dict[str, Any]:
    cache_key = f"lb:{metric}:{page}:{q}:{in_description}:{language}:{topic}"
    if cached := response_cache.get(cache_key):
        return cached

    try:
        total = db.count_leaderboard(q=q, in_description=in_description, language=language, topic=topic)
        items = db.leaderboard(
            metric=metric,
            page=page,
            q=q,
            in_description=in_description,
            language=language,
            topic=topic,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    result = {
        "page": int(page),
        "total": total,
        "totalPages": (total + int(LEADERBOARD_PAGE_SIZE) - 1) // int(LEADERBOARD_PAGE_SIZE) if total > 0 else 1,
        "items": items,
    }

    response_cache.set(cache_key, result)
    return result


@app.get("/api/repo")
def repo_latest(name: str = Query(..., description="owner/repo")) -> Dict[str, Any]:
    cache_key = f"repo:{name}"
    if cached := response_cache.get(cache_key):
        return cached

    if not (result := db.get_repo_latest(name)):
        raise HTTPException(status_code=404, detail="Repo not found")

    response_cache.set(cache_key, result)
    return result


@app.get("/api/repo/history")
def repo_history(
    name: str = Query(..., description="owner/repo"),
) -> Dict[str, Any]:
    cache_key = f"hist:{name}"
    if cached := response_cache.get(cache_key):
        return cached

    segs = db.history_segments(name_with_owner=name, limit=2920)  # 2 years

    result = {"nameWithOwner": name, "segments": segs}
    response_cache.set(cache_key, result)
    return result


@app.get("/api/rank")
def shields_rank(name: str = Query(..., description="owner/repo")) -> Dict[str, Any]:
    cache_key = f"rank:{name}"
    if cached := response_cache.get(cache_key):
        return cached

    if not (rank := db.get_global_rank(name)):
        result = {"schemaVersion": 1, "label": "rank", "message": "repo not found", "color": "inactive"}
        response_cache.set(cache_key, result)
        return result

    color = "brightgreen" if rank <= 100 else "orange" if rank <= 1000 else "blue"

    result = {"schemaVersion": 1, "label": "global rank", "message": f"#{rank}", "color": color, "cacheSeconds": 3600}
    response_cache.set(cache_key, result)
    return result


@app.get("/{owner}/{repo}")
def repo_short_url(owner: str, repo: str) -> RedirectResponse:
    name = f"{owner}/{repo}"

    if not (rank := db.get_global_rank(name)):
        raise HTTPException(status_code=404, detail="Repo not found")

    page = ((rank - 1) // LEADERBOARD_PAGE_SIZE) + 1
    qs = f"page={page}&metric=stars&view=table&highlight={quote(name, safe='')}&open={quote(name, safe='')}"
    return RedirectResponse(url=f"/?{qs}", status_code=302)
