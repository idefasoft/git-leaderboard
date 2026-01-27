import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from typing import Callable, List

import requests

from db import RepoDB

TOKEN = "replace this"
MIN_STARS = 1_000
DB_PATH = "github_repos.db"
LIVE_DB_PATH = "repos.db"
PM2_APP_NAME = "git_leaderboard"
GRAPHQL_QUERY = """
query($queryString: String!, $cursor: String) {
  rateLimit {
    remaining
    resetAt
  }
  search(query: $queryString, type: REPOSITORY, first: 100, after: $cursor) {
    repositoryCount
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      ... on Repository {
        nameWithOwner
        stargazerCount
        forkCount
        description
        watchers { totalCount }
        homepageUrl
        createdAt
        updatedAt
        pushedAt
        isArchived
        diskUsage
        primaryLanguage {
          name
        }
        repositoryTopics(first: 20) {
          nodes {
            topic { name }
          }
        }
      }
    }
  }
}
"""


class GithubGraphQL:
    def __init__(self, token: str = None):
        self.base_url = "https://api.github.com/graphql"
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/vnd.github.v3+json"} | {"Authorization": f"Bearer {token}"} if token else {})

    def _handle_rate_limit(self, response: requests.Response) -> bool:
        if response.status_code == 403 and response.headers.get("X-RateLimit-Remaining") == "0":
            reset_time = response.headers.get("X-RateLimit-Reset")
            sleep_duration = int(reset_time) - time.time() + 5
            print(f"HTTP Rate limit hit. Sleeping for {sleep_duration:.2f} seconds.")
            time.sleep(max(1, sleep_duration))
            return True

        if response.status_code == 200:
            data = response.json()
            if "errors" in data:
                if any("RATE_LIMITED" in str(e) for e in data["errors"]):
                    print("GraphQL Rate limit error detected. Sleeping 60s.")
                    time.sleep(60)
                    return True

            if "data" in data and data["data"] and "rateLimit" in data["data"]:
                rl = data["data"]["rateLimit"]
                if rl["remaining"] < 10:
                    reset_date = datetime.strptime(rl["resetAt"], "%Y-%m-%dT%H:%M:%SZ")
                    sleep_duration = (reset_date - datetime.now(tz=timezone.utc)).total_seconds() + 5
                    print(f"GraphQL Remaining low ({rl['remaining']}). Sleeping for {sleep_duration:.2f}s.")
                    time.sleep(max(1, sleep_duration))

        return False

    def execute_query(self, query: str, cursor: str = None):
        payload = {"query": GRAPHQL_QUERY, "variables": {"queryString": query, "cursor": cursor}}

        attempts = 0
        while attempts < 10:
            try:
                response = self.session.post(self.base_url, json=payload, timeout=15)

                if self._handle_rate_limit(response):
                    continue  # Retry after sleeping

                if response.status_code == 200:
                    data = response.json()
                    if "errors" in data:
                        print(f"GraphQL Errors: {data['errors']}")
                        raise Exception("GraphQL returned errors")
                    return data["data"]

                elif 500 <= response.status_code <= 504:
                    print(f"Server Error {response.status_code}. Retrying...")
                    time.sleep(5 * (attempts + 1))
                    attempts += 1
                else:
                    raise Exception(f"Request failed with status {response.status_code}: {response.text}")

            except requests.exceptions.RequestException as e:
                print(f"Network error: {e}. Retrying...")
                time.sleep(5)
                attempts += 1

        raise Exception("Max retries exceeded.")


def deploy_site(crawled_db_path: str):
    print("Preparing deployment...")

    db = RepoDB(crawled_db_path)

    row = db.conn.execute("SELECT COUNT(*) AS cnt FROM repo_latest").fetchone()
    total_repos = int(row["cnt"]) if row else 0
    formatted_total = "{:,}".format(total_repos)

    lang_rows = db.conn.execute("SELECT name FROM language ORDER BY name LIMIT 5000").fetchall()
    languages = [str(r["name"]) for r in lang_rows]

    topic_sql = """
        SELECT t.name, COUNT(rtl.repo_id) AS cnt
        FROM topic t
        JOIN repo_topic_latest rtl ON rtl.topic_id = t.id
        GROUP BY t.id
        ORDER BY cnt DESC
        LIMIT 500
    """
    topic_rows = db.conn.execute(topic_sql).fetchall()
    topics = [{"name": str(r["name"]), "count": int(r["cnt"])} for r in topic_rows]

    db.close()

    print("Stopping PM2 service...")
    try:
        subprocess.run(["pm2", "stop", PM2_APP_NAME], check=False, stdout=subprocess.DEVNULL)
    except Exception as e:
        print(f"Warning: PM2 stop failed (might not be running): {e}")

    print("Swapping Database...")
    if os.path.exists(crawled_db_path):
        if os.path.exists(LIVE_DB_PATH):
            os.remove(LIVE_DB_PATH)
        shutil.copy2(crawled_db_path, LIVE_DB_PATH)

    index_path = "index.html"
    if os.path.exists(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        html_content = re.sub(r'(id="totalRepos"[^>]*>).*?(</\w+>)', f"\\g<1>{formatted_total}\\g<2>", html_content)

        with open(index_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"Updated {index_path} with {formatted_total} repos.")

    app_js_path = "static/app.js"
    if os.path.exists(app_js_path):
        with open(app_js_path, "r", encoding="utf-8") as f:
            js_content = f.read()

        static_data = {"languages": languages, "topics": topics}
        injection_code1 = f"const STATIC_DATA = {json.dumps(static_data)};"

        if "const STATIC_DATA =" in js_content:
            js_content = re.sub(r"const STATIC_DATA = \{.*?\};", injection_code1, js_content, flags=re.DOTALL)

        with open(app_js_path, "w", encoding="utf-8") as f:
            f.write(js_content)
        print(f"Updated {app_js_path} with static lists.")

    print("Restarting PM2 service...")
    try:
        subprocess.run(["pm2", "restart", PM2_APP_NAME], check=False, stdout=subprocess.DEVNULL)
    except Exception as e:
        print(f"Warning: PM2 restart failed (maybe not running): {e}")
    print("Deployment complete.")


def crawl():
    gh = GithubGraphQL(TOKEN)
    db = RepoDB(DB_PATH)
    current_min_stars = MIN_STARS
    total_fetched = 0

    print(f"Starting crawl for repos with >= {MIN_STARS} stars...")

    while True:
        search_query = f"stars:>={current_min_stars} sort:stars-asc"
        print(f"Querying batch: '{search_query}'")

        cursor = None
        batch_repos = []
        has_next_page = True

        #  Max 1000 results allowed by GitHub
        while has_next_page:
            time.sleep(0.1)
            data = gh.execute_query(search_query, cursor)
            search_data = data["search"]

            nodes = search_data["nodes"]
            if not nodes:
                break

            batch_repos.extend(nodes)
            total_fetched += len(nodes)

            print(f"  Fetched {len(nodes)} items. Total: {total_fetched}. Last star count: {nodes[-1]['stargazerCount']}")

            page_info = search_data["pageInfo"]
            has_next_page = page_info["hasNextPage"]
            cursor = page_info["endCursor"]

            if len(batch_repos) >= 1000:
                break

        if not batch_repos:
            print("No more results found.")
            break

        last_repo_stars = batch_repos[-1]["stargazerCount"]

        if last_repo_stars == current_min_stars:
            current_min_stars += 1
        else:
            current_min_stars = last_repo_stars
        db.upsert_from_github_nodes(batch_repos)
    db.close()
    deploy_site(DB_PATH)


def run_at_hours(func: Callable, hours_list: List[int]):
    print(f"Scheduler started for hours: {hours_list}")
    last_run_hour = -1

    while True:
        current_hour = datetime.now(tz=timezone.utc).hour

        if current_hour in hours_list and current_hour != last_run_hour:
            func()
            last_run_hour = current_hour

        if current_hour not in hours_list:
            last_run_hour = -1

        time.sleep(30)


if __name__ == "__main__":
    run_at_hours(crawl, [0, 6, 12, 18])
