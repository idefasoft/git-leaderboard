import json
import os
import re
import shutil
import subprocess
import time
from datetime import datetime, timedelta, timezone
from typing import List

import requests

from db import RepoDB

TOKEN = "replace this"

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
        databaseId
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


def log(*args, **kwargs):
    timestamp = datetime.now().strftime("[%d:%H:%S]")
    print(f"{timestamp}", *args, **kwargs)


class GithubGraphQL:
    def __init__(self, token: str = None):
        self.base_url = "https://api.github.com/graphql"
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/vnd.github.v3+json"} | {"Authorization": f"Bearer {token}"} if token else {})

    def _handle_rate_limit(self, response: requests.Response) -> bool:
        if response.status_code == 403 and response.headers.get("X-RateLimit-Remaining") == "0":
            reset_time = response.headers.get("X-RateLimit-Reset")
            sleep_duration = int(reset_time) - time.time() + 5
            log(f"HTTP Rate limit hit. Sleeping for {sleep_duration:.2f} seconds.")
            time.sleep(max(1, sleep_duration))
            return True

        if response.status_code == 200:
            data = response.json()
            if "errors" in data:
                if any("RATE_LIMITED" in str(e) for e in data["errors"]):
                    log("GraphQL Rate limit error detected. Sleeping 60s.")
                    time.sleep(60)
                    return True

            if "data" in data and data["data"] and "rateLimit" in data["data"]:
                rl = data["data"]["rateLimit"]
                if rl["remaining"] < 10:
                    reset_date = datetime.strptime(rl["resetAt"], "%Y-%m-%dT%H:%M:%SZ")
                    sleep_duration = (reset_date - datetime.now(tz=timezone.utc)).total_seconds() + 5
                    log(f"GraphQL Remaining low ({rl['remaining']}). Sleeping for {sleep_duration:.2f}s.")
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
                        log(f"GraphQL Errors: {data['errors']}")
                        raise Exception("GraphQL returned errors")
                    return data["data"]

                elif 500 <= response.status_code <= 504:
                    log(f"Server Error {response.status_code}. Retrying...")
                    time.sleep(5 * (attempts + 1))
                    attempts += 1
                else:
                    raise Exception(f"Request failed with status {response.status_code}: {response.text}")

            except requests.exceptions.RequestException as e:
                log(f"Network error: {e}. Retrying...")
                time.sleep(5)
                attempts += 1

        raise Exception("Max retries exceeded.")

class Crawler:
    def __init__(
        self,
        token: str,
        min_stars: int = 1_000,
        db_path: str = "github_repos.db",
        live_db_path: str = "repos.db",
        pm2_app_name: str = "git_leaderboard",
    ):
        self.token = token
        self.min_stars = min_stars
        self.db_path = db_path
        self.live_db_path = live_db_path
        self.pm2_app_name = pm2_app_name

        self.gh = GithubGraphQL(self.token)
        self.db = RepoDB(self.db_path)

        self.current_min_stars = self.min_stars
        self.total_fetched = 0
        # used by upsert logic to avoid duplicate processing in a run
        self._processed_repo_ids: set[int] = set()

    def log(self, *args, **kwargs):
        """Simple timestamped logger"""
        timestamp = datetime.now().strftime("[%d:%H:%S]")
        print(timestamp, *args, **kwargs)

    def deploy_site(self):
        self.log("Preparing deployment...")

        # reopen a fresh connection so we don't interfere with crawling
        db = RepoDB(self.db_path)

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

        self.log("Stopping PM2 service...")
        try:
            subprocess.run(["pm2", "stop", self.pm2_app_name], check=False, stdout=subprocess.DEVNULL)
        except Exception as e:
            self.log(f"Warning: PM2 stop failed (might not be running): {e}")

        self.log("Swapping Database...")
        if os.path.exists(self.db_path):
            if os.path.exists(self.live_db_path):
                os.remove(self.live_db_path)
            shutil.copy2(self.db_path, self.live_db_path)

        index_path = "index.html"
        if os.path.exists(index_path):
            with open(index_path, "r", encoding="utf-8") as f:
                html_content = f.read()

            html_content = re.sub(r'(id="totalRepos"[^>]*>).*?(</\w+>)', f"\\g<1>{formatted_total}\\g<2>", html_content)

            with open(index_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            self.log(f"Updated {index_path} with {formatted_total} repos.")

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
            self.log(f"Updated {app_js_path} with static lists.")

        self.log("Restarting PM2 service...")
        try:
            subprocess.run(["pm2", "restart", self.pm2_app_name], check=False, stdout=subprocess.DEVNULL)
        except Exception as e:
            self.log(f"Warning: PM2 restart failed (maybe not running): {e}")
        self.log("Deployment complete.")
        
    def run_at_hours(self, hours: List[int]):
        self.log(f"Starting crawler. Will run at hours: {hours}")
        while True:
            now = datetime.now()
            if now.hour in hours:
                self.log("Starting crawl cycle...")
                try:
                    self.crawl_and_update()
                    self.deploy_site()
                except Exception as e:
                    self.log(f"Error during crawl/deploy: {e}")
                self.log("Cycle complete. Sleeping for 1 hour.")
                time.sleep(3600)
            else:
                next_run = min((h for h in hours if h > now.hour), default=hours[0] + 24)
                next_run_time = now.replace(hour=next_run % 24, minute=0, second=0, microsecond=0)
                if next_run_time <= now:
                    next_run_time += timedelta(days=1)
                sleep_seconds = (next_run_time - now).total_seconds()
                self.log(f"Current hour {now.hour} not in target hours. Sleeping for {sleep_seconds/3600:.2f} hours until {next_run_time}.")
                time.sleep(sleep_seconds)


if __name__ == "__main__":
    crawler = Crawler(TOKEN)
    crawler.run_at_hours([0, 6, 12, 18])