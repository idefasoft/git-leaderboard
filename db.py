import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


def iso_to_unix(ts: Optional[str]) -> Optional[int]:
    if not ts:
        return None
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return int(dt.timestamp())


def unix_to_iso(ts: Optional[int]) -> Optional[str]:
    if ts is None:
        return None
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def chunks(seq: Sequence[Any], n: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def row_to_obj(row: sqlite3.Row) -> Dict[str, Any]:
    topics_concat = row["topicsConcat"]
    topics = [] if topics_concat is None else str(topics_concat).split("\x1f")
    topics = [t for t in topics if t]

    res = {
        "n": row["nameWithOwner"],
        "g": None if row["globalRank"] is None else int(row["globalRank"]),
        "s": int(row["stargazerCount"]),
        "f": int(row["forkCount"]),
        "w": int(row["watchersCount"]),
        "d": None if row["diskUsage"] is None else int(row["diskUsage"]),
        "a": row["description"],
        "h": row["homepageUrl"],
        "c": unix_to_iso(row["createdAtUnix"]),
        # "u": unix_to_iso(row["updatedAtUnix"]),
        "p": unix_to_iso(row["pushedAtUnix"]),
        "i": bool(int(row["isArchived"])),
        "l": row["primaryLanguage"],
        "t": topics,
        # "x": unix_to_iso(row["fetchedAtUnix"]),
    }
    if "newStars" in row.keys() and row["newStars"]:
        res["ns"] = int(row["newStars"])
    return res


def select_latest_base_sql(include_global_rank: bool = True, extra_select: str = "") -> str:
    rank_select = "gr.globalRank     AS globalRank," if include_global_rank else "NULL            AS globalRank,"
    rank_join = (
        """
        JOIN (
            SELECT
                rl2.repo_id AS repo_id,
                ROW_NUMBER() OVER (ORDER BY rl2.stars DESC, r2.name_with_owner ASC) AS globalRank
            FROM repo_latest rl2
            JOIN repo r2 ON r2.id = rl2.repo_id
        ) gr ON gr.repo_id = rl.repo_id
        """
        if include_global_rank
        else ""
    )
    return f"""
        SELECT
            r.name_with_owner AS nameWithOwner,
            {rank_select}
            rl.stars          AS stargazerCount,
            {extra_select}
            rl.forks          AS forkCount,
            rl.watchers       AS watchersCount,
            rl.disk_usage     AS diskUsage,

            r.description     AS description,
            r.homepage_url    AS homepageUrl,
            r.created_at      AS createdAtUnix,

            rl.updated_at     AS updatedAtUnix,
            rl.pushed_at      AS pushedAtUnix,
            rl.is_archived    AS isArchived,

            lang.name         AS primaryLanguage,
            GROUP_CONCAT(t.name, char(31)) AS topicsConcat,

            fr.fetched_at     AS fetchedAtUnix
        FROM repo_latest rl
        JOIN repo r ON r.id = rl.repo_id
        {rank_join}
        JOIN fetch_run fr ON fr.id = rl.run_id
        LEFT JOIN language lang ON lang.id = rl.primary_language_id
        LEFT JOIN repo_topic_latest rtl ON rtl.repo_id = rl.repo_id
        LEFT JOIN topic t ON t.id = rtl.topic_id
    """


def count_base_sql() -> str:
    return """
        SELECT COUNT(DISTINCT rl.repo_id) AS cnt
        FROM repo_latest rl
        JOIN repo r ON r.id = rl.repo_id
        LEFT JOIN language lang ON lang.id = rl.primary_language_id
    """


class RepoDB:
    def __init__(self, path: str) -> None:
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._apply_pragmas()
        self._create_schema()

        self._run_id: Optional[int] = None
        self._lang_cache: Dict[str, int] = {}
        self._topic_cache: Dict[str, int] = {}

        self._processed_repo_ids: set[int] = set()

    def close(self) -> None:
        self.conn.close()

    def _apply_pragmas(self) -> None:
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.execute("PRAGMA journal_mode = WAL;")
        self.conn.execute("PRAGMA synchronous = NORMAL;")
        self.conn.execute("PRAGMA temp_store = MEMORY;")

    def _create_schema(self) -> None:
        with self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS repo (
                    id              INTEGER PRIMARY KEY,
                    name_with_owner TEXT NOT NULL UNIQUE,
                    description     TEXT,
                    homepage_url    TEXT,
                    created_at      INTEGER
                );

                CREATE TABLE IF NOT EXISTS language (
                    id   INTEGER PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE
                );

                CREATE TABLE IF NOT EXISTS topic (
                    id   INTEGER PRIMARY KEY,
                    name TEXT NOT NULL UNIQUE
                );

                CREATE TABLE IF NOT EXISTS fetch_run (
                    id         INTEGER PRIMARY KEY,
                    fetched_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS repo_latest (
                    repo_id              INTEGER PRIMARY KEY,
                    run_id               INTEGER NOT NULL,
                    history_start_run_id INTEGER NOT NULL,

                    stars      INTEGER NOT NULL,
                    forks      INTEGER NOT NULL,
                    watchers   INTEGER NOT NULL,
                    disk_usage INTEGER,

                    updated_at  INTEGER,
                    pushed_at   INTEGER,
                    is_archived INTEGER NOT NULL,

                    primary_language_id INTEGER,

                    FOREIGN KEY(repo_id) REFERENCES repo(id) ON DELETE CASCADE,
                    FOREIGN KEY(run_id) REFERENCES fetch_run(id) ON DELETE CASCADE,
                    FOREIGN KEY(history_start_run_id) REFERENCES fetch_run(id) ON DELETE CASCADE,
                    FOREIGN KEY(primary_language_id) REFERENCES language(id)
                );

                CREATE TABLE IF NOT EXISTS repo_metrics_hist (
                    repo_id      INTEGER NOT NULL,
                    start_run_id INTEGER NOT NULL,
                    end_run_id   INTEGER NOT NULL,

                    stars      INTEGER NOT NULL,
                    forks      INTEGER NOT NULL,
                    watchers   INTEGER NOT NULL,
                    disk_usage INTEGER,

                    PRIMARY KEY (repo_id, start_run_id),
                    FOREIGN KEY(repo_id) REFERENCES repo(id) ON DELETE CASCADE,
                    FOREIGN KEY(start_run_id) REFERENCES fetch_run(id) ON DELETE CASCADE,
                    FOREIGN KEY(end_run_id) REFERENCES fetch_run(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS repo_topic_latest (
                    repo_id  INTEGER NOT NULL,
                    topic_id INTEGER NOT NULL,
                    PRIMARY KEY (repo_id, topic_id),
                    FOREIGN KEY(repo_id) REFERENCES repo(id) ON DELETE CASCADE,
                    FOREIGN KEY(topic_id) REFERENCES topic(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_repo_name ON repo(name_with_owner);

                CREATE INDEX IF NOT EXISTS idx_repo_latest_stars ON repo_latest(stars DESC);
                CREATE INDEX IF NOT EXISTS idx_repo_latest_forks ON repo_latest(forks DESC);
                CREATE INDEX IF NOT EXISTS idx_repo_latest_watchers ON repo_latest(watchers DESC);
                CREATE INDEX IF NOT EXISTS idx_repo_latest_disk ON repo_latest(disk_usage DESC);

                CREATE INDEX IF NOT EXISTS idx_hist_repo_end ON repo_metrics_hist(repo_id, end_run_id);

                CREATE INDEX IF NOT EXISTS idx_topic_name ON topic(name);
                CREATE INDEX IF NOT EXISTS idx_repo_topic_topic ON repo_topic_latest(topic_id);
                """
            )

    def begin_run(self, fetched_at_unix: Optional[int] = None) -> int:
        if self._run_id is not None:
            return self._run_id
        ts = fetched_at_unix if fetched_at_unix is not None else int(datetime.now(tz=timezone.utc).timestamp())
        with self.conn:
            cur = self.conn.execute("INSERT INTO fetch_run(fetched_at) VALUES (?)", (ts,))
            self._run_id = int(cur.lastrowid)
        return self._run_id

    def _get_or_create_language_id(self, name: Optional[str]) -> Optional[int]:
        if not name:
            return None
        if name in self._lang_cache:
            return self._lang_cache[name]
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO language(name) VALUES (?)", (name,))
            row = self.conn.execute("SELECT id FROM language WHERE name = ?", (name,)).fetchone()
        if row is None:
            return None
        lang_id = int(row["id"])
        self._lang_cache[name] = lang_id
        return lang_id

    def _get_or_create_topic_id(self, name: str) -> int:
        if name in self._topic_cache:
            return self._topic_cache[name]
        with self.conn:
            self.conn.execute("INSERT OR IGNORE INTO topic(name) VALUES (?)", (name,))
            row = self.conn.execute("SELECT id FROM topic WHERE name = ?", (name,)).fetchone()
        if row is None:
            raise RuntimeError(f"Failed to create/fetch topic id for: {name}")
        topic_id = int(row["id"])
        self._topic_cache[name] = topic_id
        return topic_id

    def _fetch_repo_ids(self, names: List[str]) -> Dict[str, int]:
        out: Dict[str, int] = {}
        if not names:
            return out
        for chunk in chunks(names, 500):
            q = "SELECT id, name_with_owner FROM repo WHERE name_with_owner IN ({})".format(",".join(["?"] * len(chunk)))
            for row in self.conn.execute(q, tuple(chunk)).fetchall():
                out[str(row["name_with_owner"])] = int(row["id"])
        return out

    def _fetch_latest_metrics(self, repo_ids: List[int]) -> Dict[int, sqlite3.Row]:
        out: Dict[int, sqlite3.Row] = {}
        if not repo_ids:
            return out
        for chunk in chunks(repo_ids, 500):
            q = "SELECT repo_id, history_start_run_id, stars, forks, watchers, disk_usage FROM repo_latest WHERE repo_id IN ({})".format(",".join(["?"] * len(chunk)))
            for row in self.conn.execute(q, tuple(chunk)).fetchall():
                out[int(row["repo_id"])] = row
        return out

    def _base_run_id_for_window(self, window_seconds: int) -> int:
        row = self.conn.execute("SELECT MAX(fetched_at) AS mx FROM fetch_run").fetchone()
        if row is None or row["mx"] is None:
            return 0
        now_ts = int(row["mx"])
        cutoff = now_ts - int(window_seconds)

        row2 = self.conn.execute("SELECT MAX(id) AS base_id FROM fetch_run WHERE fetched_at <= ?", (cutoff,)).fetchone()
        return 0 if row2 is None or row2["base_id"] is None else int(row2["base_id"])

    def upsert_from_github_nodes(self, nodes: List[Dict[str, Any]]) -> None:
        run_id = self.begin_run()

        fresh_nodes: List[Dict[str, Any]] = []
        for n in nodes:
            repo_id = n.get("databaseId")
            if not isinstance(repo_id, int) or repo_id <= 0:
                continue
            if repo_id in self._processed_repo_ids:
                continue
            self._processed_repo_ids.add(repo_id)
            fresh_nodes.append(n)

        if not fresh_nodes:
            return

        repo_rows: List[Tuple[int, str, Optional[int], Optional[str], Optional[str]]] = []
        repo_ids: List[int] = []

        for n in fresh_nodes:
            repo_id = int(n["databaseId"])
            name = n["nameWithOwner"]
            repo_ids.append(repo_id)
            repo_rows.append((repo_id, name, iso_to_unix(n.get("createdAt")), n.get("description"), n.get("homepageUrl")))

        with self.conn:
            conflict_params = [(row[1], row[0]) for row in repo_rows]

            self.conn.executemany(
                """
                DELETE FROM repo_latest
                WHERE repo_id IN (SELECT id FROM repo WHERE name_with_owner = ? AND id != ?)
                """,
                conflict_params,
            )
            self.conn.executemany(
                """
                DELETE FROM repo_topic_latest
                WHERE repo_id IN (SELECT id FROM repo WHERE name_with_owner = ? AND id != ?)
                """,
                conflict_params,
            )
            self.conn.executemany(
                """
                UPDATE repo
                SET name_with_owner = name_with_owner || '-renamed-' || id
                WHERE name_with_owner = ? AND id != ?
                """,
                conflict_params,
            )

            self.conn.executemany(
                """
                INSERT INTO repo(id, name_with_owner, created_at, description, homepage_url)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    name_with_owner = excluded.name_with_owner,
                    description     = excluded.description,
                    homepage_url    = excluded.homepage_url
                """,
                repo_rows,
            )

        existing_latest = self._fetch_latest_metrics(repo_ids)

        latest_rows: List[Tuple[int, int, int, int, int, int, Optional[int], Optional[int], Optional[int], int, Optional[int]]] = []
        hist_insert_rows: List[Tuple[int, int, int, int, int, int, Optional[int]]] = []
        hist_extend_rows: List[Tuple[int, int, int]] = []

        topic_repo_ids_to_refresh: List[int] = []
        topic_pairs_to_insert: List[Tuple[int, int]] = []

        for n in fresh_nodes:
            repo_id = int(n["databaseId"])

            stars = int(n.get("stargazerCount", 0))
            forks = int(n.get("forkCount", 0))
            watchers = int((n.get("watchers") or {}).get("totalCount", 0))

            disk_usage = n.get("diskUsage")
            disk_usage_i: Optional[int] = None if disk_usage is None else int(disk_usage)

            updated_at = iso_to_unix(n.get("updatedAt"))
            pushed_at = iso_to_unix(n.get("pushedAt"))
            is_archived = 1 if bool(n.get("isArchived")) else 0

            pl_name = None
            pl = n.get("primaryLanguage")
            if isinstance(pl, dict):
                pl_name = pl.get("name")
            pl_id = self._get_or_create_language_id(pl_name)

            old = existing_latest.get(repo_id)
            if old is None:
                history_start_run_id = run_id
                hist_insert_rows.append((repo_id, run_id, run_id, stars, forks, watchers, disk_usage_i))
            else:
                old_stars = int(old["stars"])
                old_forks = int(old["forks"])
                old_watchers = int(old["watchers"])
                old_disk = old["disk_usage"]
                old_disk_i = None if old_disk is None else int(old_disk)

                changed = old_stars != stars or old_forks != forks or old_watchers != watchers or old_disk_i != disk_usage_i

                if changed:
                    history_start_run_id = run_id
                    hist_insert_rows.append((repo_id, run_id, run_id, stars, forks, watchers, disk_usage_i))
                else:
                    history_start_run_id = int(old["history_start_run_id"])
                    hist_extend_rows.append((run_id, repo_id, history_start_run_id))

            latest_rows.append(
                (
                    repo_id,
                    run_id,
                    history_start_run_id,
                    stars,
                    forks,
                    watchers,
                    disk_usage_i,
                    updated_at,
                    pushed_at,
                    is_archived,
                    pl_id,
                )
            )

            topics: List[str] = []
            rt = n.get("repositoryTopics")
            if isinstance(rt, dict):
                nodes2 = rt.get("nodes") or []
                if isinstance(nodes2, list):
                    for x in nodes2:
                        tname = None
                        if isinstance(x, dict):
                            t = x.get("topic")
                            if isinstance(t, dict):
                                tname = t.get("name")
                        if isinstance(tname, str) and tname:
                            topics.append(tname)

            topic_repo_ids_to_refresh.append(repo_id)
            for tname in topics:
                tid = self._get_or_create_topic_id(tname)
                topic_pairs_to_insert.append((repo_id, tid))

        with self.conn:
            self.conn.executemany(
                """
                INSERT INTO repo_latest(
                    repo_id, run_id, history_start_run_id,
                    stars, forks, watchers, disk_usage,
                    updated_at, pushed_at, is_archived,
                    primary_language_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(repo_id) DO UPDATE SET
                    run_id               = excluded.run_id,
                    history_start_run_id = excluded.history_start_run_id,
                    stars                = excluded.stars,
                    forks                = excluded.forks,
                    watchers             = excluded.watchers,
                    disk_usage           = excluded.disk_usage,
                    updated_at           = excluded.updated_at,
                    pushed_at            = excluded.pushed_at,
                    is_archived          = excluded.is_archived,
                    primary_language_id  = excluded.primary_language_id
                """,
                latest_rows,
            )

            if hist_insert_rows:
                self.conn.executemany(
                    """
                    INSERT INTO repo_metrics_hist(
                        repo_id, start_run_id, end_run_id,
                        stars, forks, watchers, disk_usage
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    hist_insert_rows,
                )

            if hist_extend_rows:
                self.conn.executemany(
                    """
                    UPDATE repo_metrics_hist
                    SET end_run_id = ?
                    WHERE repo_id = ? AND start_run_id = ?
                    """,
                    hist_extend_rows,
                )

            if topic_repo_ids_to_refresh:
                for chunk in chunks(topic_repo_ids_to_refresh, 500):
                    q = "DELETE FROM repo_topic_latest WHERE repo_id IN ({})".format(",".join(["?"] * len(chunk)))
                    self.conn.execute(q, tuple(chunk))

            if topic_pairs_to_insert:
                self.conn.executemany(
                    "INSERT OR IGNORE INTO repo_topic_latest(repo_id, topic_id) VALUES (?, ?)",
                    topic_pairs_to_insert,
                )

    def get_repo_latest(self, name_with_owner: str) -> Optional[Dict[str, Any]]:
        q = (
            select_latest_base_sql(True)
            + """
            WHERE r.name_with_owner = ?
            GROUP BY rl.repo_id
        """
        )
        row = self.conn.execute(q, (name_with_owner,)).fetchone()
        return None if row is None else row_to_obj(row)

    def _prepare_filter_conditions(
        self,
        q_text: Optional[str] = None,
        in_description: bool = True,
        language: Optional[str] = None,
        topic: Optional[str] = None,
    ) -> Tuple[str, List[Any]]:
        where: List[str] = []
        params: List[Any] = []

        if language:
            where.append("lang.name = ?")
            params.append(language)

        if topic:
            where.append(
                """
                EXISTS (
                    SELECT 1
                    FROM repo_topic_latest rtl2
                    JOIN topic t2 ON t2.id = rtl2.topic_id
                    WHERE rtl2.repo_id = rl.repo_id AND t2.name = ?
                )
                """
            )
            params.append(topic)

        if q_text and q_text.strip():
            like = f"%{q_text.strip()}%"
            if in_description:
                where.append("(r.name_with_owner LIKE ? OR r.description LIKE ?)")
                params.extend([like, like])
            else:
                where.append("r.name_with_owner LIKE ?")
                params.append(like)

        where_clause = "\nWHERE " + " AND ".join(where) if where else ""
        return where_clause, params

    def count_leaderboard(
        self,
        q: Optional[str] = None,
        in_description: bool = True,
        language: Optional[str] = None,
        topic: Optional[str] = None,
    ) -> int:
        where_clause, params = self._prepare_filter_conditions(q, in_description, language, topic)
        sql = count_base_sql() + where_clause

        row = self.conn.execute(sql, tuple(params)).fetchone()
        return int(row["cnt"]) if row else 0

    def trending_leaderboard(
        self,
        window_seconds: int,
        page: int = 1,
        q: Optional[str] = None,
        in_description: bool = True,
        language: Optional[str] = None,
        topic: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        page_size = 100
        base_run_id = self._base_run_id_for_window(window_seconds)

        where_clause, params = self._prepare_filter_conditions(q, in_description, language, topic)
        offset = (int(page) - 1) * page_size

        extra_select = """
                MAX(
                    rl.stars - COALESCE((
                        SELECT h.stars
                        FROM repo_metrics_hist h
                        WHERE h.repo_id = rl.repo_id
                        AND h.start_run_id <= ?
                        AND h.end_run_id   >= ?
                        ORDER BY h.end_run_id ASC
                        LIMIT 1
                    ), rl.stars),
                    0
                ) AS newStars,
        """

        sql = (
            select_latest_base_sql(False, extra_select=extra_select)
            + where_clause
            + """
            GROUP BY rl.repo_id
            ORDER BY newStars DESC, rl.stars DESC, r.name_with_owner ASC
            LIMIT ? OFFSET ?
            """
        )

        all_params: List[Any] = [base_run_id, base_run_id]
        all_params.extend(params)
        all_params.extend([page_size, int(offset)])

        rows = self.conn.execute(sql, tuple(all_params)).fetchall()
        return [row_to_obj(r) for r in rows]

    def leaderboard(
        self,
        metric: str = "stars",
        page: int = 1,
        q: Optional[str] = None,
        in_description: bool = True,
        language: Optional[str] = None,
        topic: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        page_size = 100

        trending_map = {
            "trending24h": 24 * 3600,
            "trending3d": 3 * 24 * 3600,
            "trending7d": 7 * 24 * 3600,
            "trending30d": 30 * 24 * 3600,
        }
        if metric in trending_map:
            return self.trending_leaderboard(
                window_seconds=trending_map[metric],
                page=page,
                q=q,
                in_description=in_description,
                language=language,
                topic=topic,
            )

        metric_map = {
            "stars": "rl.stars",
            "stargazerCount": "rl.stars",
            "forks": "rl.forks",
            "forkCount": "rl.forks",
            "watchers": "rl.watchers",
            "watchersCount": "rl.watchers",
            "diskUsage": "rl.disk_usage",
            "disk_usage": "rl.disk_usage",
        }
        order_expr = metric_map.get(metric)
        if order_expr is None:
            raise ValueError(f"Unsupported metric: {metric}")

        where_clause, params = self._prepare_filter_conditions(q, in_description, language, topic)

        offset = (int(page) - 1) * page_size

        sql = (
            select_latest_base_sql(False)
            + where_clause
            + f"""
            GROUP BY rl.repo_id
            ORDER BY {order_expr} DESC, r.name_with_owner ASC
            LIMIT ? OFFSET ?
            """
        )
        params.extend([page_size, int(offset)])

        rows = self.conn.execute(sql, tuple(params)).fetchall()
        return [row_to_obj(r) for r in rows]

    def get_global_rank(self, name_with_owner: str) -> Optional[int]:
        sql = """
            SELECT
                (
                    SELECT COUNT(*)
                    FROM repo_latest rl2
                    JOIN repo r2 ON r2.id = rl2.repo_id
                    WHERE rl2.stars > rl.stars
                       OR (rl2.stars = rl.stars AND r2.name_with_owner < r.name_with_owner)
                ) + 1 AS globalRank
            FROM repo_latest rl
            JOIN repo r ON r.id = rl.repo_id
            WHERE r.name_with_owner = ?
        """
        row = self.conn.execute(sql, (name_with_owner,)).fetchone()
        return int(row["globalRank"]) if row else None

    def history_segments(self, name_with_owner: str, limit: int = 5000) -> List[Dict[str, Any]]:
        row = self.conn.execute("SELECT id FROM repo WHERE name_with_owner = ?", (name_with_owner,)).fetchone()
        if row is None:
            return []
        repo_id = int(row["id"])

        q = """
            SELECT
                h.start_run_id, h.end_run_id,
                rs.fetched_at AS startFetchedAtUnix,
                re.fetched_at AS endFetchedAtUnix,
                h.stars, h.forks, h.watchers, h.disk_usage
            FROM repo_metrics_hist h
            JOIN fetch_run rs ON rs.id = h.start_run_id
            JOIN fetch_run re ON re.id = h.end_run_id
            WHERE h.repo_id = ?
            ORDER BY h.start_run_id ASC
            LIMIT ?
        """
        rows = self.conn.execute(q, (repo_id, int(limit))).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "startFetchedAt": unix_to_iso(r["startFetchedAtUnix"]),
                    "endFetchedAt": unix_to_iso(r["endFetchedAtUnix"]),
                    "s": int(r["stars"]),
                    "f": int(r["forks"]),
                    "w": int(r["watchers"]),
                    "d": None if r["disk_usage"] is None else int(r["disk_usage"]),
                }
            )
        return out
