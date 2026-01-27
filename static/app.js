const $ = (id) => document.getElementById(id);
const STATIC_DATA = { languages: [], topics: [] };

const cache = {};
const repoCache = {};
let didAutoScroll = false;

function getUIParams() {
  const url = new URL(location.href);
  return {
    highlight: url.searchParams.get("highlight") || "",
    open: url.searchParams.get("open") || "",
  };
}

function consumeUrlParam(name) {
  const url = new URL(location.href);
  url.searchParams.delete(name);
  history.replaceState({}, "", url.toString());
}

function cssEscape(s) {
  return CSS.escape(String(s));
}

function cacheKey(state) {
  return JSON.stringify([
    state.page,
    state.metric,
    state.q,
    state.language,
    state.topic,
  ]);
}

function fmtInt(n) {
  if (n === null || n === undefined) return "–";
  return Number(n).toLocaleString();
}

function fmtDiskSize(kb) {
  if (kb === null || kb === undefined) return "–";
  const num = Number(kb);
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + " MB";
  }
  return num.toLocaleString() + " KB";
}

function fmtDate(iso) {
  return iso ? String(iso).slice(0, 10) : "";
}

function escapeHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function archivedBadge(it) {
  return it && it.i ? `<span class="badge badge-archived">Archived</span>` : "";
}

function applyHighlightAndOpen() {
  const { highlight, open } = getUIParams();

  if (highlight && !didAutoScroll) {
    const el = document.querySelector(`[data-repo="${cssEscape(highlight)}"]`);
    if (el) {
      el.scrollIntoView({ block: "center" });
      didAutoScroll = true;
    }
  }

  if (open && currentItems.some((it) => it.n === open)) {
    openModal(open);
    consumeUrlParam("open");
  }
}

function getState() {
  const url = new URL(location.href);
  return {
    page: Number(url.searchParams.get("page") || 1),
    metric: url.searchParams.get("metric") || "stars",
    q: url.searchParams.get("q") || "",
    language: url.searchParams.get("language") || "",
    topic: url.searchParams.get("topic") || "",
    view: url.searchParams.get("view") || "table",
  };
}

function setState(state) {
  const url = new URL(location.href);
  url.searchParams.set("page", state.page);
  url.searchParams.set("metric", state.metric);
  url.searchParams.set("view", state.view);
  state.q ? url.searchParams.set("q", state.q) : url.searchParams.delete("q");
  state.language ?
    url.searchParams.set("language", state.language) :
    url.searchParams.delete("language");
  state.topic ?
    url.searchParams.set("topic", state.topic) :
    url.searchParams.delete("topic");
  history.replaceState({}, "", url.toString());
}

async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

let currentItems = [];
let currentSortKey = null;
let currentSortAsc = false;
let currentTotalPages = 1;

function sortItems(key) {
  if (currentSortKey === key) {
    currentSortAsc = !currentSortAsc;
  } else {
    currentSortKey = key;
    currentSortAsc = false;
  }

  currentItems.sort((a, b) => {
    let va = a[key],
      vb = b[key];

    if (key === "n") {
      va = va ? va.split("/").pop().toLowerCase() : "";
      vb = vb ? vb.split("/").pop().toLowerCase() : "";
    } else {
      if (va === null || va === undefined) va = -Infinity;
      if (vb === null || vb === undefined) vb = -Infinity;
      if (typeof va === "string") va = va.toLowerCase();
      if (typeof vb === "string") vb = vb.toLowerCase();
    }

    if (va < vb) return currentSortAsc ? -1 : 1;
    if (va > vb) return currentSortAsc ? 1 : -1;
    return 0;
  });

  renderContent();
}

function renderTable() {
  const state = getState();
  const startRank = (state.page - 1) * 100;
  const { highlight } = getUIParams();

  let html = `<div class="table-wrap"><table>
    <thead><tr>
      <th>#</th>
      <th data-sort="n">Repository</th>
      <th data-sort="s">Stars</th>
      <th data-sort="f">Forks</th>
      <th data-sort="l">Language</th>
    </tr></thead><tbody>`;

  currentItems.forEach((it, i) => {
    const isHi = highlight && highlight === it.n;
    html += `<tr data-repo="${escapeHtml(it.n)}" class="${isHi ? "highlight" : ""}">
      <td class="mono">${startRank + i + 1}</td>
      <td>
        <span class="repo-link" data-repo="${escapeHtml(it.n)}">${escapeHtml(it.n)}</span>
        ${archivedBadge(it)}
      </td>
      <td class="mono">${fmtInt(it.s)}</td>
      <td class="mono">${fmtInt(it.f)}</td>
      <td>${escapeHtml(it.l || "")}</td>
    </tr>`;
  });

  html += "</tbody></table></div>";
  return html;
}

function renderCards() {
  let html = '<div class="cards">';
  const { highlight } = getUIParams();

  currentItems.forEach((it) => {
    const topics = (it.t || []).slice(0, 5);
    const isHi = highlight && highlight === it.n;

    html += `<div class="card ${isHi ? "highlight" : ""}" data-repo="${escapeHtml(it.n)}">
      <div class="card-header">
      <div class="card-name-wrap">
        <span class="card-name">${escapeHtml(it.n)}</span>
        ${archivedBadge(it)}
      </div>
        <div class="card-stats">
        <span><svg width="14" height="14" viewBox="0 0 1024 1024" fill="currentColor" style="vertical-align:-2px"><path d="M923.2 429.6H608l-97.6-304-97.6 304H97.6l256 185.6L256 917.6l256-187.2 256 187.2-100.8-302.4z"/></svg> ${fmtInt(it.s)}</span>
        <span><svg width="14" height="14" viewBox="0 0 24 24" fill="currentColor" style="vertical-align:-2px"><path d="M7 5C7 3.89543 7.89543 3 9 3C10.1046 3 11 3.89543 11 5C11 5.74028 10.5978 6.38663 10 6.73244V14.0396H11.7915C12.8961 14.0396 13.7915 13.1441 13.7915 12.0396V10.7838C13.1823 10.4411 12.7708 9.78837 12.7708 9.03955C12.7708 7.93498 13.6662 7.03955 14.7708 7.03955C15.8753 7.03955 16.7708 7.93498 16.7708 9.03955C16.7708 9.77123 16.3778 10.4111 15.7915 10.7598V12.0396C15.7915 14.2487 14.0006 16.0396 11.7915 16.0396H10V17.2676C10.5978 17.6134 11 18.2597 11 19C11 20.1046 10.1046 21 9 21C7.89543 21 7 20.1046 7 19C7 18.2597 7.4022 17.6134 8 17.2676V6.73244C7.4022 6.38663 7 5.74028 7 5Z"/></svg> ${fmtInt(it.f)}</span>
        </div>
      </div>
      ${it.a ? `<div class="card-desc">${escapeHtml(it.a)}</div>` : ""}
      ${topics.length > 0 ? `<div class="card-topics">${topics.map((t) => `<a href="?topic=${encodeURIComponent(t)}" class="badge">${escapeHtml(t)}</a>`).join("")}</div>` : ""}
      ${it.h ? `<div class="card-homepage"><a href="${escapeHtml(it.h)}" target="_blank" rel="noreferrer">${escapeHtml(it.h)}</a></div>` : ""}
      <div class="card-footer">
        <div class="card-footer-left">${escapeHtml(it.l || "")}</div>
        <div class="card-footer-right">
          <span>${fmtInt(it.w)} watchers</span>
          ${it.d !== null ? `<span>${fmtDiskSize(it.d)}</span>` : ""}
        </div>
      </div>
    </div>`;
  });

  html += "</div>";
  return html;
}

function renderContent() {
  const state = getState();
  const content = $("content");
  content.innerHTML = state.view === "cards" ? renderCards() : renderTable();

  content.querySelectorAll(".repo-link, .card").forEach((el) => {
    el.addEventListener("click", (e) => {
      if (e.target.tagName === "A") return;
      openModal(el.dataset.repo);
    });
  });

  content.querySelectorAll("th[data-sort]").forEach((th) => {
    th.addEventListener("click", () => sortItems(th.dataset.sort));
  });
  applyHighlightAndOpen();
}

async function loadLeaderboard(useCache = true) {
  const state = getState();
  const key = cacheKey(state);

  $("err").style.display = "none";
  syncUI(state);

  if (useCache && cache[key]) {
    applyData(cache[key], state);
    return;
  }

  const content = $("content");
  if (currentItems.length === 0) {
    content.innerHTML = '<div class="loading">Loading...</div>';
  } else {
    content.style.opacity = "0.5";
  }

  const params = new URLSearchParams({
    page: state.page,
    metric: state.metric,
  });
  if (state.q) params.set("q", state.q);
  if (state.language) params.set("language", state.language);
  if (state.topic) params.set("topic", state.topic);

  try {
    const data = await fetchJSON(`/api/leaderboard?${params}`);
    cache[key] = data;
    applyData(data, state);
  } catch (e) {
    $("err").textContent = e.message;
    $("err").style.display = "block";
    if (currentItems.length === 0) {
      content.innerHTML = "";
    }
  } finally {
    content.style.opacity = "1";
  }
}

function applyData(data, state) {
  currentItems = data.items || [];
  currentSortKey = null;
  currentSortAsc = false;
  renderContent();

  const total = data.total || 0;
  const totalPages = data.totalPages || 1;
  const page = data.page || state.page;

  currentTotalPages = totalPages;

  $("pageInfo").textContent = `${fmtInt(total)} repos found`;
  $("pageInput").value = page;
  $("pageInput").max = totalPages;
  $("pageMax").textContent = `/ ${totalPages}`;
  $("prev").disabled = page <= 1;
  $("next").disabled = page >= totalPages;
}

function syncUI(state) {
  $("q").value = state.q;
  $("metric").value = state.metric;
  $("language").value = state.language;
  $("topic").value = state.topic;
  $("viewTable").classList.toggle("active", state.view === "table");
  $("viewCards").classList.toggle("active", state.view === "cards");
}

function loadLanguages() {
  const dl = $("languagesList");
  dl.innerHTML = "";
  (STATIC_DATA.languages || []).forEach((name) => {
    const opt = document.createElement("option");
    opt.value = name;
    dl.appendChild(opt);
  });
}

function loadTopics() {
  const dl = $("topicsList");
  dl.innerHTML = "";
  (STATIC_DATA.topics || []).forEach((t) => {
    const opt = document.createElement("option");
    opt.value = t.name;
    opt.textContent = `${t.name} (${t.count})`;
    dl.appendChild(opt);
  });
}

let modalCharts = [];

function openModal(repoName) {
  $("modalOverlay").classList.remove("hidden");
  $("modalTitle").innerHTML =
    `<a href="https://github.com/${escapeHtml(repoName)}" target="_blank" rel="noreferrer">${escapeHtml(repoName)}</a>`;
  $("modalBody").innerHTML = '<div class="loading">Loading...</div>';
  document.body.style.overflow = "hidden";
  loadRepoDetails(repoName);
}

function closeModal() {
  $("modalOverlay").classList.add("hidden");
  document.body.style.overflow = "";
  modalCharts.forEach((c) => c.destroy());
  modalCharts = [];
}

async function loadRepoDetails(name) {
  if (repoCache[name]) {
    renderModal(repoCache[name].repo, repoCache[name].segments);
    return;
  }
  try {
    const [repo, hist] = await Promise.all([
      fetchJSON(`/api/repo?name=${encodeURIComponent(name)}`),
      fetchJSON(`/api/repo/history?name=${encodeURIComponent(name)}`),
    ]);
    repoCache[name] = { repo, segments: hist.segments || [] };
    renderModal(repo, hist.segments || []);
  } catch (e) {
    $("modalBody").innerHTML =
      `<div class="error">${escapeHtml(e.message)}</div>`;
  }
}

const chartOpts = (data) => {
  const minX = data.length > 0 ? data[0].x : undefined;
  const maxX = data.length > 0 ? data[data.length - 1].x : undefined;

  return {
    type: "line",
    data: {
      datasets: [
        {
          data: data,
          borderColor: "#58a6ff",
          borderWidth: 2,
          pointRadius: 0,
          tension: 0.2,
          stepped: false,
          fill: false,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (ctx) => {
              return new Date(ctx[0].parsed.x).toISOString().slice(0, 10);
            },
          },
        },
      },
      scales: {
        x: {
          type: "linear",
          min: minX,
          max: maxX,
          grid: { color: "#30363d" },
          ticks: {
            color: "#8b949e",
            maxTicksLimit: 6,
            callback: (val) => new Date(val).toISOString().slice(0, 10),
          },
        },
        y: {
          grid: { color: "#30363d" },
          ticks: {
            color: "#8b949e",
            callback: function (val) {
              return val % 1 === 0 ? val : null;
            },
          },
        },
      },
    },
  };
};

function renderModal(repo, segments) {
  let metaHtml = '<div class="meta-grid">';

  if (repo.g !== null && repo.g !== undefined) {
    const domain = window.location.origin;
    const badgeUrl = `https://img.shields.io/endpoint?url=${encodeURIComponent(domain + "/api/rank?name=" + repo.n)}`;
    const markdown = `[![Global Rank](${badgeUrl})](${domain}/${repo.n})`;

    metaHtml += `
          <div class="meta-label">Global rank</div>
          <div class="meta-value mono">
            #${fmtInt(repo.g)}
            <button class="btn-action btn-go" style="margin-left: 8px; padding: 2px 8px; font-size: 11px;"
              onclick="navigator.clipboard.writeText('${markdown}'); this.innerText='Copied!'; setTimeout(()=>this.innerText='Copy Badge', 2000)">
              Copy Badge
            </button>
          </div>`;
  }
  if (repo.i) {
    metaHtml += `<div class="meta-label">Status</div><div class="meta-value">${archivedBadge(repo)}</div>`;
  }

  if (repo.a) {
    metaHtml += `<div class="meta-label">Description</div><div class="meta-value">${escapeHtml(repo.a)}</div>`;
  }
  if (repo.h) {
    metaHtml += `<div class="meta-label">Homepage</div><div class="meta-value"><a href="${escapeHtml(repo.h)}" target="_blank" rel="noreferrer">${escapeHtml(repo.h)}</a></div>`;
  }
  if (repo.l) {
    metaHtml += `<div class="meta-label">Language</div><div class="meta-value"><a href="?language=${encodeURIComponent(repo.l)}" class="modal-filter-link">${escapeHtml(repo.l)}</a></div>`;
  }
  if (repo.t && repo.t.length > 0) {
    const topicsHtml = repo.t
      .map(
        (t) =>
        `<a href="?topic=${encodeURIComponent(t)}" class="badge modal-filter-link">${escapeHtml(t)}</a>`,
      )
      .join(" ");
    metaHtml += `<div class="meta-label">Topics</div><div class="meta-value">${topicsHtml}</div>`;
  }
  if (repo.c) {
    metaHtml += `<div class="meta-label">Created</div><div class="meta-value">${fmtDate(repo.c)}</div>`;
  }
  if (repo.p) {
    metaHtml += `<div class="meta-label">Last push</div><div class="meta-value">${fmtDate(repo.p)}</div>`;
  }
  metaHtml += "</div>";

  const statsHtml = `
    <div class="stats-row">
      <div class="stat-item"><div class="stat-value">${fmtInt(repo.s)}</div><div class="stat-label">Stars</div></div>
      <div class="stat-item"><div class="stat-value">${fmtInt(repo.f)}</div><div class="stat-label">Forks</div></div>
      <div class="stat-item"><div class="stat-value">${fmtInt(repo.w)}</div><div class="stat-label">Watchers</div></div>
      ${repo.d !== null ? `<div class="stat-item"><div class="stat-value">${fmtDiskSize(repo.d)}</div><div class="stat-label">Disk</div></div>` : ""}
    </div>
  `;

  const chartsHtml = `
    <div class="charts-section">
      <h3>History</h3>
      <div class="chart-row-stars">
        <div class="chart-box"><h4>Stars</h4><canvas id="chartStars"></canvas></div>
      </div>
      <div class="chart-row-bottom">
        <div class="chart-box chart-forks"><h4>Forks</h4><canvas id="chartForks"></canvas></div>
        <div class="chart-stack">
          <div class="chart-box"><h4>Watchers</h4><canvas id="chartWatchers"></canvas></div>
          <div class="chart-box"><h4>Disk Usage</h4><canvas id="chartDisk"></canvas></div>
        </div>
      </div>
    </div>
  `;

  $("modalBody").innerHTML = metaHtml + statsHtml + chartsHtml;

  modalCharts.forEach((c) => c.destroy());
  modalCharts = [];

  const makeData = (key) => {
    const data = [];
    segments.forEach((s) => {
      if (s.startFetchedAt) {
        data.push({
          x: new Date(s.startFetchedAt).getTime(),
          y: s[key]
        });

      }
      if (s.endFetchedAt && s.endFetchedAt !== s.startFetchedAt) {
        data.push({
          x: new Date(s.endFetchedAt).getTime(),
          y: s[key]
        });

      }
    });
    return data;
  };

  const stars = makeData("s");
  const forks = makeData("f");
  const watchers = makeData("w");
  const disk = makeData("d");

  modalCharts.push(new Chart($("chartStars"), chartOpts(stars)));
  modalCharts.push(new Chart($("chartForks"), chartOpts(forks)));
  modalCharts.push(new Chart($("chartWatchers"), chartOpts(watchers)));
  modalCharts.push(new Chart($("chartDisk"), chartOpts(disk)));

  $("modalBody")
    .querySelectorAll(".modal-filter-link")
    .forEach((link) => {
      link.addEventListener("click", (e) => {
        e.preventDefault();
        closeModal();
        window.location.href = link.href;
      });
    });
}

function goToPage() {
  const val = parseInt($("pageInput").value, 10);
  if (val >= 1 && val <= currentTotalPages) {
    const state = getState();
    state.page = val;
    setState(state);
    loadLeaderboard();
  } else {
    const state = getState();
    $("pageInput").value = state.page;
  }
}

function wire() {
  $("apply").addEventListener("click", () => {
    const state = getState();
    state.page = 1;
    state.metric = $("metric").value;
    state.q = $("q").value.trim();
    state.language = $("language").value.trim();
    state.topic = $("topic").value.trim();
    setState(state);
    loadLeaderboard(false);
  });

  $("appTitle").addEventListener("click", (e) => {
    e.preventDefault();
    history.pushState({}, "", "/");
    loadLeaderboard();
  });

  $("q").addEventListener("keydown", (e) => {
    if (e.key === "Enter") $("apply").click();
  });

  $("prev").addEventListener("click", () => {
    const state = getState();
    if (state.page > 1) {
      state.page--;
      setState(state);
      loadLeaderboard();
    }
  });

  $("next").addEventListener("click", () => {
    const state = getState();
    if (state.page < currentTotalPages) {
      state.page++;
      setState(state);
      loadLeaderboard();
    }
  });

  $("goPage").addEventListener("click", goToPage);

  $("pageInput").addEventListener("keydown", (e) => {
    if (e.key === "Enter") goToPage();
  });

  $("viewTable").addEventListener("click", () => {
    const state = getState();
    state.view = "table";
    setState(state);
    $("viewTable").classList.add("active");
    $("viewCards").classList.remove("active");
    renderContent();
  });

  $("viewCards").addEventListener("click", () => {
    const state = getState();
    state.view = "cards";
    setState(state);
    $("viewCards").classList.add("active");
    $("viewTable").classList.remove("active");
    renderContent();
  });

  $("modalOverlay").addEventListener("click", (e) => {
    if (e.target === $("modalOverlay")) closeModal();
  });
  $("modalClose").addEventListener("click", closeModal);
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && !$("modalOverlay").classList.contains("hidden")) {
      closeModal();
    }
  });
}

(async function main() {
  wire();
  loadLanguages();
  loadTopics();
  await loadLeaderboard();
})();
