import { fetchJSON } from "./api.js";
import { $, archivedBadge, escapeHtml, fmtDate, fmtDiskSize, fmtInt } from "./lib.js";

// shared data caches
const repoCache = {};

let modalCharts = [];

export function openModal(repoName) {
  $("modalOverlay").classList.remove("hidden");
  $("modalTitle").innerHTML =
    `<a href="https://github.com/${escapeHtml(repoName)}" target="_blank" rel="noreferrer">${escapeHtml(repoName)}</a>`;
  $("modalBody").innerHTML = '<div class="loading">Loading...</div>';
  document.body.style.overflow = "hidden";
  loadRepoDetails(repoName);
}

export function closeModal() {
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

// functions used by render.js
export { applyHighlightAndOpen } from "./render.js";
export { openModal };
