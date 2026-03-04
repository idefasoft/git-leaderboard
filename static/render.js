import { archivedBadge, escapeHtml, fmtDelta, fmtInt } from "./lib.js";
import { getState } from "./state.js";

let currentItems = [];
let currentSortKey = null;
let currentSortAsc = false;
let currentTotalPages = 1;

export function sortItems(key) {
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

export function renderTable() {
  const state = getState();
  const isTrending = String(state.metric || "").startsWith("trending");
  const startRank = (state.page - 1) * 100;
  const { highlight } = getUIParams();

  let html = `<div class="table-wrap"><table>
    <thead><tr>
      <th>#</th>
      <th data-sort="n">Repository</th>
      <th data-sort="s">Stars</th>
      ${isTrending ? `<th data-sort="ns">New stars</th>` : ``}
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
      ${isTrending ? `<td class="mono">${fmtDelta(it.ns)}</td>` : ``}
      <td class="mono">${fmtInt(it.f)}</td>
      <td>${escapeHtml(it.l || "")}</td>
    </tr>`;
  });

  html += "</tbody></table></div>";
  return html;
}

export function renderCards() {
  const state = getState();
  const isTrending = String(state.metric || "").startsWith("trending");
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
        <span><svg width="14" height="14" viewBox="0 0 1024 1024" fill="currentColor" style="vertical-align:-2px"><path d="M923.2 429.6H608l-97.6-304-97.6 304H97.6l256 185.6L256 917.6l256-187.2 256 187.2-100.8-302.4z"/></svg> ${fmtInt(it.s)}${isTrending ? ` (${fmtDelta(it.ns)})` : ``}</span>
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

export function renderContent() {
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

// export for other modules to set state
export function setItems(items, totalPages, page) {
  currentItems = items;
  currentSortKey = null;
  currentSortAsc = false;
  renderContent();
  currentTotalPages = totalPages;
  return currentTotalPages;
}

export function hasItems() {
  return currentItems.length > 0;
}

export function getTotalPages() {
  return currentTotalPages;
}

// import functions used in renderContent but defined elsewhere
import { $ } from "./lib.js";
import { applyHighlightAndOpen, openModal } from "./modal.js";
import { getUIParams } from "./state.js";

