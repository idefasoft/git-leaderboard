import { $, cacheKey, fmtInt } from "./lib.js";
import { hasItems, setItems } from "./render.js";
import { getState } from "./state.js";

const cache = {};

export async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

export async function loadLeaderboard(useCache = true) {
  const state = getState();
  const key = cacheKey(state);

  $("err").style.display = "none";
  syncUI(state);

  if (useCache && cache[key]) {
    applyData(cache[key], state);
    return;
  }

  const content = $("content");
  if (!hasItems()) {
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
    if (!hasItems()) {
      content.innerHTML = "";
    }
  } finally {
    content.style.opacity = "1";
  }
}

export function applyData(data, state) {
  const items = data.items || [];
  const total = data.total || 0;
  const totalPages = data.totalPages || 1;
  const page = data.page || state.page;

  setItems(items, totalPages, page);

  $("pageInfo").textContent = `${fmtInt(total)} repos found`;
  $("pageInput").value = page;
  $("pageInput").max = totalPages;
  $("pageMax").textContent = `/ ${totalPages}`;
  $("prev").disabled = page <= 1;
  $("next").disabled = page >= totalPages;
}

// syncUI is still defined elsewhere - perhaps in app.js
export function syncUI(state) {
  $("q").value = state.q;
  $("metric").value = state.metric;
  $("language").value = state.language;
  $("topic").value = state.topic;
  $("viewTable").classList.toggle("active", state.view === "table");
  $("viewCards").classList.toggle("active", state.view === "cards");
}
