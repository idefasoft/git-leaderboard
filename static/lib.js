// utility helper functions
export const $ = (id) => document.getElementById(id);

export function cssEscape(s) {
  return CSS.escape(String(s));
}

export function fmtInt(n) {
  if (n === null || n === undefined) return "–";
  return Number(n).toLocaleString();
}

export function fmtDiskSize(kb) {
  if (kb === null || kb === undefined) return "–";
  const num = Number(kb);
  if (num >= 1000) {
    return (num / 1000).toFixed(1) + " MB";
  }
  return num.toLocaleString() + " KB";
}

export function fmtDate(iso) {
  return iso ? String(iso).slice(0, 10) : "";
}

export function fmtDelta(n) {
  if (n === null || n === undefined) return "–";
  const v = Number(n);
  const s = v.toLocaleString();
  return v < 0 ? `-${s}` : v > 0 ? `+${s}` : s;
}

export function escapeHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;");
}

export function archivedBadge(it) {
  return it && it.i ? `<span class="badge badge-archived">Archived</span>` : "";
}

export function cacheKey(state) {
  return JSON.stringify([
    state.page,
    state.metric,
    state.q,
    state.language,
    state.topic,
  ]);
}
