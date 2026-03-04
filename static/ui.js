import { loadLeaderboard } from "./api.js";
import { $ } from "./lib.js";
import { closeModal } from "./modal.js";
import { getTotalPages, renderContent } from "./render.js";
import { getState, setState } from "./state.js";

export const STATIC_DATA = { languages: [], topics: [] };

export function loadLanguages() {
  const dl = $("languagesList");
  dl.innerHTML = "";
  (STATIC_DATA.languages || []).forEach((name) => {
    const opt = document.createElement("option");
    opt.value = name;
    dl.appendChild(opt);
  });
}

export function loadTopics() {
  const dl = $("topicsList");
  dl.innerHTML = "";
  (STATIC_DATA.topics || []).forEach((t) => {
    const opt = document.createElement("option");
    opt.value = t.name;
    opt.textContent = `${t.name} (${t.count})`;
    dl.appendChild(opt);
  });
}

export function goToPage() {
  const val = parseInt($("pageInput").value, 10);
  if (val >= 1 && val <= getTotalPages()) {
    const state = getState();
    state.page = val;
    setState(state);
    loadLeaderboard();
  } else {
    const state = getState();
    $("pageInput").value = state.page;
  }
}

export function wire() {
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
    if (state.page < getTotalPages()) {
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


