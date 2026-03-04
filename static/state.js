// functions related to URL state and parameters
export function getUIParams() {
  const url = new URL(location.href);
  return {
    highlight: url.searchParams.get("highlight") || "",
    open: url.searchParams.get("open") || "",
  };
}

export function consumeUrlParam(name) {
  const url = new URL(location.href);
  url.searchParams.delete(name);
  history.replaceState({}, "", url.toString());
}

export function getState() {
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

export function setState(state) {
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
