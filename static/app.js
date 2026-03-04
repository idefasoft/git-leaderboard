// main entrypoint remnant; actual logic is split into modules
import { loadLeaderboard } from './api.js';
import { loadLanguages, loadTopics, wire } from './ui.js';

// start the application
(async function main() {
  wire();
  loadLanguages();
  loadTopics();
  await loadLeaderboard();
})();
