const form = document.querySelector("#sync-form");
const query = document.querySelector("#query");
const token = document.querySelector("#token");
const accessPanel = document.querySelector("#access-panel");
const indexers = document.querySelector("#indexers");
const jobs = document.querySelector("#jobs");
const empty = document.querySelector("#empty");
const availability = document.querySelector("#availability");
const message = document.querySelector("#form-message");
const announcer = document.querySelector("#announcer");
const refresh = document.querySelector("#refresh");
const submit = form.querySelector('button[type="submit"]');
let poll;
let previousRunning = 0;

token.value = sessionStorage.getItem("crown-index-operator-token") || "";
token.addEventListener("input", () => {
  sessionStorage.setItem("crown-index-operator-token", token.value);
});

function headers(mutating = false) {
  const values = { Accept: "application/json" };
  if (token.value) values.Authorization = "Bearer " + token.value;
  if (mutating) {
    values["Content-Type"] = "application/json";
    values["X-Crown-Index-Intent"] = "operator-sync";
  }
  return values;
}

async function request(path, options = {}) {
  const response = await fetch(path, options);
  const body = await response.json().catch(() => ({ error: "Invalid server response" }));
  if (!response.ok) {
    const error = new Error(body.error || "Request failed (" + response.status + ")");
    error.status = response.status;
    throw error;
  }
  return body;
}

function showError(error) {
  message.textContent = error.message;
  if (error.status === 401) {
    accessPanel.open = true;
    token.focus();
  }
}

function make(tag, className, text) {
  const element = document.createElement(tag);
  if (className) element.className = className;
  if (text !== undefined) element.textContent = text;
  return element;
}

async function loadConfig() {
  try {
    const config = await request("/operator/api/config", { headers: headers() });
    availability.textContent = config.available ? "Ready" : "Jackett unavailable";
    availability.className = "status-badge " + (config.available ? "completed" : "failed");
    submit.disabled = !config.available;
    indexers.replaceChildren();
    config.indexers.forEach((name, position) => {
      const option = make("div", "source-option");
      const input = make("input");
      input.type = "checkbox";
      input.id = "indexer-" + position;
      input.name = "indexer";
      input.value = name;
      input.checked = true;
      const label = make("label", "", name);
      label.htmlFor = input.id;
      option.append(input, label);
      indexers.append(option);
    });
    message.textContent = "";
    return true;
  } catch (error) {
    availability.textContent = error.status === 401 ? "Token required" : "Unavailable";
    submit.disabled = true;
    showError(error);
    return false;
  }
}

function formatTime(epoch) {
  const options = { dateStyle: "medium", timeStyle: "short" };
  return epoch ? new Intl.DateTimeFormat(undefined, options).format(epoch * 1000) : "Waiting";
}

async function jobDetail(id, body) {
  if (body.dataset.loaded === "true") return;
  body.dataset.loaded = "true";
  try {
    const detail = await request("/operator/api/jobs/" + id, { headers: headers() });
    const metrics = make("div", "metrics");
    [
      ["Found", detail.fetched],
      ["Imported", detail.imported],
      ["Already known / skipped", detail.skipped],
      ["Metadata deferred", detail.deferred],
      ["Saturated sources", detail.saturated_sources],
      ["TMDB matched", detail.matched],
      ["TMDB rejected", detail.rejected],
      ["Enrichment pending", detail.enrichment_pending],
    ].forEach(([label, value]) => metrics.append(make("span", "", label + " " + value)));
    const list = make("ul", "sources");
    detail.sources.forEach((source) => {
      const item = make("li", "source");
      item.append(
        make("strong", "", source.indexer),
        make("span", "source-meta", source.status + " · " + source.fetched + " found · " + source.imported + " imported" + (source.saturated ? " · limit reached" : ""))
      );
      if (source.error) item.append(make("p", "error", source.error));
      list.append(item);
    });
    body.append(metrics, list);
    if (detail.error) body.append(make("p", "error", detail.error));
  } catch (error) {
    body.dataset.loaded = "false";
    body.append(make("p", "error", error.message));
  }
}

function renderJobs(items) {
  jobs.replaceChildren();
  empty.hidden = items.length !== 0;
  let running = 0;
  items.forEach((item) => {
    if (item.status === "queued" || item.status === "running") running += 1;
    const listItem = make("li", "job");
    const disclosure = make("details");
    const summary = make("summary");
    const identity = make("span");
    identity.append(
      make("span", "job-title", item.query),
      make("span", "job-meta", "#" + item.id + " · " + formatTime(item.created_at) + " · " + item.requested_indexers.length + " sources")
    );
    const visibleState = item.status === "running" ? item.phase : item.status;
    summary.append(identity, make("span", "state " + item.status, visibleState));
    const body = make("div", "job-body");
    disclosure.addEventListener("toggle", () => {
      if (disclosure.open) jobDetail(item.id, body);
    });
    disclosure.append(summary, body);
    listItem.append(disclosure);
    jobs.append(listItem);
  });
  if (previousRunning > 0 && running === 0) announcer.textContent = "Targeted sync finished.";
  previousRunning = running;
  clearTimeout(poll);
  poll = setTimeout(loadJobs, running > 0 ? 1500 : 10000);
}

async function loadJobs() {
  try {
    renderJobs(await request("/operator/api/jobs", { headers: headers() }));
  } catch (error) {
    showError(error);
    clearTimeout(poll);
  }
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (!form.reportValidity()) return;
  submit.disabled = true;
  message.textContent = "";
  const selected = Array.from(form.querySelectorAll('input[name="indexer"]:checked')).map((input) => input.value);
  if (selected.length === 0) {
    message.textContent = "Select at least one source.";
    submit.disabled = false;
    return;
  }
  try {
    const job = await request("/operator/api/jobs", {
      method: "POST",
      headers: headers(true),
      body: JSON.stringify({ query: query.value, indexers: selected }),
    });
    query.value = "";
    announcer.textContent = "Sync " + job.id + " queued.";
    await loadJobs();
  } catch (error) {
    showError(error);
  } finally {
    submit.disabled = false;
  }
});

refresh.addEventListener("click", async () => {
  refresh.disabled = true;
  if (await loadConfig()) await loadJobs();
  refresh.disabled = false;
});
window.addEventListener("pagehide", () => clearTimeout(poll));

loadConfig().then((ready) => {
  if (ready) loadJobs();
});
