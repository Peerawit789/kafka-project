// Frontend State & Handlers
let jobs = [];
let ws = null;
let currentViewingJob = null;
let reconnectInterval = 2000;

const INTERFACE_REGEX = /^(FastEthernet|GigabitEthernet|Ethernet|Serial)\d+(\/\d+)*$/i;

// Elements
const jobForm = document.getElementById("jobForm");
const actionSelect = document.getElementById("actionSelect");
const interfaceInput = document.getElementById("interfaceInput");
const noParamNotice = document.getElementById("noParamNotice");
const formValidationMsg = document.getElementById("formValidationMsg");
const submitBtn = document.getElementById("submitBtn");
const submitBtnText = document.getElementById("submitBtnText");
const submitSpinner = document.getElementById("submitSpinner");

const wsStatusBadge = document.getElementById("wsStatusBadge");
const wsStatusDot = document.getElementById("wsStatusDot");
const wsStatusText = document.getElementById("wsStatusText");

const jobsTableBody = document.getElementById("jobsTableBody");
const jobCountBadge = document.getElementById("jobCountBadge");
const refreshJobsBtn = document.getElementById("refreshJobsBtn");

const outputModal = document.getElementById("outputModal");
const modalJobTitle = document.getElementById("modalJobTitle");
const modalJobMeta = document.getElementById("modalJobMeta");
const modalOutputTerminal = document.getElementById("modalOutputTerminal");
const modalOutputErrorBox = document.getElementById("modalOutputErrorBox");
const modalOutputErrorText = document.getElementById("modalOutputErrorText");
const modalTimingInfo = document.getElementById("modalTimingInfo");
const closeModalBtn = document.getElementById("closeModalBtn");
const closeModalBottomBtn = document.getElementById("closeModalBottomBtn");
const copyOutputBtn = document.getElementById("copyOutputBtn");

// Handle Action change
actionSelect.addEventListener("change", () => {
  const val = actionSelect.value;
  if (val === "interface_shutdown" || val === "interface_no_shutdown") {
    interfaceInput.classList.remove("hidden");
    interfaceInput.disabled = false;
    noParamNotice.classList.add("hidden");
  } else {
    interfaceInput.classList.add("hidden");
    interfaceInput.disabled = true;
    noParamNotice.classList.remove("hidden");
    interfaceInput.value = "";
    formValidationMsg.textContent = "";
  }
});

// Setup WebSocket Connection
function initWebSocket() {
  const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
  const wsUrl = `${protocol}//${window.location.host}/ws/jobs`;

  setWsStatus("connecting");
  ws = new WebSocket(wsUrl);

  ws.onopen = () => {
    setWsStatus("connected");
    reconnectInterval = 2000;
  };

  ws.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (msg.event === "job_created") {
        upsertJob(msg.data, true);
      } else if (msg.event === "job_updated") {
        upsertJob(msg.data, false);
      }
    } catch (e) {
      console.warn("WS parse error:", e);
    }
  };

  ws.onerror = (err) => {
    console.warn("WebSocket error:", err);
  };

  ws.onclose = () => {
    setWsStatus("disconnected");
    setTimeout(initWebSocket, reconnectInterval);
    reconnectInterval = Math.min(reconnectInterval * 1.5, 10000);
  };
}

function setWsStatus(status) {
  if (status === "connected") {
    wsStatusDot.className = "w-2 h-2 rounded-full bg-emerald-400";
    wsStatusText.textContent = "Live Connected";
    wsStatusBadge.className = "flex items-center gap-1.5 px-3 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400";
  } else if (status === "connecting") {
    wsStatusDot.className = "w-2 h-2 rounded-full bg-amber-400 animate-pulse";
    wsStatusText.textContent = "Connecting WS...";
    wsStatusBadge.className = "flex items-center gap-1.5 px-3 py-1 rounded-full bg-amber-500/10 border border-amber-500/30 text-amber-400";
  } else {
    wsStatusDot.className = "w-2 h-2 rounded-full bg-rose-400";
    wsStatusText.textContent = "Reconnecting...";
    wsStatusBadge.className = "flex items-center gap-1.5 px-3 py-1 rounded-full bg-rose-500/10 border border-rose-500/30 text-rose-400";
  }
}

// Fetch Initial Jobs List
async function fetchJobs() {
  try {
    const res = await fetch("/api/jobs");
    if (!res.ok) throw new Error("Failed to load jobs");
    jobs = await res.json();
    renderJobsTable();
  } catch (err) {
    console.error("fetchJobs error:", err);
    jobsTableBody.innerHTML = `<tr><td colspan="8" class="text-center py-6 text-rose-400">Error loading jobs: ${err.message}</td></tr>`;
  }
}

function upsertJob(jobData, isNew = false) {
  const index = jobs.findIndex(j => j.job_id === jobData.job_id);
  if (index !== -1) {
    jobs[index] = { ...jobs[index], ...jobData };
  } else {
    jobs.unshift(jobData);
  }

  renderJobsTable();

  // If currently opened in modal, update live
  if (currentViewingJob && currentViewingJob.job_id === jobData.job_id) {
    openOutputModal(jobs.find(j => j.job_id === jobData.job_id));
  }
}

function getStatusBadge(status) {
  switch (status?.toLowerCase()) {
    case "pending":
      return `<span class="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-[11px] font-semibold bg-amber-500/10 text-amber-400 border border-amber-500/30"><span class="w-1.5 h-1.5 rounded-full bg-amber-400 animate-pulse"></span>Pending</span>`;
    case "running":
      return `<span class="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-[11px] font-semibold bg-blue-500/10 text-blue-400 border border-blue-500/30"><span class="w-1.5 h-1.5 rounded-full bg-blue-400 animate-spin"></span>Running</span>`;
    case "success":
      return `<span class="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-[11px] font-semibold bg-emerald-500/10 text-emerald-400 border border-emerald-500/30"><span class="w-1.5 h-1.5 rounded-full bg-emerald-400"></span>Success</span>`;
    case "failed":
      return `<span class="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-[11px] font-semibold bg-rose-500/10 text-rose-400 border border-rose-500/30"><span class="w-1.5 h-1.5 rounded-full bg-rose-400"></span>Failed</span>`;
    default:
      return `<span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-[11px] font-semibold bg-slate-800 text-slate-400">${status || "Unknown"}</span>`;
  }
}

function formatDate(isoStr) {
  if (!isoStr) return "-";
  try {
    const d = new Date(isoStr);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
  } catch {
    return isoStr;
  }
}

function renderJobsTable() {
  jobCountBadge.textContent = `${jobs.length} jobs`;
  if (!jobs || jobs.length === 0) {
    jobsTableBody.innerHTML = `<tr><td colspan="8" class="text-center py-10 text-slate-500">No jobs dispatched yet. Submit a command above!</td></tr>`;
    return;
  }

  jobsTableBody.innerHTML = jobs.map(job => {
    const paramsStr = Object.keys(job.params || {}).length > 0 
      ? JSON.stringify(job.params) 
      : '<span class="text-slate-600">-</span>';
    const shortId = job.job_id.substring(0, 8);

    return `
      <tr class="hover:bg-slate-800/40 transition">
        <td class="py-3 px-4">${getStatusBadge(job.status)}</td>
        <td class="py-3 px-4 font-mono-code text-cyan-400 flex items-center gap-1">
          <span title="${job.job_id}">${shortId}...</span>
          <button onclick="navigator.clipboard.writeText('${job.job_id}')" class="text-slate-500 hover:text-slate-300" title="Copy Job UUID">
            <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/></svg>
          </button>
        </td>
        <td class="py-3 px-4 font-semibold text-slate-200 uppercase">${job.router_id}</td>
        <td class="py-3 px-4 font-mono-code text-slate-300">${job.action}</td>
        <td class="py-3 px-4 font-mono-code text-slate-400 text-[11px]">${paramsStr}</td>
        <td class="py-3 px-4 text-slate-400">${job.attempts || 0}</td>
        <td class="py-3 px-4 text-slate-400">${formatDate(job.created_at)}</td>
        <td class="py-3 px-4 text-right">
          <button onclick="viewJobDetails('${job.job_id}')" class="px-2.5 py-1 rounded bg-slate-800 hover:bg-cyan-500/20 hover:text-cyan-300 border border-slate-700 hover:border-cyan-500/40 text-slate-300 font-medium transition text-xs">
            Inspect
          </button>
        </td>
      </tr>
    `;
  }).join("");
}

// Modal View
window.viewJobDetails = (jobId) => {
  const job = jobs.find(j => j.job_id === jobId);
  if (job) {
    openOutputModal(job);
  }
};

function openOutputModal(job) {
  currentViewingJob = job;
  modalJobTitle.innerHTML = `Output: <span class="text-cyan-400 font-mono-code uppercase ml-1">${job.router_id}</span> - <span class="font-mono-code text-slate-300">${job.action}</span>`;
  modalJobMeta.textContent = `Job ID: ${job.job_id} | Status: ${job.status.toUpperCase()}`;

  if (job.error) {
    modalOutputErrorBox.classList.remove("hidden");
    modalOutputErrorText.textContent = job.error;
  } else {
    modalOutputErrorBox.classList.add("hidden");
    modalOutputErrorText.textContent = "";
  }

  modalOutputTerminal.textContent = job.output || (job.status === "pending" || job.status === "running" ? "Awaiting output from worker SSH session..." : "No output received.");

  let timingText = `Dispatched: ${job.created_at || "-"}`;
  if (job.started_at) timingText += ` | Started: ${formatDate(job.started_at)}`;
  if (job.finished_at) timingText += ` | Finished: ${formatDate(job.finished_at)}`;
  modalTimingInfo.textContent = timingText;

  outputModal.classList.remove("hidden");
}

function closeOutputModal() {
  outputModal.classList.add("hidden");
  currentViewingJob = null;
}

closeModalBtn.addEventListener("click", closeOutputModal);
closeModalBottomBtn.addEventListener("click", closeOutputModal);
outputModal.addEventListener("click", (e) => {
  if (e.target === outputModal) closeOutputModal();
});

copyOutputBtn.addEventListener("click", () => {
  if (currentViewingJob) {
    const textToCopy = (currentViewingJob.output || "") + (currentViewingJob.error ? `\nError: ${currentViewingJob.error}` : "");
    navigator.clipboard.writeText(textToCopy);
    copyOutputBtn.textContent = "Copied!";
    setTimeout(() => { copyOutputBtn.innerHTML = `<svg class="w-3.5 h-3.5 mr-1 inline" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/></svg>Copy`; }, 1500);
  }
});

// Handle Form Submit
jobForm.addEventListener("submit", async (e) => {
  e.preventDefault();
  formValidationMsg.textContent = "";

  const formData = new FormData(jobForm);
  const router_id = formData.get("router_id");
  const action = actionSelect.value;
  const params = {};

  if (action === "interface_shutdown" || action === "interface_no_shutdown") {
    const ifName = interfaceInput.value.trim();
    if (!ifName) {
      formValidationMsg.textContent = "Please enter an interface name (e.g. FastEthernet0/0)";
      formValidationMsg.className = "text-xs text-rose-400";
      return;
    }
    if (!INTERFACE_REGEX.test(ifName)) {
      formValidationMsg.textContent = "Invalid format! Examples: FastEthernet0/0, GigabitEthernet0/1";
      formValidationMsg.className = "text-xs text-rose-400";
      return;
    }
    params.interface = ifName;
  }

  // Set submitting state
  submitBtn.disabled = true;
  submitSpinner.classList.remove("hidden");
  submitBtnText.textContent = "Dispatching...";

  try {
    const response = await fetch("/api/jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        router_id,
        action,
        params,
        requested_by: "operator"
      })
    });

    const resJson = await response.json();
    if (!response.ok) {
      throw new Error(resJson.detail || "Submission failed");
    }

    formValidationMsg.textContent = `Job dispatched! ID: ${resJson.job_id.substring(0, 8)}...`;
    formValidationMsg.className = "text-xs text-emerald-400";

    // Clear interface field if any
    interfaceInput.value = "";
  } catch (err) {
    formValidationMsg.textContent = `Error: ${err.message}`;
    formValidationMsg.className = "text-xs text-rose-400";
  } finally {
    submitBtn.disabled = false;
    submitSpinner.classList.add("hidden");
    submitBtnText.textContent = "Dispatch Command to Kafka";
  }
});

refreshJobsBtn.addEventListener("click", fetchJobs);

// Initialize
fetchJobs();
initWebSocket();
