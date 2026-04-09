/**
 * Developer dashboard: token query, bulk smart-import queue, highlighted logs.
 */
document.addEventListener("DOMContentLoaded", () => {
    const urlsTa = document.getElementById("dev-smart-urls");
    const smartBtn = document.getElementById("dev-smart-import");
    const scanLog = document.getElementById("dev-scan-log");
    const discoveryList = document.getElementById("dev-discovery-list");
    const maintLog = document.getElementById("dev-maint-log");
    const tbody = document.getElementById("dev-dealers-tbody");
    const queuePanel = document.getElementById("dev-queue-panel");
    const queueTbody = document.getElementById("dev-queue-tbody");
    const geocodeBtn = document.getElementById("dev-geocode");
    const dedupeBtn = document.getElementById("dev-dedupe");
    const refreshStatus = document.getElementById("dev-refresh-status");
    const statusPanel = document.getElementById("dev-status-panel");

    let pollTimer = null;
    let queuePollTimer = null;
    let activeJobId = null;
    let currentQueueId = null;
    /** Last single-URL Smart Import target (for headed retry). */
    let lastSingleSmartUrl = null;

    const headedRetryWrap = document.getElementById("dev-headed-retry-wrap");
    const headedRetryBtn = document.getElementById("dev-headed-retry");

    function logIndicatesZeroVehicles(log) {
        if (!log) return false;
        if (/Upserted\s+0\s+unique vehicles/i.test(log)) return true;
        if (/SCAN_VEHICLE_COUNT:\s*0\b/.test(log)) return true;
        return false;
    }

    function getToken() {
        return new URLSearchParams(window.location.search).get("token") || "";
    }

    function apiUrl(path) {
        const t = getToken();
        if (!t) return path;
        const u = new URL(path, window.location.origin);
        u.searchParams.set("token", t);
        return u.pathname + u.search;
    }

    function escHtml(s) {
        const d = document.createElement("div");
        d.textContent = s;
        return d.innerHTML;
    }

    function colorizeLog(text) {
        if (!text) return "";
        return text
            .split("\n")
            .map((line) => {
                let cls = "dev-log-line";
                const lower = line.toLowerCase();
                if (line.includes("[turbo]")) cls += " dev-log-turbo";
                else if (line.includes("[slow]")) cls += " dev-log-slow";
                if (line.startsWith("DISCOVERY:")) cls += " dev-log-discovery";
                if (
                    /\b(error|failed|exception)\b/i.test(line) ||
                    lower.includes("smart_import_error") ||
                    line.includes("SMART_IMPORT_ERROR:")
                ) {
                    cls += " dev-log-err";
                }
                return `<span class="${cls}">${escHtml(line)}</span>`;
            })
            .join("\n");
    }

    function renderLog(el, raw) {
        if (!el) return;
        el.innerHTML = colorizeLog(raw);
    }

    function showMaint(obj) {
        maintLog.textContent = typeof obj === "string" ? obj : JSON.stringify(obj, null, 2);
    }

    function parseUrlLines() {
        const raw = (urlsTa && urlsTa.value) || "";
        return raw
            .split("\n")
            .map((s) => s.trim())
            .filter(Boolean);
    }

    function listingsFilterUrl(registryId) {
        const u = new URL("/search", window.location.origin);
        u.searchParams.set("dealership_registry_id", String(registryId));
        return u.pathname + u.search;
    }

    async function refreshDealersTable() {
        const res = await fetch(apiUrl("/api/dev/dealers"));
        const data = await res.json();
        if (!data.ok || !tbody) return;
        const rows = data.dealerships || [];
        if (!rows.length) {
            tbody.innerHTML =
                '<tr class="dev-empty-row"><td colspan="7">No dealerships yet. Run a smart import above.</td></tr>';
            return;
        }
        tbody.innerHTML = "";
        for (const d of rows) {
            const tr = document.createElement("tr");
            tr.dataset.id = String(d.id);
            const latlon =
                d.latitude != null && d.longitude != null
                    ? `${Number(d.latitude).toFixed(4)}, ${Number(d.longitude).toFixed(4)}`
                    : "—";
            const dup = d.duplicate_of_id ? `#${d.duplicate_of_id}` : "—";

            const tdName = document.createElement("td");
            tdName.textContent = d.name;

            const tdUrl = document.createElement("td");
            const link = document.createElement("a");
            link.href = d.website_url;
            link.target = "_blank";
            link.rel = "noopener";
            link.className = "dev-table-link";
            link.textContent = d.website_url;
            tdUrl.appendChild(link);

            const tdLoc = document.createElement("td");
            tdLoc.textContent = `${d.city}, ${d.state}`;

            const tdLat = document.createElement("td");
            tdLat.className = "dev-mono";
            tdLat.textContent = latlon;

            const tdWhen = document.createElement("td");
            tdWhen.className = "dev-mono";
            tdWhen.textContent = d.created_at || "";

            const tdDup = document.createElement("td");
            tdDup.textContent = dup;

            const tdDel = document.createElement("td");
            const del = document.createElement("button");
            del.type = "button";
            del.className = "dev-delete-btn";
            del.dataset.id = String(d.id);
            del.textContent = "Delete";
            tdDel.appendChild(del);

            tr.append(tdName, tdUrl, tdLoc, tdLat, tdWhen, tdDup, tdDel);
            tbody.appendChild(tr);
        }
        tbody.querySelectorAll(".dev-delete-btn").forEach((btn) =>
            btn.addEventListener("click", onDelete)
        );
    }

    async function onDelete(ev) {
        if (!getToken()) {
            alert("Missing token in URL.");
            return;
        }
        const id = ev.target && ev.target.dataset ? ev.target.dataset.id : null;
        if (!id) return;
        if (!window.confirm(`Delete dealership #${id}?`)) return;
        const res = await fetch(apiUrl(`/api/dev/dealer/${id}`), { method: "DELETE" });
        const data = await res.json();
        if (data.ok) refreshDealersTable();
        else showMaint({ error: data.error || "delete failed" });
    }

    document.querySelectorAll(".dev-delete-btn").forEach((btn) => {
        btn.addEventListener("click", onDelete);
    });

    function stopPoll() {
        if (pollTimer) {
            clearInterval(pollTimer);
            pollTimer = null;
        }
    }

    function stopQueuePoll() {
        if (queuePollTimer) {
            clearInterval(queuePollTimer);
            queuePollTimer = null;
        }
    }

    function renderDiscovery(events) {
        if (!discoveryList) return;
        discoveryList.innerHTML = "";
        if (!events || !events.length) {
            const li = document.createElement("li");
            li.className = "dev-discovery-placeholder";
            li.textContent = "No discovery steps yet.";
            discoveryList.appendChild(li);
            return;
        }
        for (const ev of events) {
            const li = document.createElement("li");
            li.className = "dev-discovery-item";
            li.textContent = ev.message || ev.step || JSON.stringify(ev);
            discoveryList.appendChild(li);
        }
    }

    async function pollScannerJob(jobId) {
        const res = await fetch(apiUrl(`/api/dev/scanner-job/${jobId}`));
        const data = await res.json();
        if (!data.ok) {
            renderLog(scanLog, data.error || "Job not found.");
            stopPoll();
            smartBtn.disabled = false;
            return;
        }
        renderLog(scanLog, data.log || "");
        if (Array.isArray(data.discovery) && data.discovery.length) {
            renderDiscovery(data.discovery);
        }
        if (data.done) {
            stopPoll();
            smartBtn.disabled = false;
            let tail = `\n---\nExit code: ${data.exit_code ?? "unknown"}\n`;
            if (data.insert_id != null) tail += `Saved dealership id: ${data.insert_id}\n`;
            if (data.cars_linked != null) tail += `Cars linked to registry: ${data.cars_linked}\n`;
            if (data.insert_error) tail += `Insert validation: ${JSON.stringify(data.insert_error)}\n`;
            if (data.smart_error) tail += `Smart import: ${JSON.stringify(data.smart_error)}\n`;
            renderLog(scanLog, (data.log || "") + tail);
            if (headedRetryWrap) {
                headedRetryWrap.hidden = !(
                    lastSingleSmartUrl &&
                    logIndicatesZeroVehicles((data.log || "") + tail)
                );
            }
            refreshDealersTable();
        }
    }

    function renderQueueTable(items) {
        if (!queueTbody || !queuePanel) return;
        queuePanel.hidden = !items || !items.length;
        queueTbody.innerHTML = "";
        for (const row of items) {
            const tr = document.createElement("tr");
            const tdU = document.createElement("td");
            tdU.textContent = row.url.length > 56 ? row.url.slice(0, 54) + "…" : row.url;
            tdU.title = row.url;

            const tdS = document.createElement("td");
            tdS.textContent = row.queue_status || "—";

            const tdR = document.createElement("td");
            tdR.textContent = row.insert_id != null ? `#${row.insert_id}` : "—";

            const tdA = document.createElement("td");
            if (row.insert_id && row.queue_status === "completed") {
                const a = document.createElement("a");
                a.href = listingsFilterUrl(row.insert_id);
                a.className = "dev-view-site-btn";
                a.textContent = "View on site";
                tdA.appendChild(a);
            } else {
                tdA.textContent = "—";
            }

            tr.append(tdU, tdS, tdR, tdA);
            queueTbody.appendChild(tr);
        }
    }

    async function pollImportQueue() {
        if (!currentQueueId) return;
        const res = await fetch(apiUrl(`/api/dev/import-queue/${currentQueueId}`));
        const data = await res.json();
        if (!data.ok) return;
        renderQueueTable(data.items || []);

        const processing = (data.items || []).find((x) => x.queue_status === "processing");
        if (processing && processing.job_id) {
            activeJobId = processing.job_id;
            const jr = await fetch(apiUrl(`/api/dev/scanner-job/${activeJobId}`));
            const jd = await jr.json();
            if (jd.ok) {
                renderLog(scanLog, jd.log || "");
                if (jd.discovery) renderDiscovery(jd.discovery);
            }
        }

        if (data.queue_done) {
            stopQueuePoll();
            smartBtn.disabled = false;
            const last = (data.items || []).filter((x) => x.job_id).pop();
            if (last && last.job_id) {
                const jr = await fetch(apiUrl(`/api/dev/scanner-job/${last.job_id}`));
                const jd = await jr.json();
                if (jd.ok) renderLog(scanLog, jd.log || "");
            }
            refreshDealersTable();
        }
    }

    if (headedRetryBtn) {
        headedRetryBtn.addEventListener("click", async () => {
            if (!getToken()) {
                renderLog(scanLog, "Missing access token. Open this page with ?token=… in the URL.");
                return;
            }
            if (!lastSingleSmartUrl) return;
            if (headedRetryWrap) headedRetryWrap.hidden = true;
            smartBtn.disabled = true;
            renderDiscovery([{ message: "Starting headed retry…" }]);
            renderLog(scanLog, "Retrying with visible browser (headed)…\n");
            const res = await fetch(apiUrl("/api/dev/smart-import"), {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ url: lastSingleSmartUrl, token: getToken(), headed: true }),
            });
            const data = await res.json();
            if (!res.ok || !data.job_id) {
                renderLog(scanLog, data.error || "Failed to start headed retry.");
                smartBtn.disabled = false;
                return;
            }
            currentQueueId = null;
            queuePanel.hidden = true;
            stopQueuePoll();
            activeJobId = data.job_id;
            pollTimer = setInterval(() => pollScannerJob(data.job_id), 400);
            pollScannerJob(data.job_id);
        });
    }

    if (smartBtn && urlsTa && scanLog) {
        smartBtn.addEventListener("click", async () => {
            if (!getToken()) {
                renderLog(scanLog, "Missing access token. Open this page with ?token=… in the URL.");
                return;
            }
            const urls = parseUrlLines();
            if (!urls.length) {
                renderLog(scanLog, "Enter at least one URL (one per line).");
                return;
            }
            smartBtn.disabled = true;
            if (headedRetryWrap) headedRetryWrap.hidden = true;
            renderDiscovery([{ message: "Starting…" }]);
            renderLog(scanLog, "Starting import jobs…\n");

            if (urls.length === 1) {
                lastSingleSmartUrl = urls[0];
                const res = await fetch(apiUrl("/api/dev/smart-import"), {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ url: urls[0], token: getToken() }),
                });
                const data = await res.json();
                if (!res.ok || !data.job_id) {
                    renderLog(scanLog, data.error || "Failed to start job.");
                    smartBtn.disabled = false;
                    return;
                }
                currentQueueId = null;
                queuePanel.hidden = true;
                stopQueuePoll();
                activeJobId = data.job_id;
                pollTimer = setInterval(() => pollScannerJob(data.job_id), 400);
                pollScannerJob(data.job_id);
                return;
            }

            lastSingleSmartUrl = null;
            const res = await fetch(apiUrl("/api/dev/smart-import-bulk"), {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ urls, token: getToken() }),
            });
            const data = await res.json();
            if (!res.ok || !data.queue_id) {
                renderLog(scanLog, data.error || "Failed to start bulk import.");
                smartBtn.disabled = false;
                return;
            }
            currentQueueId = data.queue_id;
            renderQueueTable(data.items || []);
            queuePanel.hidden = false;
            stopPoll();
            queuePollTimer = setInterval(pollImportQueue, 600);
            pollImportQueue();
        });
    }

    if (geocodeBtn) {
        geocodeBtn.addEventListener("click", async () => {
            if (!getToken()) {
                showMaint("Missing token in URL.");
                return;
            }
            geocodeBtn.disabled = true;
            showMaint("Running geocoding…");
            try {
                const res = await fetch(apiUrl("/api/dev/geocode-missing"), { method: "POST" });
                const data = await res.json();
                showMaint(data);
                refreshDealersTable();
            } catch (err) {
                showMaint(String(err));
            }
            geocodeBtn.disabled = false;
        });
    }

    if (dedupeBtn) {
        dedupeBtn.addEventListener("click", async () => {
            if (!getToken()) {
                showMaint("Missing token in URL.");
                return;
            }
            dedupeBtn.disabled = true;
            showMaint("Running deduplication…");
            try {
                const res = await fetch(apiUrl("/api/dev/deduplicate"), { method: "POST" });
                const data = await res.json();
                showMaint(data);
                refreshDealersTable();
            } catch (err) {
                showMaint(String(err));
            }
            dedupeBtn.disabled = false;
        });
    }

    async function refreshStatusPanel() {
        if (!statusPanel) return;
        const res = await fetch(apiUrl("/api/dev/status"));
        const data = await res.json();
        if (!data.ok) return;
        const dbCls = data.db_connected ? "dev-ok" : "dev-bad";
        const nodeCls = data.node_executable ? "dev-ok" : "dev-bad";
        statusPanel.innerHTML = `
            <div class="dev-status-card">
                <p class="dev-status-label">inventory.db</p>
                <p class="dev-status-value ${dbCls}">${data.db_connected ? "Connected" : "Unavailable"}</p>
                <p class="dev-status-meta">${data.inventory_db_path || ""}</p>
            </div>
            <div class="dev-status-card">
                <p class="dev-status-label">Node.js</p>
                <p class="dev-status-value ${nodeCls}">${data.node_executable ? "Detected" : "Not found"}</p>
                <p class="dev-status-meta">${data.node_executable || "Install Node 18+ and ensure it is on PATH."}</p>
            </div>`;
    }

    if (refreshStatus) {
        refreshStatus.addEventListener("click", refreshStatusPanel);
    }
});
