/**
 * Developer dashboard at /dev: session cookie auth, bulk smart-import queue, highlighted logs.
 */
function readCsrfToken() {
    const m = document.querySelector('meta[name="csrf-token"]');
    return m ? (m.getAttribute("content") || "").trim() : "";
}

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
    /** null | "smart" — which bulk queue API to poll */
    let currentQueueKind = null;
    /** Last single-URL Smart Import target (for headed retry). */
    let lastSingleSmartUrl = null;
    /** Avoid re-rendering scanner log every poll tick (preserves text selection while job runs). */
    let lastScanLogSnapshot = "";
    let lastDiscoverySnapshot = "";

    const headedRetryWrap = document.getElementById("dev-headed-retry-wrap");
    const headedRetryBtn = document.getElementById("dev-headed-retry");

    function setImportButtonsDisabled(disabled) {
        if (smartBtn) smartBtn.disabled = disabled;
    }

    function logIndicatesZeroVehicles(log) {
        if (!log) return false;
        if (/Upserted\s+0\s+unique vehicles/i.test(log)) return true;
        if (/SCAN_VEHICLE_COUNT:\s*0\b/.test(log)) return true;
        return false;
    }

    /** e.g. "dealers" or "/status" -> "/dev/api/dealers" */
    function devApi(endpoint) {
        const e = String(endpoint).replace(/^\//, "");
        return `/dev/api/${e}`;
    }

    const fetchOpts = { credentials: "same-origin" };

    async function devFetch(url, options = {}) {
        const opts = { ...fetchOpts, ...options };
        const method = String(opts.method || "GET").toUpperCase();
        if (!["GET", "HEAD", "OPTIONS"].includes(method)) {
            const t = readCsrfToken();
            if (t) {
                opts.headers = { ...(opts.headers || {}), "X-CSRF-Token": t };
            }
        }
        const res = await fetch(url, opts);
        if (res.status === 401) {
            window.location.href = "/dev/login?next=" + encodeURIComponent(window.location.pathname);
            throw new Error("unauthorized");
        }
        return res;
    }

    function escHtml(s) {
        const d = document.createElement("div");
        d.textContent = s;
        return d.innerHTML;
    }

    function isHttpUrl(s) {
        const t = String(s || "").trim();
        return /^https?:\/\//i.test(t);
    }

    function cssSingleQuotedUrl(url) {
        const raw = String(url || "").trim();
        if (!isHttpUrl(raw)) return "/static/placeholder.svg";
        try {
            const abs = new URL(raw);
            if (abs.protocol !== "http:" && abs.protocol !== "https:") {
                return "/static/placeholder.svg";
            }
            return abs.href.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
        } catch (_) {
            return "/static/placeholder.svg";
        }
    }

    const QUEUE_STALE_MSG = "Server restarted — refresh /dev and start the job again.";

    function stopBulkQueueOnStale(reasonLine) {
        stopQueuePoll();
        stopPoll();
        currentQueueId = null;
        currentQueueKind = null;
        queuePanel.hidden = true;
        setImportButtonsDisabled(false);
        const line = reasonLine || QUEUE_STALE_MSG;
        if (scanLog) renderLog(scanLog, `\n${line}\n`);
        syncScanPollSnapshots(line, [{ message: line }]);
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

    function syncScanPollSnapshots(logText, discoveryArr) {
        lastScanLogSnapshot = logText || "";
        lastDiscoverySnapshot = JSON.stringify(Array.isArray(discoveryArr) ? discoveryArr : []);
    }

    function maybeRenderScanPoll(logText, discoveryArr) {
        if (!scanLog) return;
        const disc = Array.isArray(discoveryArr) ? discoveryArr : [];
        const dStr = JSON.stringify(disc);
        const lt = logText || "";
        if (lt === lastScanLogSnapshot && dStr === lastDiscoverySnapshot) {
            return;
        }
        lastScanLogSnapshot = lt;
        lastDiscoverySnapshot = dStr;
        requestAnimationFrame(() => {
            renderLog(scanLog, lt);
            if (disc.length) renderDiscovery(disc);
        });
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
        const res = await devFetch(devApi("dealers"));
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
        const id = ev.target && ev.target.dataset ? ev.target.dataset.id : null;
        if (!id) return;
        if (!window.confirm(`Delete dealership #${id}?`)) return;
        const res = await devFetch(devApi(`dealer/${id}`), { method: "DELETE" });
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
        const res = await devFetch(devApi(`scanner-job/${jobId}`));
        let data = {};
        try {
            data = await res.json();
        } catch (_) {
            data = { ok: false };
        }
        if (res.status === 404 || data.error === "unknown job" || data.error === "job_expired") {
            const msg = data.message || data.error || "Job not found.";
            renderLog(scanLog, msg);
            syncScanPollSnapshots(msg, []);
            stopPoll();
            if (currentQueueId) {
                stopBulkQueueOnStale(data.message);
            } else {
                setImportButtonsDisabled(false);
            }
            return;
        }
        if (!data.ok) {
            renderLog(scanLog, data.error || "Job not found.");
            syncScanPollSnapshots(data.error || "Job not found.", []);
            stopPoll();
            setImportButtonsDisabled(false);
            return;
        }
        if (data.done) {
            stopPoll();
            setImportButtonsDisabled(false);
            let tail = `\n---\nExit code: ${data.exit_code ?? "unknown"}\n`;
            if (data.insert_id != null) tail += `Saved dealership id: ${data.insert_id}\n`;
            if (data.cars_linked != null) tail += `Cars linked to registry: ${data.cars_linked}\n`;
            if (data.insert_error) tail += `Insert validation: ${JSON.stringify(data.insert_error)}\n`;
            if (data.smart_error) tail += `Smart import: ${JSON.stringify(data.smart_error)}\n`;
            const finalLog = (data.log || "") + tail;
            renderLog(scanLog, finalLog);
            if (Array.isArray(data.discovery) && data.discovery.length) {
                renderDiscovery(data.discovery);
            }
            syncScanPollSnapshots(finalLog, data.discovery || []);
            if (headedRetryWrap) {
                headedRetryWrap.hidden = !(
                    lastSingleSmartUrl &&
                    logIndicatesZeroVehicles(finalLog)
                );
            }
            refreshDealersTable();
            refreshIncompleteCars();
            return;
        }
        maybeRenderScanPoll(data.log || "", data.discovery);
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
        const qPath = `import-queue/${currentQueueId}`;
        const res = await devFetch(devApi(qPath));
        let data = {};
        try {
            data = await res.json();
        } catch (_) {
            data = { ok: false };
        }
        if (res.status === 404 || data.error === "unknown queue" || data.error === "queue_expired") {
            stopBulkQueueOnStale(data.message);
            return;
        }
        if (!data.ok) return;
        renderQueueTable(data.items || []);

        const processing = (data.items || []).find((x) => x.queue_status === "processing");
        if (processing && processing.job_id) {
            activeJobId = processing.job_id;
            const jr = await devFetch(devApi(`scanner-job/${activeJobId}`));
            let jd = {};
            try {
                jd = await jr.json();
            } catch (_) {
                jd = { ok: false };
            }
            if (jr.status === 404 || jd.error === "unknown job" || jd.error === "job_expired") {
                stopBulkQueueOnStale(jd.message);
                return;
            }
            if (jd.ok) {
                maybeRenderScanPoll(jd.log || "", jd.discovery);
            }
        }

        if (data.queue_done) {
            stopQueuePoll();
            setImportButtonsDisabled(false);
            const last = (data.items || []).filter((x) => x.job_id).pop();
            if (last && last.job_id) {
                const jr = await devFetch(devApi(`scanner-job/${last.job_id}`));
                const jd = await jr.json();
                if (jd.ok) {
                    renderLog(scanLog, jd.log || "");
                    if (Array.isArray(jd.discovery) && jd.discovery.length) {
                        renderDiscovery(jd.discovery);
                    }
                    syncScanPollSnapshots(jd.log || "", jd.discovery || []);
                }
            }
            refreshDealersTable();
            refreshIncompleteCars();
        }
    }

    if (headedRetryBtn) {
        headedRetryBtn.addEventListener("click", async () => {
            if (!lastSingleSmartUrl) return;
            if (headedRetryWrap) headedRetryWrap.hidden = true;
            setImportButtonsDisabled(true);
            renderDiscovery([{ message: "Starting headed retry…" }]);
            renderLog(scanLog, "Retrying with visible browser (headed)…\n");
            syncScanPollSnapshots("Retrying with visible browser (headed)…\n", [
                { message: "Starting headed retry…" },
            ]);
            const res = await devFetch(devApi("smart-import"), {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ url: lastSingleSmartUrl, headed: true }),
            });
            const data = await res.json();
            if (!res.ok || !data.job_id) {
                const err = data.error || "Failed to start headed retry.";
                renderLog(scanLog, err);
                syncScanPollSnapshots(err, []);
                setImportButtonsDisabled(false);
                return;
            }
            currentQueueId = null;
            currentQueueKind = null;
            queuePanel.hidden = true;
            stopQueuePoll();
            activeJobId = data.job_id;
            pollTimer = setInterval(() => pollScannerJob(data.job_id), 400);
            pollScannerJob(data.job_id);
        });
    }

    if (smartBtn && urlsTa && scanLog) {
        smartBtn.addEventListener("click", async () => {
            const urls = parseUrlLines();
            if (!urls.length) {
                renderLog(scanLog, "Enter at least one URL (one per line).");
                syncScanPollSnapshots("Enter at least one URL (one per line).", []);
                return;
            }
            setImportButtonsDisabled(true);
            if (headedRetryWrap) headedRetryWrap.hidden = true;
            renderDiscovery([{ message: "Starting…" }]);
            renderLog(scanLog, "Starting import jobs…\n");
            syncScanPollSnapshots("Starting import jobs…\n", [{ message: "Starting…" }]);

            if (urls.length === 1) {
                lastSingleSmartUrl = urls[0];
                const res = await devFetch(devApi("smart-import"), {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ url: urls[0] }),
                });
                const data = await res.json();
                if (!res.ok || !data.job_id) {
                    const err = data.error || "Failed to start job.";
                    renderLog(scanLog, err);
                    syncScanPollSnapshots(err, []);
                    setImportButtonsDisabled(false);
                    return;
                }
                currentQueueId = null;
                currentQueueKind = null;
                queuePanel.hidden = true;
                stopQueuePoll();
                activeJobId = data.job_id;
                pollTimer = setInterval(() => pollScannerJob(data.job_id), 400);
                pollScannerJob(data.job_id);
                return;
            }

            lastSingleSmartUrl = null;
            const res = await devFetch(devApi("smart-import-bulk"), {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ urls }),
            });
            const data = await res.json();
            if (!res.ok || !data.queue_id) {
                const err = data.error || "Failed to start bulk import.";
                renderLog(scanLog, err);
                syncScanPollSnapshots(err, []);
                setImportButtonsDisabled(false);
                return;
            }
            currentQueueKind = "smart";
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
            geocodeBtn.disabled = true;
            showMaint("Running geocoding…");
            try {
                const res = await devFetch(devApi("geocode-missing"), { method: "POST" });
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
            dedupeBtn.disabled = true;
            showMaint("Running deduplication…");
            try {
                const res = await devFetch(devApi("deduplicate"), { method: "POST" });
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
        const res = await devFetch(devApi("status"));
        const data = await res.json();
        if (!data.ok) return;
        const dbCls = data.db_connected ? "dev-ok" : "dev-bad";
        const nodeCls = data.node_version ? "dev-ok" : "dev-bad";
        const adminCls = data.admin_password_configured ? "dev-ok" : "dev-bad";
        const regCls = data.dev_registration_open ? "dev-ok" : "dev-bad";
        const dbPath = escHtml(data.inventory_db_path || "");
        const nodePath = escHtml(
            data.node_status_line ||
                data.node_executable ||
                "Install Node 18+; server probes PATH, NODE_BINARY, Homebrew, and nvm."
        );
        const nodeVer = escHtml(data.node_version || "Not verified");
        const adminMeta = data.is_production
            ? "Production requires ADMIN_PASSWORD."
            : "Non-production may use env defaults.";
        const regMeta = data.is_production
            ? "Production: set ALLOW_DEV_PUBLIC_REGISTER=1 to allow /dev/register."
            : "Non-prod: set DEV_DISABLE_PUBLIC_REGISTER=1 to close.";
        const dotenvLine = data.dotenv_file_present
            ? `.env present (${escHtml(data.dotenv_file_path || "")}) — loaded into env on server start.`
            : "No repo .env file; use shell exports or add .env at project root.";
        statusPanel.innerHTML = `
            <div class="dev-status-card">
                <p class="dev-status-label">inventory.db</p>
                <p class="dev-status-value ${dbCls}">${data.db_connected ? "Connected" : "Unavailable"}</p>
                <p class="dev-status-meta">${dbPath}</p>
            </div>
            <div class="dev-status-card">
                <p class="dev-status-label">Node.js</p>
                <p class="dev-status-value ${nodeCls}">${nodeVer}</p>
                <p class="dev-status-meta" style="white-space: pre-wrap; word-break: break-word;">${nodePath}</p>
            </div>
            <div class="dev-status-card">
                <p class="dev-status-label">Dev accounts DB</p>
                <p class="dev-status-value dev-ok">dev_users.db</p>
                <p class="dev-status-meta">${escHtml(data.dev_users_db_path || "")}</p>
            </div>
            <div class="dev-status-card">
                <p class="dev-status-label">Admin bootstrap</p>
                <p class="dev-status-value ${adminCls}">${data.admin_password_configured ? "Ready" : "ADMIN_PASSWORD unset"}</p>
                <p class="dev-status-meta">${escHtml(adminMeta)}</p>
            </div>
            <div class="dev-status-card">
                <p class="dev-status-label">Dev self-register</p>
                <p class="dev-status-value ${regCls}">${data.dev_registration_open ? "Open" : "Closed"}</p>
                <p class="dev-status-meta">${escHtml(regMeta)} ${escHtml(dotenvLine)}</p>
            </div>`;
    }

    if (refreshStatus) {
        refreshStatus.addEventListener("click", refreshStatusPanel);
    }

    // ── Incomplete cars section ──

    const incompleteGrid = document.getElementById("dev-incomplete-grid");
    const incompleteCount = document.getElementById("dev-incomplete-count");
    const refreshIncomplete = document.getElementById("dev-refresh-incomplete");

    function fmtNumber(n) { return Number(n).toLocaleString(); }
    function fmtUSD(n) {
        if (n == null || n === "" || Number(n) === 0) return "No price";
        return "$" + Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 });
    }

    const INCOMPLETE_FIELD_LABELS = {
        title: "Title",
        images: "Images / photos",
        price: "Price",
        year: "Year",
        make: "Make",
        model: "Model",
        trim: "Trim",
        engine: "Engine",
        transmission: "Transmission",
        drivetrain: "Drivetrain",
        body_style: "Body style",
        fuel_type: "Fuel type",
        condition: "Condition",
        cylinders: "Cylinders",
        exterior_color: "Exterior color",
        interior_color: "Interior color",
        vin: "VIN",
    };

    function missingTags(c) {
        if (Array.isArray(c.incomplete_missing_fields) && c.incomplete_missing_fields.length) {
            return c.incomplete_missing_fields.map(
                (code) => INCOMPLETE_FIELD_LABELS[code] || code
            );
        }
        const bad = new Set(["", "n/a", "na", "null", "none", "unknown", "undefined", "-", "--", "---"]);
        const isMissing = (v) => v == null || bad.has(String(v).trim().toLowerCase());
        const isPlaceholder = (v) => v != null && String(v).trim() !== "" && bad.has(String(v).trim().toLowerCase());
        const tags = [];
        if (isMissing(c.title)) tags.push("No title");
        const gallery = Array.isArray(c.gallery) ? c.gallery : [];
        const hasImg =
            (gallery.length && isHttpUrl(gallery[0])) || (c.image_url && isHttpUrl(c.image_url));
        if (!hasImg) tags.push("No images");
        if (isMissing(c.make)) tags.push("No make");
        if (isMissing(c.model)) tags.push("No model");
        if (!c.price || c.price === 0) tags.push("No price");
        if (isPlaceholder(c.transmission)) tags.push("Transmission: " + c.transmission);
        if (isPlaceholder(c.drivetrain)) tags.push("Drivetrain: " + c.drivetrain);
        if (isPlaceholder(c.fuel_type)) tags.push("Fuel type: " + c.fuel_type);
        if (isPlaceholder(c.exterior_color)) tags.push("Ext. color: " + c.exterior_color);
        if (isPlaceholder(c.interior_color)) tags.push("Int. color: " + c.interior_color);
        return tags;
    }

    function summarizeIncompleteFromCars(cars) {
        const counts = new Map();
        for (const c of cars) {
            const f = c.incomplete_missing_fields;
            if (!Array.isArray(f)) continue;
            for (const code of f) {
                if (!code || typeof code !== "string") continue;
                const k = code.trim();
                if (!k) continue;
                counts.set(k, (counts.get(k) || 0) + 1);
            }
        }
        const n = cars.length;
        return Array.from(counts.entries())
            .sort((a, b) => b[1] - a[1])
            .map(([code, count]) => ({
                code,
                label: INCOMPLETE_FIELD_LABELS[code] || code,
                count,
                pct: n ? Math.round((1000 * count) / n) / 10 : 0,
            }));
    }

    function renderIssuesSummary(cars, issuesSummary) {
        const el = document.getElementById("dev-incomplete-issues");
        if (!el) return;
        if (!cars.length) {
            el.innerHTML = "";
            el.style.display = "none";
            return;
        }
        el.style.display = "";
        const rows =
            Array.isArray(issuesSummary) && issuesSummary.length
                ? issuesSummary
                : summarizeIncompleteFromCars(cars);
        if (!rows.length) {
            el.innerHTML = "";
            return;
        }
        const n = cars.length;
        const items = rows
            .map((r) => {
                const cnt = Number(r.count) || 0;
                const pct = r.pct != null ? String(r.pct) : "";
                const plural = cnt === 1 ? "" : "s";
                return `<li><span class="dev-incomplete-issues-label">${escHtml(r.label || r.code)}</span> <span class="dev-incomplete-issues-meta">${fmtNumber(cnt)} car${plural} (${escHtml(pct)}%)</span></li>`;
            })
            .join("");
        el.innerHTML = `<h3 class="dev-incomplete-issues-title">What to fix first</h3><p class="dev-incomplete-issues-lead">Counts are across all <strong>${fmtNumber(
            n
        )}</strong> incomplete row(s) below (a car can have multiple gaps).</p><ol class="dev-incomplete-issues-list">${items}</ol>`;
    }

    function renderIncompleteGrid(cars, issuesSummary) {
        if (!incompleteGrid) return;
        if (incompleteCount) incompleteCount.textContent = cars.length;
        renderIssuesSummary(cars, issuesSummary);

        if (!cars.length) {
            incompleteGrid.innerHTML = '<div class="dev-incomplete-empty"><p>All cars have complete data.</p></div>';
            return;
        }

        incompleteGrid.innerHTML = cars.map(c => {
            const gallery = Array.isArray(c.gallery) ? c.gallery : [];
            const imgCandidate = (gallery.length && gallery[0]) ? gallery[0] : (c.image_url || "");
            const imgRaw = isHttpUrl(imgCandidate) ? imgCandidate : "";
            const imgQuoted = cssSingleQuotedUrl(imgRaw);
            const tags = missingTags(c)
                .map((t) => `<span class="dev-missing-tag">${escHtml(t)}</span>`)
                .join("");
            const idNum = Number(c.id);
            const idAttr = Number.isFinite(idNum) && idNum > 0 ? String(Math.floor(idNum)) : "0";
            return `
            <div class="result-card dev-incomplete-card" data-car-id="${idAttr}">
                <div class="result-image-wrap">
                    <div class="result-image" style="background-image:url('${imgQuoted}')"></div>
                    <span class="dev-incomplete-badge">Incomplete</span>
                </div>
                <div class="result-content">
                    <h2>${escHtml(c.title || "No title")}</h2>
                    <p class="result-trim">${escHtml(c.trim || "")}</p>
                    <p class="result-price">${fmtUSD(c.price)}</p>
                    <p class="result-meta">
                        ${c.mileage ? fmtNumber(c.mileage) + " mi" : "-- mi"}
                        &middot; ${escHtml(c.fuel_type || "--")}
                        &middot; ${escHtml(c.drivetrain || "--")}
                    </p>
                    <p class="result-dealer">${escHtml(c.dealer_name || "Unknown dealer")}</p>
                    <div class="dev-incomplete-missing">${tags}</div>
                    <div class="dev-incomplete-actions">
                        <a href="/car/${c.id}" class="dev-view-site-btn" target="_blank">View</a>
                        <button type="button" class="dev-delete-btn dev-delete-car-btn" data-car-id="${c.id}">Delete</button>
                    </div>
                </div>
            </div>`;
        }).join("");

        incompleteGrid.querySelectorAll(".dev-delete-car-btn").forEach(btn =>
            btn.addEventListener("click", onDeleteCar)
        );
    }

    async function onDeleteCar(ev) {
        const id = ev.target.dataset.carId;
        if (!id) return;
        if (!window.confirm(`Delete car #${id} from inventory?`)) return;
        const res = await devFetch(devApi(`incomplete-cars/${id}`), { method: "DELETE" });
        const data = await res.json();
        if (data.ok) {
            refreshIncompleteCars();
        } else {
            showMaint({ error: data.error || "delete failed" });
        }
    }

    async function refreshIncompleteCars() {
        const res = await devFetch(devApi("incomplete-cars"));
        const data = await res.json();
        if (data.ok) renderIncompleteGrid(data.cars || [], data.issues_summary);
    }

    if (refreshIncomplete) {
        refreshIncomplete.addEventListener("click", refreshIncompleteCars);
    }

    document.querySelectorAll(".dev-delete-car-btn").forEach(btn =>
        btn.addEventListener("click", onDeleteCar)
    );
});
