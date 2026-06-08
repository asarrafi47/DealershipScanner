/**
 * Dev scan lab: manifest scan job polling, scan_runs table, DB row editor.
 */
(function () {
    function readCsrfToken() {
        const m = document.querySelector('meta[name="csrf-token"]');
        return m ? (m.getAttribute("content") || "").trim() : "";
    }

    function appPathPrefix() {
        const m = document.querySelector('meta[name="application-path-prefix"]');
        const raw = m ? String(m.getAttribute("content") || "").trim() : "";
        return raw.replace(/\/$/, "");
    }

    function scanLabApi(path) {
        const p = String(path).replace(/^\//, "");
        const base = appPathPrefix();
        return base ? `${base}/dev/scan-lab/api/${p}` : `/dev/scan-lab/api/${p}`;
    }

    async function labFetch(url, options) {
        const opts = Object.assign({ credentials: "same-origin" }, options || {});
        const method = String(opts.method || "GET").toUpperCase();
        if (!["GET", "HEAD", "OPTIONS"].includes(method)) {
            const t = readCsrfToken();
            if (t) {
                opts.headers = Object.assign({}, opts.headers || {}, { "X-CSRF-Token": t });
            }
        }
        const res = await fetch(url, opts);
        if (res.status === 401) {
            const pre = appPathPrefix();
            window.location.href =
                (pre ? `${pre}/dev/login` : "/dev/login") +
                "?next=" +
                encodeURIComponent(window.location.pathname + window.location.search);
            return null;
        }
        return res;
    }

    let pollTimer = null;
    let activeJobId = null;

    function bootData() {
        const el = document.getElementById("scan-lab-boot");
        if (!el) return {};
        try {
            return JSON.parse(el.textContent || "{}");
        } catch (e) {
            return {};
        }
    }

    function setLog(text) {
        const log = document.getElementById("scan-lab-log");
        if (!log) return;
        log.textContent = text || "";
        log.scrollTop = log.scrollHeight;
    }

    async function pollJob(jobId) {
        const res = await labFetch(scanLabApi(`scan/${jobId}`));
        if (!res) return;
        const data = await res.json();
        if (!data.ok) return;
        setLog(data.log || "");
        if (!data.done) return;
        clearInterval(pollTimer);
        pollTimer = null;
        const btn = document.getElementById("scan-lab-start");
        if (btn) btn.disabled = false;
        loadScanRuns();
    }

    function startPolling(jobId) {
        activeJobId = jobId;
        const panel = document.getElementById("scan-lab-job-panel");
        if (panel) panel.hidden = false;
        if (pollTimer) clearInterval(pollTimer);
        pollTimer = setInterval(function () {
            pollJob(jobId);
        }, 2000);
        pollJob(jobId);
    }

    async function startScan() {
        const btn = document.getElementById("scan-lab-start");
        if (btn) btn.disabled = true;
        const boot = bootData();
        const res = await labFetch(scanLabApi("scan/start"), {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ manifest_path: boot.manifest_path || "" }),
        });
        if (!res) return;
        const data = await res.json();
        if (!data.ok) {
            if (btn) btn.disabled = false;
            alert(data.error || "Scan failed to start");
            return;
        }
        startPolling(data.job_id);
    }

    async function loadScanRuns() {
        const tbody = document.querySelector("#scan-lab-runs-table tbody");
        if (!tbody) return;
        const res = await labFetch(scanLabApi("scan-runs"));
        if (!res) return;
        const data = await res.json();
        if (!data.ok || !Array.isArray(data.runs)) return;
        if (!data.runs.length) {
            tbody.innerHTML = '<tr><td colspan="6">No scan_runs yet.</td></tr>';
            return;
        }
        tbody.innerHTML = data.runs
            .map(function (r) {
                const err = r.error ? String(r.error).slice(0, 80) : "";
                return (
                    "<tr>" +
                    "<td>" +
                    (r.finished_at || "") +
                    "</td>" +
                    "<td>" +
                    (r.dealer_name || r.dealer_id || "") +
                    "</td>" +
                    "<td>" +
                    (r.upserted != null ? r.upserted : "—") +
                    "</td>" +
                    "<td>" +
                    (r.inventory_rows != null ? r.inventory_rows : "—") +
                    "</td>" +
                    "<td>" +
                    (r.vdps_visited != null ? r.vdps_visited : "—") +
                    "</td>" +
                    "<td>" +
                    err +
                    "</td>" +
                    "</tr>"
                );
            })
            .join("");
    }

    let editingCarId = null;

    async function loadCarRow(carId) {
        const res = await labFetch(scanLabApi(`db/car/${carId}`));
        if (!res) return null;
        const data = await res.json();
        return data.ok ? data.car : null;
    }

    async function openEditor(carId) {
        editingCarId = carId;
        const panel = document.getElementById("scan-lab-editor");
        const idEl = document.getElementById("scan-lab-edit-id");
        const ta = document.getElementById("scan-lab-edit-json");
        const status = document.getElementById("scan-lab-edit-status");
        if (panel) panel.hidden = false;
        if (idEl) idEl.textContent = String(carId);
        if (status) status.textContent = "Loading…";
        const row = await loadCarRow(carId);
        if (!row) {
            if (status) status.textContent = "Not found.";
            return;
        }
        if (ta) ta.value = JSON.stringify(row, null, 2);
        if (status) status.textContent = "Loaded. Edit JSON values and Save PATCH (only known columns are applied).";
    }

    async function saveEditor() {
        if (!editingCarId) return;
        const ta = document.getElementById("scan-lab-edit-json");
        const status = document.getElementById("scan-lab-edit-status");
        let body;
        try {
            body = JSON.parse(ta ? ta.value : "{}");
        } catch (e) {
            if (status) status.textContent = "Invalid JSON: " + e.message;
            return;
        }
        const res = await labFetch(scanLabApi(`db/car/${editingCarId}`), {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        if (!res) return;
        const data = await res.json();
        if (status) {
            status.textContent = data.ok
                ? "Saved: " + (data.updated_fields || []).join(", ")
                : "Error: " + (data.error || "unknown");
        }
    }

    function listingsBoot() {
        const el = document.getElementById("scan-lab-listings-boot");
        if (!el) return {};
        try {
            return JSON.parse(el.textContent || "{}");
        } catch (e) {
            return {};
        }
    }

    function esc(s) {
        return String(s || "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    function formatPrice(n) {
        if (n == null || n === "") return "—";
        const v = Number(n);
        return Number.isFinite(v) ? "$" + v.toLocaleString("en-US", { maximumFractionDigits: 0 }) : "—";
    }

    function formatMiles(n) {
        if (n == null || n === "") return "—";
        const v = Number(n);
        return Number.isFinite(v) ? v.toLocaleString("en-US") : "—";
    }

    function renderListingRow(car, vdpBase) {
        const incomplete = car.public_incomplete ? " scan-lab-incomplete" : "";
        const img = car.image_url
            ? '<img src="' + esc(car.image_url) + '" alt="" class="scan-lab-thumb" loading="lazy">'
            : "—";
        const dq = car.data_quality_display != null ? esc(car.data_quality_display) : "—";
        const status = car.public_incomplete ? '<span class="dev-bad">incomplete</span>' : "ok";
        const vdp = vdpBase + "/" + encodeURIComponent(car.id);
        return (
            "<tr class=\"" + incomplete.trim() + "\">" +
            "<td>" + img + "</td>" +
            "<td><strong>" + esc(car.year) + " " + esc(car.make) + " " + esc(car.model) + "</strong>" +
            (car.trim ? "<br><span class=\"dev-hint\">" + esc(car.trim) + "</span>" : "") +
            "<br><code class=\"dev-hint\">" + esc(car.vin) + "</code></td>" +
            "<td>" + formatPrice(car.price) + "</td>" +
            "<td>" + formatMiles(car.mileage) + "</td>" +
            "<td>" + esc(car.dealer_name || car.dealer_id) + "</td>" +
            "<td>" + dq + (String(dq).indexOf("%") === -1 && dq !== "—" ? "%" : "") + "</td>" +
            "<td>" + status + "</td>" +
            "<td><a href=\"" + esc(vdp) + "\">Open VDP</a></td>" +
            "</tr>"
        );
    }

    async function loadListingsPage(reset) {
        const tbody = document.getElementById("scan-lab-listings-tbody");
        const moreBtn = document.getElementById("scan-lab-listings-more");
        const meta = document.getElementById("scan-lab-listings-meta");
        const sub = document.getElementById("scan-lab-listings-sub");
        if (!tbody) return;

        const boot = listingsBoot();
        if (!loadListingsPage._state || reset) {
            loadListingsPage._state = {
                offset: 0,
                total: 0,
                dealer_id: boot.dealer_id || "",
                vdpBase: boot.vdp_base || "/dev/scan-lab/car",
                loading: false,
                done: false,
            };
            tbody.innerHTML = "<tr><td colspan=\"8\">Loading…</td></tr>";
        }
        const st = loadListingsPage._state;
        if (st.loading || st.done) return;
        st.loading = true;

        const qs = new URLSearchParams({
            limit: "50",
            offset: String(st.offset),
        });
        if (st.dealer_id) qs.set("dealer_id", st.dealer_id);

        const res = await labFetch(scanLabApi("cars?" + qs.toString()));
        st.loading = false;
        if (!res) return;
        const data = await res.json();
        if (!data.ok) return;

        st.total = data.total || 0;
        const cars = data.cars || [];
        if (st.offset === 0 && !cars.length) {
            tbody.innerHTML = "<tr><td colspan=\"8\">No cars yet. <a href=\"/dev/scan-lab\">Run a scan</a>.</td></tr>";
        } else if (st.offset === 0) {
            tbody.innerHTML = cars.map(function (c) {
                return renderListingRow(c, st.vdpBase);
            }).join("");
        } else {
            tbody.insertAdjacentHTML(
                "beforeend",
                cars.map(function (c) {
                    return renderListingRow(c, st.vdpBase);
                }).join("")
            );
        }

        st.offset += cars.length;
        st.done = st.offset >= st.total || cars.length === 0;

        if (sub) {
            sub.textContent = st.total.toLocaleString() + " vehicles from manifest dealers. Click a row for full VDP.";
        }
        if (meta) {
            meta.textContent = "Showing " + Math.min(st.offset, st.total) + " of " + st.total;
        }
        if (moreBtn) moreBtn.hidden = st.done;
    }

    document.addEventListener("DOMContentLoaded", function () {
        const startBtn = document.getElementById("scan-lab-start");
        if (startBtn) startBtn.addEventListener("click", startScan);

        loadScanRuns();
        loadListingsPage(true);

        const moreBtn = document.getElementById("scan-lab-listings-more");
        if (moreBtn) {
            moreBtn.addEventListener("click", function () {
                loadListingsPage(false);
            });
        }

        const boot = bootData();
        if (boot.active_job_id) {
            startPolling(boot.active_job_id);
        } else {
            labFetch(scanLabApi("scan/active")).then(function (res) {
                if (!res) return;
                return res.json();
            }).then(function (data) {
                if (data && data.ok && data.job && data.job.job_id && !data.job.done) {
                    startPolling(data.job.job_id);
                }
            });
        }

        document.querySelectorAll(".scan-lab-edit-btn").forEach(function (btn) {
            btn.addEventListener("click", function () {
                const id = parseInt(btn.getAttribute("data-car-id"), 10);
                if (id > 0) openEditor(id);
            });
        });

        const saveBtn = document.getElementById("scan-lab-save");
        if (saveBtn) saveBtn.addEventListener("click", saveEditor);

        const reloadBtn = document.getElementById("scan-lab-reload-row");
        if (reloadBtn) reloadBtn.addEventListener("click", function () {
            if (editingCarId) openEditor(editingCarId);
        });
    });
})();
