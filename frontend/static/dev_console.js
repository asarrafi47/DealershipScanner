(function () {
  const bodyEl = document.getElementById("dev-dealers-body");
  const statusEl = document.getElementById("dev-status");
  const saveBtn = document.getElementById("dev-save");
  const addBtn = document.getElementById("dev-add-row");
  const inferBtn = document.getElementById("dev-infer-btn");
  const inferUrlEl = document.getElementById("dev-infer-url");
  const inferHtmlEl = document.getElementById("dev-infer-html");
  const providersEl = document.getElementById("dev-providers-json");
  let providers = ["dealer_dot_com", "dealer_on"];
  try {
    const parsed = JSON.parse(providersEl.textContent || "[]");
    if (Array.isArray(parsed) && parsed.length) providers = parsed;
  } catch (_) {
    /* keep default */
  }

  function setStatus(msg, kind) {
    statusEl.textContent = msg || "";
    statusEl.classList.remove("dev-status--ok", "dev-status--err");
    if (kind === "ok") statusEl.classList.add("dev-status--ok");
    if (kind === "err") statusEl.classList.add("dev-status--err");
  }

  function providerOptions(selected) {
    return providers
      .map(
        (p) =>
          `<option value="${escapeAttr(p)}"${p === selected ? " selected" : ""}>${escapeAttr(
            p
          )}</option>`
      )
      .join("");
  }

  function escapeAttr(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/"/g, "&quot;")
      .replace(/</g, "&lt;");
  }

  function rowTemplate(d) {
    const name = d.name || "";
    const url = d.url || "";
    const provider = d.provider || "dealer_dot_com";
    const dealerId = d.dealer_id || "";
    const brand = d.brand != null ? String(d.brand) : "";
    return `
<tr class="dev-dealer-row">
  <td><input type="text" class="dev-in-name" value="${escapeAttr(name)}" placeholder="Dealer display name" autocomplete="organization"></td>
  <td><input type="url" class="dev-in-url" value="${escapeAttr(url)}" placeholder="https://…" autocomplete="url"></td>
  <td><select class="dev-in-provider">${providerOptions(provider)}</select></td>
  <td><input type="text" class="dev-in-id" value="${escapeAttr(dealerId)}" placeholder="slug-id" pattern="[a-z0-9]+(-[a-z0-9]+)*" autocomplete="off"></td>
  <td><input type="text" class="dev-in-brand" value="${escapeAttr(brand)}" placeholder="Optional" autocomplete="off"></td>
  <td class="dev-col-actions"><div class="dev-row-actions"><button type="button" class="dev-row-scan secondary-button">Scan</button><button type="button" class="dev-row-remove">Remove</button></div></td>
</tr>`;
  }

  function bindRemove(root) {
    root.querySelectorAll(".dev-row-remove").forEach((btn) => {
      btn.addEventListener("click", () => {
        const tr = btn.closest("tr");
        if (tr) tr.remove();
        setStatus("");
      });
    });
  }

  function formatScanError(data, status) {
    const code = data && data.code ? String(data.code) : "";
    const msg = (data && (data.message || data.error)) || "";
    const hasDid = data && Object.prototype.hasOwnProperty.call(data, "dealer_id");
    const parts = [];
    if (code) parts.push(`[${code}]`);
    if (msg) parts.push(msg);
    else parts.push(`Scan request failed (HTTP ${status}).`);
    if (hasDid) {
      parts.push(`(dealer_id: ${JSON.stringify(data.dealer_id)})`);
    }
    return parts.join(" ");
  }

  function bindScan(root) {
    root.querySelectorAll(".dev-row-scan").forEach((btn) => {
      btn.addEventListener("click", async () => {
        const tr = btn.closest("tr");
        const idInput = tr && tr.querySelector(".dev-in-id");
        const dealer_id = idInput ? String(idInput.value || "").trim() : "";
        if (!dealer_id) {
          setStatus("Set a Dealer ID on this row before scanning.", "err");
          return;
        }
        const payload = { dealer_id: dealer_id };
        const body = JSON.stringify(payload);
        console.debug("[dev] POST /api/dev/scan-dealer", { payload, body });

        setStatus(`Requesting scan for ${dealer_id}…`, null);
        btn.disabled = true;
        try {
          const res = await fetch("/api/dev/scan-dealer", {
            method: "POST",
            credentials: "same-origin",
            headers: { "Content-Type": "application/json", Accept: "application/json" },
            body: body,
          });
          const data = await res.json().catch(() => ({}));
          if (!res.ok) {
            setStatus(`Rejected — ${formatScanError(data, res.status)}`, "err");
            return;
          }
          if (res.status !== 202) {
            setStatus(
              `Accepted (HTTP ${res.status}) — ${data.message || "Scan request completed."}`,
              "ok"
            );
            return;
          }
          setStatus(
            `Accepted — ${data.message ||
              `Scanner started for ${dealer_id}. Watch the server terminal for progress.`}`,
            "ok"
          );
        } catch (e) {
          setStatus(e.message || "Network error", "err");
        } finally {
          btn.disabled = false;
        }
      });
    });
  }

  function addRow(data) {
    const wrap = document.createElement("tbody");
    wrap.innerHTML = rowTemplate(data || {}).trim();
    const tr = wrap.querySelector("tr");
    bodyEl.appendChild(tr);
    bindRemove(tr);
    bindScan(tr);
  }

  function collect() {
    const rows = bodyEl.querySelectorAll("tr.dev-dealer-row");
    const dealers = [];
    rows.forEach((tr) => {
      const name = (tr.querySelector(".dev-in-name") || {}).value || "";
      const url = (tr.querySelector(".dev-in-url") || {}).value || "";
      const provider = (tr.querySelector(".dev-in-provider") || {}).value || "dealer_dot_com";
      const dealer_id = (tr.querySelector(".dev-in-id") || {}).value || "";
      const brandRaw = (tr.querySelector(".dev-in-brand") || {}).value || "";
      const o = { name: name.trim(), url: url.trim(), provider, dealer_id: dealer_id.trim() };
      const brand = brandRaw.trim();
      if (brand) o.brand = brand;
      dealers.push(o);
    });
    return dealers;
  }

  async function load() {
    setStatus("Loading…", null);
    try {
      const res = await fetch("/api/dev/dealers", {
        credentials: "same-origin",
        headers: { Accept: "application/json" },
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setStatus(data.error || `Failed to load (${res.status})`, "err");
        return;
      }
      bodyEl.innerHTML = "";
      const list = data.dealers || [];
      if (!list.length) {
        addRow({});
        setStatus("No dealerships yet — add one below.", null);
        return;
      }
      list.forEach((d) => addRow(d));
      setStatus(`Loaded ${list.length} dealership${list.length === 1 ? "" : "s"}.`, "ok");
    } catch (e) {
      setStatus(e.message || "Network error", "err");
    }
  }

  saveBtn.addEventListener("click", async () => {
    const dealers = collect();
    setStatus("Saving…", null);
    try {
      const res = await fetch("/api/dev/dealers", {
        method: "PUT",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ dealers }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        setStatus(data.error || `Save failed (${res.status})`, "err");
        return;
      }
      setStatus("Saved dealers.json.", "ok");
    } catch (e) {
      setStatus(e.message || "Network error", "err");
    }
  });

  addBtn.addEventListener("click", () => {
    addRow({});
    setStatus("");
  });

  if (inferBtn && inferUrlEl) {
    inferBtn.addEventListener("click", async () => {
      const url = (inferUrlEl.value || "").trim();
      if (!url) {
        setStatus("Enter a dealership site URL.", "err");
        return;
      }
      setStatus("Fetching site and inferring fields…", null);
      inferBtn.disabled = true;
      try {
        const pasted = inferHtmlEl ? (inferHtmlEl.value || "").trim() : "";
        const payload = { url };
        if (pasted) payload.html = pasted;
        const res = await fetch("/api/dev/infer-dealer", {
          method: "POST",
          credentials: "same-origin",
          headers: { "Content-Type": "application/json", Accept: "application/json" },
          body: JSON.stringify(payload),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok) {
          setStatus(data.error || `Look up failed (${res.status})`, "err");
          return;
        }
        addRow(data.dealer || {});
        const hints = (data.hints || []).join(" ");
        setStatus(hints || "Added a row from URL — review and save.", "ok");
        if (inferHtmlEl) inferHtmlEl.value = "";
      } catch (e) {
        setStatus(e.message || "Network error", "err");
      } finally {
        inferBtn.disabled = false;
      }
    });
  }

  bindRemove(bodyEl);
  bindScan(bodyEl);
  load();
})();
