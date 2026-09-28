/**
 * Account profile -- Hidden dealerships, Recent searches, Saved searches.
 *
 * Every list is server-rendered; this script only wires the buttons:
 *   Hidden dealerships  Remove  -> DELETE /api/profile/hidden-dealers/<dealer_id>
 *   Recent searches     Remove  -> DELETE /api/profile/search-history/<id>
 *                       Clear   -> DELETE /api/profile/search-history
 *                       Save    -> POST   /api/saved-searches {filters}
 *   Saved searches      Remove  -> DELETE /api/saved-searches/<id>
 * and refreshes the hidden-dealer list from GET /api/profile/hidden-dealers.
 */
(function () {
    "use strict";

    if (typeof window.fetch !== "function") return;

    function csrfToken() {
        var m = document.querySelector('meta[name="csrf-token"]');
        return m && m.content ? m.content : "";
    }

    function jsonHeaders() {
        return { "Content-Type": "application/json", "X-CSRF-Token": csrfToken() };
    }

    function errorSetter(box) {
        return function (msg) {
            if (!box) return;
            box.textContent = msg || "";
            box.hidden = !msg;
        };
    }

    function syncEmpty(list, empty, extra) {
        var has = list.children.length > 0;
        if (empty) empty.hidden = has;
        if (extra) extra.hidden = !has;
    }

    // Mirrors search_history_format.listings_url_for_filters for rows built client-side.
    function hrefForFilters(filters) {
        var params = new URLSearchParams();
        Object.keys(filters || {}).sort().forEach(function (k) {
            if (k === "page") return;
            var key = k === "dealer_registry_ids" ? "dealer_registry_id" : k;
            var v = filters[k];
            var vals = Array.isArray(v) ? v : [v];
            vals.forEach(function (x) {
                if (x === undefined || x === null || x === "" || x === false) return;
                params.append(key, x === true ? "1" : String(x));
            });
        });
        var qs = params.toString();
        return "/listings" + (qs ? "?" + qs : "");
    }

    function handleAuth(r) {
        if (r.status === 401) {
            window.location.href = "/login";
            return null;
        }
        return r.json().catch(function () { return {}; }).then(function (j) {
            return { status: r.status, body: j || {} };
        });
    }

    // ------------------------------------------------------------------
    // Hidden dealerships
    // ------------------------------------------------------------------
    (function hiddenDealers() {
        var list = document.getElementById("hidden-dealers-list");
        var empty = document.getElementById("hidden-dealers-empty");
        var showError = errorSetter(document.getElementById("hidden-dealers-error"));
        if (!list) return;

        function rowFor(d) {
            var li = document.createElement("li");
            li.className = "account-profile__hidden-row";
            li.setAttribute("data-dealer-id", d.dealer_id);
            var a = document.createElement("a");
            a.className = "account-profile__hidden-name";
            a.href = "/dealership/" + encodeURIComponent(d.dealer_id);
            a.textContent = d.dealer_name || d.dealer_id;
            var btn = document.createElement("button");
            btn.type = "button";
            btn.className = "secondary-button account-profile__hidden-remove";
            btn.setAttribute("data-dealer-id", d.dealer_id);
            btn.textContent = "Remove";
            li.appendChild(a);
            li.appendChild(btn);
            return li;
        }

        function render(dealers) {
            list.textContent = "";
            (dealers || []).forEach(function (d) {
                if (d && d.dealer_id) list.appendChild(rowFor(d));
            });
            syncEmpty(list, empty);
        }

        fetch("/api/profile/hidden-dealers", { credentials: "same-origin" })
            .then(function (r) { return r.ok ? r.json() : null; })
            .then(function (j) {
                if (j && j.ok && Array.isArray(j.dealers)) render(j.dealers);
            })
            .catch(function () { /* keep the server-rendered list */ });

        list.addEventListener("click", function (ev) {
            var btn = ev.target && ev.target.closest
                ? ev.target.closest(".account-profile__hidden-remove")
                : null;
            if (!btn) return;
            var dealerId = btn.getAttribute("data-dealer-id") || "";
            if (!dealerId) return;
            btn.disabled = true;
            showError("");
            fetch("/api/profile/hidden-dealers/" + encodeURIComponent(dealerId), {
                method: "DELETE",
                credentials: "same-origin",
                headers: { "X-CSRF-Token": csrfToken() },
            })
                .then(handleAuth)
                .then(function (res) {
                    if (res === null) return;
                    if (!res.body.ok) {
                        btn.disabled = false;
                        showError("Could not remove that dealership. Try again.");
                        return;
                    }
                    var row = btn.closest(".account-profile__hidden-row");
                    if (row) row.remove();
                    syncEmpty(list, empty);
                })
                .catch(function () {
                    btn.disabled = false;
                    showError("Could not remove that dealership. Try again.");
                });
        });

        syncEmpty(list, empty);
    })();

    // ------------------------------------------------------------------
    // Saved searches (rows are appended here when a recent search is saved)
    // ------------------------------------------------------------------
    var savedList = document.getElementById("saved-searches-list");
    var savedEmpty = document.getElementById("saved-searches-empty");
    var showSavedError = errorSetter(document.getElementById("saved-searches-error"));

    function savedRowFor(id, label, url) {
        var li = document.createElement("li");
        li.className = "account-profile__history-row";
        li.setAttribute("data-saved-id", String(id));

        var main = document.createElement("div");
        main.className = "account-profile__history-main";
        var a = document.createElement("a");
        a.className = "account-profile__history-label";
        a.href = url;
        a.textContent = label;
        var meta = document.createElement("p");
        meta.className = "account-profile__history-meta";
        meta.textContent = "Saved just now";
        main.appendChild(a);
        main.appendChild(meta);

        var actions = document.createElement("div");
        actions.className = "account-profile__history-actions";
        var run = document.createElement("a");
        run.className = "secondary-button account-profile__history-btn";
        run.href = url;
        run.textContent = "Run";
        var rm = document.createElement("button");
        rm.type = "button";
        rm.className = "secondary-button account-profile__history-btn account-profile__saved-remove";
        rm.setAttribute("data-saved-id", String(id));
        rm.textContent = "Remove";
        actions.appendChild(run);
        actions.appendChild(rm);

        li.appendChild(main);
        li.appendChild(actions);
        return li;
    }

    if (savedList) {
        savedList.addEventListener("click", function (ev) {
            var btn = ev.target && ev.target.closest
                ? ev.target.closest(".account-profile__saved-remove")
                : null;
            if (!btn) return;
            var id = btn.getAttribute("data-saved-id") || "";
            if (!id) return;
            btn.disabled = true;
            showSavedError("");
            fetch("/api/saved-searches/" + encodeURIComponent(id), {
                method: "DELETE",
                credentials: "same-origin",
                headers: { "X-CSRF-Token": csrfToken() },
            })
                .then(handleAuth)
                .then(function (res) {
                    if (res === null) return;
                    if (!res.body.ok && res.status !== 404) {
                        btn.disabled = false;
                        showSavedError("Could not remove that saved search. Try again.");
                        return;
                    }
                    var row = btn.closest(".account-profile__history-row");
                    if (row) row.remove();
                    syncEmpty(savedList, savedEmpty);
                })
                .catch(function () {
                    btn.disabled = false;
                    showSavedError("Could not remove that saved search. Try again.");
                });
        });
        syncEmpty(savedList, savedEmpty);
    }

    // ------------------------------------------------------------------
    // Recent searches
    // ------------------------------------------------------------------
    (function recentSearches() {
        var list = document.getElementById("recent-searches-list");
        var empty = document.getElementById("recent-searches-empty");
        var clearBtn = document.getElementById("recent-searches-clear");
        var showError = errorSetter(document.getElementById("recent-searches-error"));
        if (!list) return;

        function filtersForRow(row) {
            var raw = row ? row.getAttribute("data-filters") : "";
            if (!raw) return null;
            try {
                var parsed = JSON.parse(raw);
                return parsed && typeof parsed === "object" ? parsed : null;
            } catch (e) {
                return null;
            }
        }

        function removeEntry(btn, id) {
            btn.disabled = true;
            showError("");
            fetch("/api/profile/search-history/" + encodeURIComponent(id), {
                method: "DELETE",
                credentials: "same-origin",
                headers: { "X-CSRF-Token": csrfToken() },
            })
                .then(handleAuth)
                .then(function (res) {
                    if (res === null) return;
                    if (!res.body.ok && res.status !== 404) {
                        btn.disabled = false;
                        showError("Could not remove that search. Try again.");
                        return;
                    }
                    var row = btn.closest(".account-profile__history-row");
                    if (row) row.remove();
                    syncEmpty(list, empty, clearBtn);
                })
                .catch(function () {
                    btn.disabled = false;
                    showError("Could not remove that search. Try again.");
                });
        }

        function saveEntry(btn, row) {
            var filters = filtersForRow(row);
            if (!filters || !Object.keys(filters).length) {
                showError("This search has no filters to save.");
                return;
            }
            btn.disabled = true;
            showError("");
            fetch("/api/saved-searches", {
                method: "POST",
                credentials: "same-origin",
                headers: jsonHeaders(),
                body: JSON.stringify({ filters: filters }),
            })
                .then(handleAuth)
                .then(function (res) {
                    if (res === null) return;
                    var body = res.body;
                    if (!body.ok) {
                        btn.disabled = false;
                        if (body.error === "premium_required" && body.upgrade_url) {
                            showError("Saving searches is part of " + (body.upgrade_plan_name || "Premium") + ". See plans at " + body.upgrade_url);
                        } else if (body.error === "login_required") {
                            window.location.href = "/login";
                        } else {
                            showError("Could not save that search. Try again.");
                        }
                        return;
                    }
                    btn.textContent = "Saved";
                    var labelEl = row ? row.querySelector(".account-profile__history-label") : null;
                    var label = labelEl ? labelEl.textContent : "Saved search";
                    if (savedList) {
                        savedList.insertBefore(
                            savedRowFor(body.id, label, hrefForFilters(body.filters || filters)),
                            savedList.firstChild
                        );
                        syncEmpty(savedList, savedEmpty);
                    }
                })
                .catch(function () {
                    btn.disabled = false;
                    showError("Could not save that search. Try again.");
                });
        }

        list.addEventListener("click", function (ev) {
            var target = ev.target;
            if (!target || !target.closest) return;
            var rm = target.closest(".account-profile__history-remove");
            if (rm) {
                var id = rm.getAttribute("data-search-id") || "";
                if (id) removeEntry(rm, id);
                return;
            }
            var save = target.closest(".account-profile__history-save");
            if (save) saveEntry(save, save.closest(".account-profile__history-row"));
        });

        if (clearBtn) {
            clearBtn.addEventListener("click", function () {
                if (!list.children.length) return;
                if (!window.confirm("Clear all recent searches?")) return;
                clearBtn.disabled = true;
                showError("");
                fetch("/api/profile/search-history", {
                    method: "DELETE",
                    credentials: "same-origin",
                    headers: { "X-CSRF-Token": csrfToken() },
                })
                    .then(handleAuth)
                    .then(function (res) {
                        clearBtn.disabled = false;
                        if (res === null) return;
                        if (!res.body.ok) {
                            showError("Could not clear your searches. Try again.");
                            return;
                        }
                        list.textContent = "";
                        syncEmpty(list, empty, clearBtn);
                    })
                    .catch(function () {
                        clearBtn.disabled = false;
                        showError("Could not clear your searches. Try again.");
                    });
            });
        }

        syncEmpty(list, empty, clearBtn);
    })();
})();
