/**
 * Account profile -- Hidden dealerships section.
 *
 * The list is server-rendered; this script refreshes it from
 * GET /api/profile/hidden-dealers and wires each Remove button to
 * DELETE /api/profile/hidden-dealers/<dealer_id>.
 */
(function () {
    "use strict";

    var list = document.getElementById("hidden-dealers-list");
    var empty = document.getElementById("hidden-dealers-empty");
    var errorBox = document.getElementById("hidden-dealers-error");
    if (!list || typeof window.fetch !== "function") return;

    function csrfToken() {
        var m = document.querySelector('meta[name="csrf-token"]');
        return m && m.content ? m.content : "";
    }

    function showError(msg) {
        if (!errorBox) return;
        errorBox.textContent = msg || "";
        errorBox.hidden = !msg;
    }

    function syncEmpty() {
        if (empty) empty.hidden = list.children.length > 0;
    }

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
        syncEmpty();
    }

    function refresh() {
        fetch("/api/profile/hidden-dealers", { credentials: "same-origin" })
            .then(function (r) { return r.ok ? r.json() : null; })
            .then(function (j) {
                if (j && j.ok && Array.isArray(j.dealers)) render(j.dealers);
            })
            .catch(function () { /* keep the server-rendered list */ });
    }

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
            .then(function (r) {
                if (r.status === 401) {
                    window.location.href = "/login";
                    return null;
                }
                return r.json();
            })
            .then(function (j) {
                if (j === null) return;
                if (!j || !j.ok) {
                    btn.disabled = false;
                    showError("Could not remove that dealership. Try again.");
                    return;
                }
                var row = btn.closest(".account-profile__hidden-row");
                if (row) row.remove();
                syncEmpty();
            })
            .catch(function () {
                btn.disabled = false;
                showError("Could not remove that dealership. Try again.");
            });
    });

    syncEmpty();
    refresh();
})();
