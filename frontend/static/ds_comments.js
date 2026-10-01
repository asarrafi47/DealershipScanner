/* Comment threads for cars and dealerships.
 *
 * One widget drives both scopes. A mount point declares what it is talking to:
 *
 *   <div class="ds-comments"
 *        data-comments-scope="car|dealer"
 *        data-comments-target="<id>"
 *        data-comments-logged-in="0|1"></div>
 *
 * Endpoints (backend/routes/community_api.py):
 *   GET    /api/cars/<id>/comments                  -> {ok, comments, count, can_post}
 *   POST   /api/cars/<id>/comments                  {body} or multipart {body, images[]}
 *   DELETE /api/cars/<id>/comments/<comment_id>
 *   POST   /api/cars/<id>/comments/<comment_id>/flag
 *   ...and the /api/dealerships/<id>/... mirror of each.
 *
 * The API returns body_html already escaped (newlines as <br>), so that is the only
 * field written with innerHTML; everything else goes through textContent. Threads load
 * lazily on first reveal because both mounts live inside tab panels that start hidden.
 *
 * Photos follow the same discipline. A comment may carry an `attachments` array of
 * {id, url, width, height}; each one becomes a real <img> whose src is assigned as an
 * element property, never spliced into an HTML string, so a hostile URL cannot become
 * markup. A post with files selected goes out as FormData -- Content-Type is left to
 * the browser (setting it by hand loses the multipart boundary) while X-CSRF-Token is
 * still sent explicitly, so the server's CSRF check is unchanged from the JSON path.
 */
(function () {
    "use strict";

    var PATHS = {
        car: function (id) { return "/api/cars/" + encodeURIComponent(id) + "/comments"; },
        dealer: function (id) { return "/api/dealerships/" + encodeURIComponent(id) + "/comments"; },
    };

    // Mirrors backend/utils/comment_images.py. The server is the authority on all
    // three -- these only exist to fail fast, before an 8 MB upload leaves the phone.
    var MAX_IMAGES = 4;
    var MAX_IMAGE_BYTES = 8 * 1024 * 1024;
    var ACCEPT_TYPES = ["image/jpeg", "image/png", "image/webp"];

    var PLACEHOLDER = {
        car: "Seen this car in person? Anything a photo would not show?",
        dealer: "How were they to deal with? Add-ons, pressure, condition of the lot...",
    };

    var EMPTY = {
        car: "No comments on this car yet. Be the first.",
        dealer: "No comments on this dealership yet. Be the first.",
    };

    function csrfToken() {
        return window.DS.csrfToken();
    }

    function el(tag, cls, text) {
        var n = document.createElement(tag);
        if (cls) n.className = cls;
        if (text != null) n.textContent = text;
        return n;
    }

    // Coarse on purpose: an exact timestamp invites arguing about staleness, and the
    // useful signal is "recent" vs "a while ago".
    function relTime(iso) {
        var t = Date.parse(iso);
        if (isNaN(t)) return "";
        var secs = Math.max(0, (Date.now() - t) / 1000);
        if (secs < 90) return "just now";
        var mins = secs / 60;
        if (mins < 60) return Math.round(mins) + " min ago";
        var hrs = mins / 60;
        if (hrs < 24) return Math.round(hrs) + " hr ago";
        var days = hrs / 24;
        if (days < 30) return Math.round(days) + " days ago";
        return new Date(t).toLocaleDateString();
    }

    // --- full-size viewer --------------------------------------------------
    // One overlay for the whole page, built on first use. Both mounts on the car
    // page would otherwise each carry their own copy of it.
    var lightbox = null;

    function closeLightbox() {
        if (lightbox) {
            lightbox.hidden = true;
            lightbox.img.removeAttribute("src");
        }
    }

    function openLightbox(url) {
        if (!lightbox) {
            var box = el("div", "ds-comments__lightbox");
            box.hidden = true;
            var img = document.createElement("img");
            img.className = "ds-comments__lightbox-img";
            img.alt = "Attached photo, full size";
            var close = el("button", "ds-comments__lightbox-close", "Close");
            close.type = "button";
            box.appendChild(close);
            box.appendChild(img);
            box.addEventListener("click", closeLightbox);
            document.addEventListener("keydown", function (ev) {
                if (ev.key === "Escape" || ev.key === "Esc") closeLightbox();
            });
            document.body.appendChild(box);
            box.img = img;
            lightbox = box;
        }
        lightbox.img.src = url;  // property assignment, never string-built markup
        lightbox.hidden = false;
    }

    var pickerSeq = 0;

    function Widget(root) {
        this.root = root;
        this.scope = root.getAttribute("data-comments-scope") === "dealer" ? "dealer" : "car";
        this.target = root.getAttribute("data-comments-target") || "";
        this.loggedIn = root.getAttribute("data-comments-logged-in") === "1";
        this.loaded = false;
        this.previewUrls = [];
        this.build();
    }

    Widget.prototype.url = function (suffix) {
        return PATHS[this.scope](this.target) + (suffix || "");
    };

    Widget.prototype.build = function () {
        this.root.innerHTML = "";

        this.composer = el("form", "ds-comments__composer");
        this.textarea = el("textarea", "ds-comments__input");
        this.textarea.rows = 3;
        this.textarea.maxLength = 4000;
        this.textarea.placeholder = PLACEHOLDER[this.scope];
        this.textarea.setAttribute("aria-label", "Write a comment");

        // Photo picker. The visible control is the <label>; the input itself is
        // hidden by CSS so it can be styled like the rest of the composer without
        // losing keyboard focus or the label's click target.
        var pickerId = "ds-comments-file-" + (++pickerSeq);
        this.fileInput = document.createElement("input");
        this.fileInput.type = "file";
        this.fileInput.className = "ds-comments__file";
        this.fileInput.id = pickerId;
        this.fileInput.multiple = true;
        this.fileInput.accept = ACCEPT_TYPES.join(",");
        this.fileInput.addEventListener("change", this.onPick.bind(this));
        var pickerLabel = el("label", "ds-comments__attach", "Add photos");
        pickerLabel.setAttribute("for", pickerId);

        this.previews = el("ul", "ds-comments__previews");
        this.previews.hidden = true;

        var actions = el("div", "ds-comments__composer-actions");
        this.submit = el("button", "ds-comments__submit", "Post comment");
        this.submit.type = "submit";
        this.status = el("p", "ds-comments__status");
        this.status.setAttribute("role", "status");
        // Input before label so CSS can style the label from the input's focus.
        actions.appendChild(this.fileInput);
        actions.appendChild(pickerLabel);
        actions.appendChild(this.status);
        actions.appendChild(this.submit);

        if (this.loggedIn) {
            this.composer.appendChild(this.textarea);
            this.composer.appendChild(this.previews);
            this.composer.appendChild(actions);
            this.composer.addEventListener("submit", this.onSubmit.bind(this));
        } else {
            var signIn = el("p", "ds-comments__signin");
            signIn.appendChild(document.createTextNode("​"));
            var a = el("a", null, "Sign in");
            a.href = "/login";
            signIn.textContent = "";
            signIn.appendChild(a);
            signIn.appendChild(document.createTextNode(" to leave a comment."));
            this.composer.appendChild(signIn);
        }

        this.list = el("ul", "ds-comments__list");
        this.empty = el("p", "ds-comments__empty", "Loading comments…");

        this.root.appendChild(this.composer);
        this.root.appendChild(this.empty);
        this.root.appendChild(this.list);
    };

    Widget.prototype.load = function () {
        if (this.loaded || !this.target) return;
        this.loaded = true;
        var self = this;
        fetch(this.url(), { credentials: "same-origin" })
            .then(function (r) { return r.ok ? r.json() : null; })
            .then(function (d) {
                if (!d || !d.ok) throw new Error("load failed");
                self.render(d.comments || []);
            })
            .catch(function () {
                // Allow a retry on the next reveal rather than latching the failure.
                self.loaded = false;
                self.empty.textContent = "Could not load comments. Reopen this tab to retry.";
                self.empty.hidden = false;
            });
    };

    Widget.prototype.render = function (comments) {
        this.list.innerHTML = "";
        if (!comments.length) {
            this.empty.textContent = EMPTY[this.scope];
            this.empty.hidden = false;
            return;
        }
        this.empty.hidden = true;
        for (var i = 0; i < comments.length; i++) {
            this.list.appendChild(this.row(comments[i]));
        }
    };

    // Server-side flag. The API deliberately does not ship the author's user_id
    // (comments render as an anonymous "Shopper"), so ownership cannot be, and is
    // no longer, recomputed here from a leaked id.
    Widget.prototype.isMine = function (c) {
        return !!c.is_mine;
    };

    Widget.prototype.row = function (c) {
        var li = el("li", "ds-comments__item");
        li.setAttribute("data-comment-id", c.id);
        var mine = this.isMine(c);

        var head = el("div", "ds-comments__item-head");
        head.appendChild(el("span", "ds-comments__author", mine ? "You" : "Shopper"));
        head.appendChild(el("span", "ds-comments__time", relTime(c.created_at)));

        var body = el("div", "ds-comments__body");
        // body_html is escaped server-side; see module header.
        body.innerHTML = c.body_html || "";

        li.appendChild(head);
        li.appendChild(body);

        var media = this.media(c.attachments);
        if (media) li.appendChild(media);

        var tools = el("div", "ds-comments__item-tools");
        if (mine) {
            var del = el("button", "ds-comments__link", "Delete");
            del.type = "button";
            del.addEventListener("click", this.onDelete.bind(this, c.id, li));
            tools.appendChild(del);
        } else if (this.loggedIn) {
            var flag = el("button", "ds-comments__link", "Report");
            flag.type = "button";
            flag.addEventListener("click", this.onFlag.bind(this, c.id, tools));
            tools.appendChild(flag);
        }
        if (tools.childNodes.length) li.appendChild(tools);
        return li;
    };

    // Thumbnails for one comment, or null when it has none. Every URL is assigned
    // as an element property (img.src / the button's handler) rather than
    // interpolated into markup, which is the same rule the body follows: only
    // body_html, escaped by the server, is ever written with innerHTML.
    Widget.prototype.media = function (attachments) {
        if (!attachments || !attachments.length) return null;
        var ul = el("ul", "ds-comments__media");
        for (var i = 0; i < attachments.length; i++) {
            var a = attachments[i];
            if (!a || !a.url) continue;
            var li = el("li", "ds-comments__media-item");
            var btn = el("button", "ds-comments__thumb");
            btn.type = "button";
            btn.setAttribute("aria-label", "View photo " + (i + 1) + " full size");
            var img = document.createElement("img");
            img.src = a.url;
            img.alt = "Photo attached to this comment";
            img.loading = "lazy";
            img.decoding = "async";
            // Intrinsic size from the server so the row does not reflow on load.
            if (a.width) img.width = a.width;
            if (a.height) img.height = a.height;
            btn.appendChild(img);
            btn.addEventListener("click", openLightbox.bind(null, a.url));
            li.appendChild(btn);
            ul.appendChild(li);
        }
        return ul.childNodes.length ? ul : null;
    };

    Widget.prototype.pickedFiles = function () {
        if (!this.fileInput || !this.fileInput.files) return [];
        return Array.prototype.slice.call(this.fileInput.files);
    };

    // Client-side checks are a courtesy, not a control: every one of them is
    // repeated server-side in backend/utils/comment_images.py, which decodes the
    // file rather than believing its type. This only saves an 8 MB round trip.
    Widget.prototype.onPick = function () {
        var files = this.pickedFiles();
        var problem = "";
        if (files.length > MAX_IMAGES) {
            problem = "Attach at most " + MAX_IMAGES + " photos.";
        }
        for (var i = 0; i < files.length && !problem; i++) {
            if (ACCEPT_TYPES.indexOf(files[i].type) === -1) {
                problem = "Photos must be JPEG, PNG or WebP.";
            } else if (files[i].size > MAX_IMAGE_BYTES) {
                problem = "Each photo must be under 8 MB.";
            }
        }
        if (problem) {
            this.clearPicked();
            this.say(problem, true);
            return;
        }
        this.say("");
        this.showPreviews(files);
    };

    Widget.prototype.clearPicked = function () {
        if (this.fileInput) this.fileInput.value = "";
        this.showPreviews([]);
    };

    Widget.prototype.showPreviews = function (files) {
        // Object URLs pin the file in memory until revoked; re-picking would leak
        // the previous selection otherwise.
        for (var i = 0; i < this.previewUrls.length; i++) {
            URL.revokeObjectURL(this.previewUrls[i]);
        }
        this.previewUrls = [];
        this.previews.innerHTML = "";
        this.previews.hidden = !files.length;
        for (var j = 0; j < files.length; j++) {
            var url = URL.createObjectURL(files[j]);
            this.previewUrls.push(url);
            var li = el("li", "ds-comments__preview");
            var img = document.createElement("img");
            img.src = url;
            img.alt = "Selected photo " + (j + 1);
            li.appendChild(img);
            this.previews.appendChild(li);
        }
        if (files.length) {
            var clear = el("li", "ds-comments__preview-clear");
            var btn = el("button", "ds-comments__link", "Remove photos");
            btn.type = "button";
            btn.addEventListener("click", this.clearPicked.bind(this));
            clear.appendChild(btn);
            this.previews.appendChild(clear);
        }
    };

    Widget.prototype.say = function (msg, isError) {
        this.status.textContent = msg || "";
        this.status.classList.toggle("ds-comments__status--error", !!isError);
    };

    Widget.prototype.onSubmit = function (ev) {
        ev.preventDefault();
        var text = (this.textarea.value || "").trim();
        if (!text) { this.say("Write something first.", true); return; }
        var self = this;
        var files = this.pickedFiles();
        this.submit.disabled = true;
        this.say(files.length ? "Uploading…" : "Posting…");

        var init = {
            method: "POST",
            credentials: "same-origin",
            headers: { "X-CSRF-Token": csrfToken() },
        };
        if (files.length) {
            // Content-Type is deliberately left unset: the browser has to add the
            // multipart boundary. The CSRF header still goes out, so the server's
            // check is exactly the one the JSON path uses.
            var fd = new FormData();
            fd.append("body", text);
            for (var i = 0; i < files.length; i++) fd.append("images", files[i]);
            init.body = fd;
        } else {
            init.headers["Content-Type"] = "application/json";
            init.body = JSON.stringify({ body: text });
        }

        fetch(this.url(), init)
            .then(function (r) {
                // A 413 comes back from the server as HTML, not JSON.
                return r.json()
                    .catch(function () { return null; })
                    .then(function (d) { return { status: r.status, d: d }; });
            })
            .then(function (res) {
                if (res.status === 429) throw new Error("You are posting too quickly. Try again in a minute.");
                if (res.status === 401) throw new Error("Sign in to leave a comment.");
                if (res.status === 413) throw new Error("Those photos are too large to upload.");
                if (!res.d || !res.d.ok) {
                    throw new Error((res.d && (res.d.message || res.d.error)) || "Could not post that.");
                }
                self.textarea.value = "";
                self.clearPicked();
                self.say("");
                self.empty.hidden = true;
                var c = res.d.comment || res.d;
                c.is_mine = true;
                self.list.insertBefore(self.row(c), self.list.firstChild);
            })
            .catch(function (e) { self.say(e.message || "Could not post that.", true); })
            .then(function () { self.submit.disabled = false; });
    };

    Widget.prototype.onDelete = function (id, li) {
        var self = this;
        fetch(this.url("/" + id), {
            method: "DELETE",
            credentials: "same-origin",
            headers: { "X-CSRF-Token": csrfToken() },
        })
            .then(function (r) { return r.ok; })
            .then(function (ok) {
                if (!ok) { self.say("Could not delete that comment.", true); return; }
                li.remove();
                if (!self.list.childNodes.length) {
                    self.empty.textContent = EMPTY[self.scope];
                    self.empty.hidden = false;
                }
            })
            .catch(function () { self.say("Could not delete that comment.", true); });
    };

    Widget.prototype.onFlag = function (id, tools) {
        fetch(this.url("/" + id + "/flag"), {
            method: "POST",
            credentials: "same-origin",
            headers: { "X-CSRF-Token": csrfToken() },
        })
            .then(function () { tools.textContent = "Reported — thanks."; })
            .catch(function () {});
    };

    var widgets = [];

    function init() {
        var mounts = document.querySelectorAll(".ds-comments");
        for (var i = 0; i < mounts.length; i++) {
            var w = new Widget(mounts[i]);
            widgets.push(w);
            // Mounts inside an already-visible panel still need their first load.
            if (mounts[i].offsetParent !== null) w.load();
        }
    }

    // car_page.js calls this when a tab is revealed; the panels start hidden, so this
    // is where most threads actually load.
    window.__DS_loadCommentsIn = function (container) {
        for (var i = 0; i < widgets.length; i++) {
            if (!container || container.contains(widgets[i].root)) widgets[i].load();
        }
    };

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init, { once: true });
    } else {
        init();
    }
})();
