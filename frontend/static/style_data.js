/* Dynamic styles without style="" attributes (P16.8). Templates and JS-built markup carry
   the value in a data-* attribute; this applies it through the CSSOM, which a CSP that
   forbids inline style attributes still allows.
     data-bg        image URL (already percent-encoded by css_url / safeCssBackgroundUrl)
     data-w         width in percent, 0-100
     data-bg-color  #rgb / #rrggbb / #rrggbbaa
   Elements added later (innerHTML, Leaflet divIcons) are picked up by the observer. */
(function () {
    "use strict";
    var SELECTOR = "[data-bg],[data-w],[data-bg-color]";
    var HEX = /^#[0-9a-f]{3,8}$/i;

    function applyOne(el) {
        var bg = el.getAttribute("data-bg");
        if (bg && /^(https?:\/\/|\/)/i.test(bg) && !/['"()\\\s]/.test(bg)) {
            el.style.backgroundImage = 'url("' + bg + '")';
        }
        var w = el.getAttribute("data-w");
        if (w !== null && w !== "" && isFinite(Number(w))) {
            el.style.width = Math.max(0, Math.min(100, Number(w))) + "%";
        }
        var color = el.getAttribute("data-bg-color");
        if (color && HEX.test(color)) el.style.backgroundColor = color;
    }

    function applyTree(root) {
        if (!root || root.nodeType !== 1) return;
        if (root.matches(SELECTOR)) applyOne(root);
        var found = root.querySelectorAll(SELECTOR);
        for (var i = 0; i < found.length; i++) applyOne(found[i]);
    }

    window.applyDataStyles = applyTree;

    function start() {
        applyTree(document.documentElement);
        if (typeof MutationObserver !== "function") return;
        new MutationObserver(function (records) {
            for (var i = 0; i < records.length; i++) {
                var r = records[i];
                if (r.type === "attributes") applyTree(r.target);
                for (var j = 0; j < r.addedNodes.length; j++) applyTree(r.addedNodes[j]);
            }
        }).observe(document.documentElement, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ["data-bg", "data-w", "data-bg-color"],
        });
    }

    if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", start);
    else start();
})();
