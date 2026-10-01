"use strict";
/**
 * Load browser helper scripts from frontend/static into a fresh node:vm context
 * with a tiny window/document stub. Only "pure" helper files work here: they
 * define functions on window.* namespaces and touch the DOM only when called.
 */
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const STATIC_DIR = path.resolve(__dirname, "..", "..", "..", "frontend", "static");

function makeDocument(opts) {
    const metas = Object.assign({}, opts.metas || {});
    return {
        querySelector(sel) {
            const m = /^meta\[name="([^"]+)"\]$/.exec(sel);
            if (m && Object.prototype.hasOwnProperty.call(metas, m[1])) {
                const content = metas[m[1]];
                return {
                    content: content == null ? "" : String(content),
                    getAttribute: (k) => (k === "content" ? content : null),
                };
            }
            return null;
        },
        getElementById() {
            return null;
        },
    };
}

/**
 * @param {string[]} files  basenames under frontend/static, loaded in order
 * @param {{metas?: Object<string,string>, origin?: string}} [opts]
 * @returns the context's global object (also reachable as `window` inside it)
 */
function loadStatic(files, opts) {
    const o = opts || {};
    const sandbox = {
        URL,
        setTimeout,
        clearTimeout,
        AbortController,
        console,
        location: { origin: o.origin || "https://example.test", href: (o.origin || "https://example.test") + "/" },
        document: makeDocument(o),
    };
    sandbox.window = sandbox;
    vm.createContext(sandbox);
    for (const f of files) {
        const code = fs.readFileSync(path.join(STATIC_DIR, f), "utf8");
        vm.runInContext(code, sandbox, { filename: f });
    }
    return sandbox;
}

/** Cross-realm values (arrays/objects built inside the vm) -> plain host values. */
function plain(v) {
    return JSON.parse(JSON.stringify(v));
}

module.exports = { loadStatic, plain, STATIC_DIR };
