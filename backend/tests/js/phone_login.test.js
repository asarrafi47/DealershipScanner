"use strict";
/**
 * frontend/static/phone_login.js (card S8): the progressive enhancement of the two phone sign-in
 * pages. The script is an IIFE over the live DOM, so it is loaded into a node:vm context with a
 * small DOM stub and a manual fake clock: timers fire only when a test advances the clock, and
 * Date.now() reads the same clock, so no test ever waits.
 *
 * Covered: the consent button state (aria-disabled, hint, aria-describedby), the "person check did
 * not load" notice (8 s, goes away when the widget arrives), the resend countdown (a deadline, not a
 * tick count: it survives a suspended phone) and the one-shot Sign in button.
 */
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { STATIC_DIR } = require("./_load.js");

const SCRIPT = fs.readFileSync(path.join(STATIC_DIR, "phone_login.js"), "utf8");
const START_MS = 1700000000000;

/** A fake clock. advance() fires due timers in order; jump() moves time without firing any. */
function makeClock() {
    const clock = { now: START_MS, timers: new Map(), seq: 0 };
    const add = (fn, ms, every) => {
        clock.seq += 1;
        clock.timers.set(clock.seq, { fn, every, next: clock.now + ms });
        return clock.seq;
    };
    clock.setInterval = (fn, ms) => add(fn, ms, ms);
    clock.setTimeout = (fn, ms) => add(fn, ms, 0);
    clock.clearInterval = (id) => { clock.timers.delete(id); };
    clock.clearTimeout = clock.clearInterval;
    clock.advance = (ms) => {
        const end = clock.now + ms;
        for (;;) {
            let due = null;
            for (const [id, t] of clock.timers) {
                if (t.next <= end && (due === null || t.next < due.t.next)) due = { id, t };
            }
            if (!due) break;
            clock.now = due.t.next;
            if (due.t.every) due.t.next += due.t.every;
            else clock.timers.delete(due.id);
            due.t.fn();
        }
        clock.now = end;
    };
    // A phone that is locked or in another app: the time passes, the timers do not run.
    clock.jump = (ms) => { clock.now += ms; };
    return clock;
}

function makeEvent(extra) {
    return Object.assign({
        defaultPrevented: false,
        preventDefault() { this.defaultPrevented = true; },
    }, extra || {});
}

/** One DOM element: attributes, hidden, textContent, checked, focus() and listeners. */
function makeEl(id, o) {
    const opts = o || {};
    const attrs = Object.assign({}, opts.attrs);
    const listeners = {};
    return {
        id,
        hidden: !!opts.hidden,
        textContent: opts.text || "",
        checked: !!opts.checked,
        focused: 0,
        getAttribute(k) { return Object.prototype.hasOwnProperty.call(attrs, k) ? attrs[k] : null; },
        setAttribute(k, v) { attrs[k] = String(v); },
        removeAttribute(k) { delete attrs[k]; },
        hasAttribute(k) { return Object.prototype.hasOwnProperty.call(attrs, k); },
        addEventListener(type, fn) { (listeners[type] = listeners[type] || []).push(fn); },
        listenerCount(type) { return (listeners[type] || []).length; },
        focus() { this.focused += 1; },
        fire(type, extra) {
            const ev = makeEvent(extra);
            (listeners[type] || []).forEach((fn) => fn(ev));
            return ev;
        },
    };
}

/**
 * Build the page and run the script.
 * @param {{els?: Object<string, object>, selectors?: Object<string, object>, turnstile?: object}} spec
 *   els: element id -> makeEl options; selectors: querySelector string -> element (or null)
 */
function boot(spec) {
    const clock = makeClock();
    const els = {};
    for (const [id, o] of Object.entries(spec.els || {})) els[id] = makeEl(id, o);
    const selectors = Object.assign({}, spec.selectors);
    const docListeners = {};
    const winListeners = {};
    const document = {
        hidden: false,
        getElementById(id) { return Object.prototype.hasOwnProperty.call(els, id) ? els[id] : null; },
        querySelector(sel) { return Object.prototype.hasOwnProperty.call(selectors, sel) ? selectors[sel] : null; },
        addEventListener(type, fn) { (docListeners[type] = docListeners[type] || []).push(fn); },
    };
    const sandbox = {
        document,
        console,
        Date: { now: () => clock.now },
        setInterval: clock.setInterval,
        clearInterval: clock.clearInterval,
        setTimeout: clock.setTimeout,
        clearTimeout: clock.clearTimeout,
        addEventListener(type, fn) { (winListeners[type] = winListeners[type] || []).push(fn); },
    };
    if (spec.turnstile) sandbox.turnstile = spec.turnstile;
    sandbox.window = sandbox;
    vm.createContext(sandbox);
    vm.runInContext(SCRIPT, sandbox, { filename: "phone_login.js" });
    return {
        win: sandbox,
        doc: document,
        els,
        clock,
        selectors,
        docListenerCount: (type) => (docListeners[type] || []).length,
        winListenerCount: (type) => (winListeners[type] || []).length,
        fireDoc: (type) => (docListeners[type] || []).forEach((fn) => fn(makeEvent())),
        fireWin: (type) => (winListeners[type] || []).forEach((fn) => fn(makeEvent())),
    };
}

// ---------------------------------------------------------------- number page

function numberPage(extra) {
    const spec = {
        els: {
            "ph-start-form": {},
            "ph-consent": { checked: false },
            "ph-send": { text: "Text me a code", attrs: { "aria-disabled": "true", "aria-describedby": "ph-consent-hint" } },
            "ph-consent-hint": { text: "Tick the box to continue" },
        },
    };
    return boot(Object.assign(spec, extra || {}));
}

test("ticking the consent box enables the button, hides the hint and drops its aria-describedby; unticking restores all three", () => {
    const page = numberPage();
    const { "ph-consent": box, "ph-send": send, "ph-consent-hint": hint } = page.els;
    // untouched page: the button waits, the hint is read as its description
    assert.equal(send.getAttribute("aria-disabled"), "true");
    assert.equal(hint.hidden, false);
    assert.equal(send.getAttribute("aria-describedby"), "ph-consent-hint");

    box.checked = true;
    box.fire("change");
    assert.equal(send.getAttribute("aria-disabled"), "false");
    assert.equal(hint.hidden, true);
    // a hidden hint would still be announced as a description, so the reference goes with the text
    assert.equal(send.hasAttribute("aria-describedby"), false);

    box.checked = false;
    box.fire("change");
    assert.equal(send.getAttribute("aria-disabled"), "true");
    assert.equal(hint.hidden, false);
    assert.equal(send.getAttribute("aria-describedby"), "ph-consent-hint");
});

test("a consent box the browser restored as ticked starts with an enabled button and no stale description", () => {
    const page = boot({
        els: {
            "ph-start-form": {},
            "ph-consent": { checked: true },
            "ph-send": { text: "Text me a code", attrs: { "aria-disabled": "true", "aria-describedby": "ph-consent-hint" } },
            "ph-consent-hint": { text: "Tick the box to continue" },
        },
    });
    const send = page.els["ph-send"];
    assert.equal(send.getAttribute("aria-disabled"), "false");
    assert.equal(page.els["ph-consent-hint"].hidden, true);
    assert.equal(send.hasAttribute("aria-describedby"), false);
});

test("the number form is refused until the box is ticked, then goes through once and says Sending", () => {
    const page = numberPage();
    const { "ph-start-form": form, "ph-consent": box, "ph-send": send } = page.els;

    const refused = form.fire("submit");
    assert.equal(refused.defaultPrevented, true);
    assert.equal(box.focused, 1, "focus goes to the box that is missing");
    assert.equal(send.textContent, "Text me a code");

    box.checked = true;
    box.fire("change");
    const sent = form.fire("submit");
    assert.equal(sent.defaultPrevented, false);
    assert.equal(send.getAttribute("aria-disabled"), "true");
    assert.equal(send.textContent, "Sending…");

    assert.equal(form.fire("submit").defaultPrevented, true, "a double tap does not send a second text");

    // back from the next page restores this one from the back-forward cache: the button works again
    page.fireWin("pageshow");
    assert.equal(send.textContent, "Text me a code");
    assert.equal(send.getAttribute("aria-disabled"), "false");
});

// ---------------------------------------------------------------- the person check did not load

function checkPage(extra) {
    const widget = makeEl("widget");
    const spec = {
        els: { "ph-check-missing": { hidden: true, text: "The check that you are a person did not load." } },
        selectors: { ".cf-turnstile": widget },
    };
    return boot(Object.assign(spec, extra || {}));
}

test("the 'check did not load' notice stays hidden for 7 seconds, shows at 8, and stops polling after 120", () => {
    const page = checkPage();
    const notice = page.els["ph-check-missing"];
    page.clock.advance(7000);
    assert.equal(notice.hidden, true, "7 s is not long enough to call it missing");
    page.clock.advance(1000);
    assert.equal(notice.hidden, false, "8 s without the widget shows the notice");
    page.clock.advance(112000); // 120 s in all
    assert.equal(notice.hidden, false, "never loaded: the notice stays");
    assert.equal(page.clock.timers.size, 0, "the poll gives up at 120 s");
});

test("a widget that arrives after the notice showed hides it again and stops the poll", () => {
    const page = checkPage();
    const notice = page.els["ph-check-missing"];
    page.clock.advance(9000);
    assert.equal(notice.hidden, false);
    page.win.turnstile = { render() {} }; // Cloudflare's script finished loading
    page.clock.advance(1000);
    assert.equal(notice.hidden, true);
    assert.equal(page.clock.timers.size, 0);
    page.clock.advance(30000);
    assert.equal(notice.hidden, true, "and it does not come back");
});

test("an iframe or the response field counts as the check being there, like window.turnstile", () => {
    for (const sel of [".cf-turnstile iframe", 'input[name="cf-turnstile-response"]']) {
        const page = checkPage();
        const notice = page.els["ph-check-missing"];
        page.clock.advance(9000);
        assert.equal(notice.hidden, false, sel);
        page.selectors[sel] = makeEl("late");
        page.clock.advance(1000);
        assert.equal(notice.hidden, true, sel);
    }
});

test("with window.turnstile there from the start the notice never shows", () => {
    const page = checkPage({ turnstile: { render() {} } });
    const notice = page.els["ph-check-missing"];
    page.clock.advance(200000);
    assert.equal(notice.hidden, true);
    assert.equal(page.clock.timers.size, 0, "found on the first look, so the poll ends there");
});

test("a page without the widget element starts no poll", () => {
    const page = boot({ els: { "ph-check-missing": { hidden: true } } });
    assert.equal(page.clock.timers.size, 0);
    page.clock.advance(60000);
    assert.equal(page.els["ph-check-missing"].hidden, true);
});

// ---------------------------------------------------------------- resend countdown

const READY = "You can ask for another code now.";

function codePage(wait) {
    return boot({
        els: {
            "ph-resend": { text: "Send again", attrs: { "data-wait": String(wait), "aria-disabled": "true" } },
            "ph-wait": { text: "You can ask for another code in " + wait + " seconds." },
            "ph-wait-live": {},
        },
    });
}

test("after a 90 s jump with no timer ticks, coming back to the page enables Send again at once", () => {
    const page = codePage(60);
    const { "ph-resend": resend, "ph-wait": visible, "ph-wait-live": live } = page.els;
    assert.equal(resend.getAttribute("aria-disabled"), "true");
    page.clock.jump(90000); // the phone slept: the interval never fired
    assert.equal(resend.getAttribute("aria-disabled"), "true", "nothing has run yet");

    page.doc.hidden = true;
    page.fireDoc("visibilitychange");
    assert.equal(resend.getAttribute("aria-disabled"), "true", "going to the background does not recount");

    page.doc.hidden = false;
    page.fireDoc("visibilitychange");
    assert.equal(resend.getAttribute("aria-disabled"), "false");
    assert.equal(visible.textContent, READY);
    assert.equal(live.textContent, READY);
    assert.equal(page.clock.timers.size, 0, "the interval is cleared once the wait is over");
});

test("a page restored from the back-forward cache re-syncs the countdown from the deadline", () => {
    const page = codePage(60);
    page.clock.advance(5000);
    page.clock.jump(70000);
    page.fireWin("pageshow");
    assert.equal(page.els["ph-resend"].getAttribute("aria-disabled"), "false");
    assert.equal(page.els["ph-wait"].textContent, READY);
});

test("normal ticks count down from the deadline, rounding up, and announce only at the start and the end", () => {
    const page = codePage(60);
    const { "ph-resend": resend, "ph-wait": visible, "ph-wait-live": live } = page.els;
    const first = "You can ask for another code in 60 seconds.";
    assert.equal(live.textContent, first, "announced once at the start");

    page.clock.advance(10000);
    assert.equal(visible.textContent, "You can ask for another code in 50 seconds.");
    assert.equal(live.textContent, first, "the ticking count is silent");

    // half a second more: 49.5 s are left and the page says 50, never "0" early
    page.clock.jump(500);
    page.fireDoc("visibilitychange");
    assert.equal(visible.textContent, "You can ask for another code in 50 seconds.");
    assert.equal(resend.getAttribute("aria-disabled"), "true");

    page.clock.advance(49500);
    assert.equal(resend.getAttribute("aria-disabled"), "false");
    assert.equal(visible.textContent, READY);
    assert.equal(live.textContent, READY, "announced once more when it is ready");
});

test("the countdown never ends a second early: 59 s into a 60 s wait the button is still off", () => {
    const page = codePage(60);
    page.clock.advance(59000);
    assert.equal(page.els["ph-resend"].getAttribute("aria-disabled"), "true");
    assert.match(page.els["ph-wait"].textContent, /in 1 second/);
    page.clock.advance(1000);
    assert.equal(page.els["ph-resend"].getAttribute("aria-disabled"), "false");
});

test("data-wait of 0 (or junk) attaches no timer and no visibility or pageshow listener, and leaves the markup alone", () => {
    for (const wait of ["0", "-5", "soon", ""]) {
        const page = boot({
            els: {
                "ph-resend": { text: "Send again", attrs: { "data-wait": wait } },
                "ph-wait": { text: READY },
                "ph-wait-live": {},
            },
        });
        const why = "data-wait=" + JSON.stringify(wait);
        assert.equal(page.clock.timers.size, 0, why);
        assert.equal(page.docListenerCount("visibilitychange"), 0, why);
        assert.equal(page.winListenerCount("pageshow"), 0, why);
        assert.equal(page.els["ph-resend"].hasAttribute("aria-disabled"), false, why);
        assert.equal(page.els["ph-wait"].textContent, READY, why);
        assert.equal(page.els["ph-wait-live"].textContent, "", why);
    }
});

test("a click on Send again is cancelled while the wait runs and goes through once it is over", () => {
    const page = codePage(30);
    const resend = page.els["ph-resend"];
    assert.equal(resend.fire("click").defaultPrevented, true);
    page.clock.advance(30000);
    assert.equal(resend.fire("click").defaultPrevented, false);
});

// ---------------------------------------------------------------- code form

test("the code form disables Sign in on the first submit and stops a second one", () => {
    const page = boot({
        els: {
            "ph-code": {},
            "ph-code-form": {},
            "ph-verify": { text: "Sign in" },
        },
    });
    const { "ph-code": code, "ph-code-form": form, "ph-verify": verify } = page.els;
    assert.equal(code.focused, 1, "the code field has the focus on arrival");

    const first = form.fire("submit");
    assert.equal(first.defaultPrevented, false);
    assert.equal(verify.getAttribute("aria-disabled"), "true");
    assert.equal(verify.textContent, "Signing in…");

    const second = form.fire("submit");
    assert.equal(second.defaultPrevented, true, "the second tap would spend a second try on the same code");
    assert.equal(verify.textContent, "Signing in…");

    // back from the next page restores this one from the back-forward cache: usable again
    page.fireWin("pageshow");
    assert.equal(verify.getAttribute("aria-disabled"), "false");
    assert.equal(verify.textContent, "Sign in");
    assert.equal(form.fire("submit").defaultPrevented, false);
});
