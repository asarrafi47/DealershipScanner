"""Browser-side JavaScript payloads for VDP extraction.

These are large inline browser-JS string literals extracted verbatim from
``backend.scanner.vdp.core`` (a mechanical split; ZERO logic change). ``core``
re-exports every name from here so existing imports keep resolving.
"""

GALLERY_MODAL_NUDGE_JS = r"""
() => {
  let n = 0;
  try {
    const d = document.querySelector('[role="dialog"], [aria-modal="true"]');
    if (d) {
      d.scrollTop = d.scrollHeight;
      d.scrollTo(0, d.scrollHeight);
      n += 1;
    }
  } catch (e1) {}
  try {
    const wr = document.querySelector(
      ".swiper-wrapper, [class*='swiper-wrapper'], [class*='thumbnails'], [class*='vdp-media'], [class*='vehicle-gallery'], [data-gallery]"
    );
    if (wr) {
      wr.scrollLeft = wr.scrollWidth;
      wr.scrollTo(wr.scrollWidth, 0);
      n += 1;
    }
  } catch (e2) {}
  try {
    const tb = document.querySelector("[class*='thumbnail'], [class*='thumbs']");
    if (tb) {
      tb.scrollLeft = tb.scrollWidth;
      n += 1;
    }
  } catch (e3) {}
  return n;
}
"""

PAGE_EXTRACT_JS = r"""
() => {
  const result = {
    dataLayerEps: [],
    dataLayerFlatVehicle: [],
    dataLayerRows: 0,
    ldJsonVehicle: [],
    inlineJsonHits: [],
    inlineEpObjects: [],
    domSpecs: {},
    domFeatures: [],
    domBadges: [],
    domGalleryUrls: [],
    jsonGalleryUrls: [],
    domVehicleHistoryUrls: [],
    domStickerUrls: [],
    domMonroneyTextSnippets: [],
    domLocationSnippets: [],
    domDescription: "",
    domDealerNotes: "",
    domPackagesStructured: [],
    domPackagesSections: [],
    domInTransit: false,
    pageTextSample: "",
    vdpPriceHints: [],
    scriptSrcSample: [],
    metaGenerator: "",
    galleryExtractDebug: { photosTabClicked: false, domImgSample: 0 },
    extractDebug: {
      dataLayerLength: 0,
      dataLayerRowTopKeys: [],
      dataLayerEvents: [],
      dataLayerEpCount: 0,
      dataLayerFlatCount: 0,
      inlineEpParseCount: 0,
      inlineKeySamples: [],
      analyticsEventKeys: []
    }
  };
  const epSeen = new Set();
  function pushEp(ep) {
    if (!ep || typeof ep !== "object" || Array.isArray(ep)) return;
    if (epSeen.has(ep)) return;
    epSeen.add(ep);
    result.dataLayerEps.push(ep);
  }
  function walkForEp(obj, depth, seen) {
    if (depth > 18 || !obj || typeof obj !== "object") return;
    if (seen.has(obj)) return;
    seen.add(obj);
    if (obj.ep && typeof obj.ep === "object" && !Array.isArray(obj.ep)) {
      pushEp(obj.ep);
    }
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (!v || typeof v !== "object") continue;
      if (Array.isArray(v)) {
        for (const it of v) walkForEp(it, depth + 1, seen);
      } else {
        walkForEp(v, depth + 1, seen);
      }
    }
  }
  function hasKeyHint(keys, hint) {
    const h = hint.replace(/_/g, "").toLowerCase();
    for (const raw of keys) {
      const k = String(raw).replace(/_/g, "").toLowerCase();
      if (k === h) return true;
      if (h.length >= 6 && (k.indexOf(h) >= 0 || h.indexOf(k) >= 0)) return true;
    }
    return false;
  }
  function vehicleLikePrimitiveScore(o) {
    const keys = Object.keys(o);
    let sc = 0;
    const hints = [
      "transmission", "drivetrain", "drivetype", "drive_train", "driveline", "fueltype", "fuel",
      "engine", "bodystyle", "body_style", "vehiclemodel", "vehicle_model",
      "exteriorcolor", "exterior_color", "interiorcolor", "interior_color",
      "cityfueleconomy", "city_fuel_economy", "highwayfueleconomy", "highway_fuel_economy",
      "cityfuelefficiency", "highwayfuelefficiency",
      "inventorytype", "inventory_type", "certified", "trim", "stockid", "stock_id"
    ];
    for (const h of hints) {
      if (hasKeyHint(keys, h)) sc++;
    }
    for (const k of keys) {
      const lk = String(k).replace(/_/g, "").toLowerCase();
      if (lk === "vin" || lk === "vinnumber") sc += 2;
    }
    return sc;
  }
  function parseJsonFromBrace(txt, openBraceIdx) {
    let depth = 0;
    let start = -1;
    const lim = Math.min(openBraceIdx + 90000, txt.length);
    for (let i = openBraceIdx; i < lim; i++) {
      const c = txt[i];
      if (c === "{") {
        if (depth === 0) start = i;
        depth++;
      } else if (c === "}") {
        depth--;
        if (depth === 0 && start >= 0) {
          const chunk = txt.slice(start, i + 1);
          try {
            return JSON.parse(chunk);
          } catch (e) {
            return null;
          }
        }
      }
    }
    return null;
  }
  const flatSig = new Set();
  function extractVehicleFlat(obj, depth, seen, out) {
    if (depth > 16 || !obj || typeof obj !== "object") return;
    if (seen.has(obj)) return;
    seen.add(obj);
    const prims = {};
    let primCount = 0;
    for (const [k, v] of Object.entries(obj)) {
      if (v === null || typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
        if (String(k).length < 120 && (typeof v !== "string" || v.length < 2000)) {
          prims[k] = v;
          primCount++;
        }
      }
    }
    const sc = vehicleLikePrimitiveScore(prims);
    const vin = prims.vin || prims.VIN || prims.vinNumber;
    const hasVin = vin && String(vin).replace(/\s/g, "").length === 17;
    const good =
      (sc >= 3 && primCount >= 3) ||
      (hasVin && sc >= 2 && primCount >= 3) ||
      (sc >= 4 && primCount >= 2);
    if (good) {
      const sig = JSON.stringify(prims);
      if (!flatSig.has(sig) && out.length < 22) {
        flatSig.add(sig);
        out.push(prims);
      }
    }
    for (const k of Object.keys(obj)) {
      const v = obj[k];
      if (!v || typeof v !== "object") continue;
      if (k === "ep") continue;
      if (Array.isArray(v)) {
        for (const it of v) extractVehicleFlat(it, depth + 1, seen, out);
      } else {
        extractVehicleFlat(v, depth + 1, seen, out);
      }
    }
  }
  try {
    if (window.dataLayer && Array.isArray(window.dataLayer)) {
      result.dataLayerRows = window.dataLayer.length;
      result.extractDebug.dataLayerLength = result.dataLayerRows;
      const rowSeen = new WeakSet();
      const flatSeen = new WeakSet();
      for (let i = 0; i < window.dataLayer.length; i++) {
        const row = window.dataLayer[i];
        walkForEp(row, 0, rowSeen);
        extractVehicleFlat(row, 0, flatSeen, result.dataLayerFlatVehicle);
        if (i < 24) {
          try {
            const ev = row && typeof row === "object" && row.event != null ? String(row.event) : "";
            if (ev) result.extractDebug.dataLayerEvents.push(ev.slice(0, 120));
            if (row && typeof row === "object") {
              const ks = Object.keys(row).slice(0, 50);
              result.extractDebug.dataLayerRowTopKeys.push(ks);
              result.extractDebug.analyticsEventKeys.push(
                (ev || "?").slice(0, 90) + " | " + ks.slice(0, 28).join(", ")
              );
            } else {
              result.extractDebug.dataLayerRowTopKeys.push([]);
            }
          } catch (e2) {}
        }
      }
    }
  } catch (e) {}
  result.extractDebug.dataLayerEpCount = result.dataLayerEps.length;
  result.extractDebug.dataLayerFlatCount = result.dataLayerFlatVehicle.length;
  const lds = document.querySelectorAll('script[type="application/ld+json"]');
  for (const s of lds) {
    try {
      const j = JSON.parse(s.textContent || "{}");
      const stack = Array.isArray(j) ? j : [j];
      for (const node of stack) {
        if (!node || typeof node !== "object") continue;
        const t = [].concat(node["@type"] || []);
        const ts = t.map((x) => String(x).toLowerCase());
        const hasVin = !!(node.vehicleIdentificationNumber || node.vin || node.VIN);
        if (
          ts.some((x) => /vehicle|car|automobile/.test(x)) ||
          (hasVin && ts.some((x) => x === "product"))
        ) {
          result.ldJsonVehicle.push(node);
        }
      }
    } catch (e) {}
  }
  const mg = document.querySelector('meta[name="generator"]');
  if (mg && mg.getAttribute("content")) result.metaGenerator = mg.getAttribute("content").slice(0, 200);
  const sscripts = document.querySelectorAll("script[src]");
  for (let i = 0; i < Math.min(sscripts.length, 35); i++) {
    const src = sscripts[i].getAttribute("src") || "";
    if (src) result.scriptSrcSample.push(src.slice(0, 220));
  }
  const inlineScripts = document.querySelectorAll("script:not([src])");
  const inlineKeySamples = new Set();
  const epLiteral = /["']ep["']\s*:\s*\{/g;
  for (const sc of inlineScripts) {
    const txt = (sc.textContent || "").slice(0, 220000);
    if (txt.length < 40) continue;
    if (!/["']ep["']\s*:|ep\.|vehicle_model|drive_train|driveLine|driveline|fuel_type|transmission|drivetrain|fueltype|bodystyle|inventory_type|cityFuelEfficiency|highwayFuelEfficiency|engineDescription/i.test(txt)) continue;
    let em;
    epLiteral.lastIndex = 0;
    while ((em = epLiteral.exec(txt)) !== null && result.inlineEpObjects.length < 14) {
      const braceIdx = txt.indexOf("{", em.index);
      if (braceIdx < 0) continue;
      const parsedEp = parseJsonFromBrace(txt, braceIdx);
      if (parsedEp && typeof parsedEp === "object" && !Array.isArray(parsedEp)) {
        result.inlineEpObjects.push(parsedEp);
        result.extractDebug.inlineEpParseCount++;
        Object.keys(parsedEp).slice(0, 55).forEach((k) => inlineKeySamples.add(k));
      }
    }
    if (/cityFuelEconomy|highwayFuelEconomy|cityFuelEfficiency/i.test(txt) && result.inlineEpObjects.length < 14) {
      const vinFuel = txt.match(/"vin"\\s*:\\s*"([A-HJ-NPR-Z0-9]{17})"/i);
      if (vinFuel && vinFuel.index != null) {
        const start = txt.lastIndexOf("{", vinFuel.index);
        if (start >= 0) {
          const parsedFuel = parseJsonFromBrace(txt, start);
          if (parsedFuel && typeof parsedFuel === "object" && !Array.isArray(parsedFuel)) {
            const hasFuel =
              parsedFuel.cityFuelEconomy != null ||
              parsedFuel.highwayFuelEconomy != null ||
              parsedFuel.cityFuelEfficiency != null ||
              parsedFuel.highwayFuelEfficiency != null;
            if (hasFuel) {
              result.inlineEpObjects.push(parsedFuel);
              result.extractDebug.inlineEpParseCount++;
            }
          }
        }
      }
    }
  }
  result.extractDebug.inlineKeySamples = Array.from(inlineKeySamples).slice(0, 70);
  for (const sc of inlineScripts) {
    const txt = (sc.textContent || "").slice(0, 120000);
    if (txt.length < 80) continue;
    if (!/vin|vehicle|inventory|drivetrain|driveline|driveLine|transmission|["']ep["']/i.test(txt)) continue;
    let parsed = null;
    try {
      const m = txt.match(/\\{\\s*"vin"\\s*:\\s*"[^"]+"/i);
      if (m && m.index != null) {
        let depth = 0;
        let start = -1;
        for (let i = m.index; i < Math.min(m.index + 25000, txt.length); i++) {
          const c = txt[i];
          if (c === "{") {
            if (depth === 0) start = i;
            depth++;
          } else if (c === "}") {
            depth--;
            if (depth === 0 && start >= 0) {
              const chunk = txt.slice(start, i + 1);
              try {
                parsed = JSON.parse(chunk);
                break;
              } catch (e) {
                parsed = null;
              }
            }
          }
        }
      }
    } catch (e) {}
    if (parsed && typeof parsed === "object") {
      result.inlineJsonHits.push(parsed);
      if (result.inlineJsonHits.length >= 5) break;
    }
  }
  // Expand accordion / collapsible spec sections before reading (handles + buttons like cars.com)
  try {
    const accordionTriggers = Array.from(document.querySelectorAll(
      "button[aria-expanded='false'], [role='button'][aria-expanded='false'], " +
      ".accordion-trigger:not(.active), .collapsible:not(.open), " +
      "[class*='accordion'][class*='header'], [class*='toggle'][class*='spec'], " +
      "summary, details:not([open]) > summary"
    ));
    for (const el of accordionTriggers.slice(0, 40)) {
      try {
        const t = (el.textContent || "").trim().toLowerCase();
        if (/spec|feature|convenience|suspension|powertrain|body|safety|seat|entertain|lighting|dimension|equipment|package|option|accessori|standard|dealer notes|included/i.test(t)) {
          el.click();
        }
      } catch (e) {}
    }
  } catch (e) {}

  const specSelectors = [
    "dl", "dl.vehicle-specs", ".vehicle-specs", ".specifications",
    "table.specs", ".vdp-specs", ".spec-list", "[class*='spec-table']",
    "[class*='features-list']", ".vehicle-features",
  ];
  for (const sel of specSelectors) {
    try {
      const els = document.querySelectorAll(sel);
      els.forEach((el) => {
        const rows = el.querySelectorAll("tr, dt, li, [class*='spec-item'], [class*='feature-item']");
        rows.forEach((row) => {
          const label = (row.querySelector("th, dt, .label, .name, [class*='label'], [class*='name']") || row.cells?.[0])?.textContent?.trim();
          const val = (row.querySelector("td, dd, .value, [class*='value']") || row.cells?.[1])?.textContent?.trim();
          if (label && val && label.length < 80 && val.length < 400) {
            result.domSpecs[label.slice(0, 60)] = val.slice(0, 300);
          } else if (!val && label && label.length < 120) {
            // single-column feature list item
            result.domFeatures.push(label.slice(0, 120));
          }
        });
      });
    } catch (e) {}
  }
  // autoWALL / ShopperExpress style: div.row containing exactly two div.col children (label + value)
  try {
    document.querySelectorAll(".row").forEach((row) => {
      const cols = row.querySelectorAll(":scope > .col");
      if (cols.length === 2) {
        const label = (cols[0].textContent || "").trim().replace(/:$/, "");
        const val = (cols[1].textContent || "").trim();
        if (label && val && label.length < 60 && val.length < 200) {
          result.domSpecs[label.slice(0, 60)] = val.slice(0, 300);
        }
      }
    });
  } catch (e) {}
  document.querySelectorAll("[class*='feature'], .features li, ul.features li, [class*='highlight'] li").forEach((el, idx) => {
    if (idx > 100) return;
    const t = (el.textContent || "").trim();
    if (t && t.length < 200) result.domFeatures.push(t);
  });
  document.querySelectorAll(".badge, [class*='badge'], .label-pill, [class*='tag']").forEach((el, idx) => {
    if (idx > 40) return;
    const t = (el.textContent || "").trim();
    if (t && t.length < 120) result.domBadges.push(t);
  });
  try {
    const packageSectionRe = /included packages|packages\\s*&\\s*accessories|packages\\s*&\\s*options|standard features|included options|factory installed|equipment groups|(?:^|\\s)(?:[\\w/&+-]+\\s+)*package\\s*$/i;
    const dealerNotesRe = /^dealer notes\b|^seller notes\b|^dealer comments\b|^about this vehicle\b/i;
    const priceRe = /\\$[\\d,]+(?:\\.\\d{2})?/;
    try {
      const pkgAccordions = Array.from(document.querySelectorAll(
        "h4[aria-expanded='false'], h3[aria-expanded='false'], button[aria-expanded='false'], [role='button'][aria-expanded='false']"
      )).filter((el) => {
        const t = (el.textContent || "").trim().toLowerCase();
        return /package/.test(t) && !/spec|dimension|powertrain|suspension|safety|convenience|entertainment/i.test(t);
      });
      for (const el of pkgAccordions.slice(0, 35)) {
        try { el.click(); } catch (ePkgAcc) {}
      }
      const showAllPkg = Array.from(
        document.querySelectorAll("button, a, [role='button']")
      ).find((el) => /show all package items/i.test((el.textContent || "").trim()));
      if (showAllPkg) showAllPkg.click();
    } catch (eShowAll) {}
    const pkgSeen = new Set();
    function pushPkg(section, name, priceLabel, features) {
      const n = (name || "").trim().replace(/\\s+/g, " ");
      if (!n || n.length < 2 || n.length > 180) return;
      const key = (section + "|" + n + "|" + (priceLabel || "")).toLowerCase();
      if (pkgSeen.has(key)) return;
      pkgSeen.add(key);
      const row = { section: section, name: n, features: (features || []).slice(0, 24) };
      if (priceLabel) row.price_label = priceLabel;
      const pm = (priceLabel || n).match(priceRe);
      if (pm) {
        const num = parseFloat(pm[0].replace(/[$,]/g, ""));
        if (isFinite(num) && num > 0) row.price = Math.round(num);
      }
      result.domPackagesStructured.push(row);
    }
    function sectionRootForHeading(h) {
      return (
        h.closest("section, article, [class*='package'], [class*='option'], [class*='feature'], [class*='equipment'], [class*='accessory']")
        || h.parentElement
      );
    }
    function parsePackageBlock(root, sectionName) {
      if (!root) return;
      const rows = root.querySelectorAll(
        "li, tr, [class*='package-row'], [class*='option-row'], [class*='feature-row'], " +
        "[class*='package-item'], [class*='option-item'], dt, .row"
      );
      rows.forEach((row) => {
        const txt = (row.innerText || row.textContent || "").trim().replace(/\\s+/g, " ");
        if (!txt || txt.length < 3 || txt.length > 500) return;
        const lines = txt.split(/\\n+/).map((x) => x.trim()).filter(Boolean);
        if (!lines.length) return;
        const head = lines[0];
        const pm = head.match(priceRe);
        let name = head;
        let priceLabel = "";
        if (pm) {
          priceLabel = pm[0];
          name = head.replace(priceRe, "").trim();
        }
        if (!name || name.length < 2) return;
        const feats = lines.slice(1).filter((ln) => ln.length >= 2 && ln.length <= 160);
        pushPkg(sectionName, name, priceLabel, feats);
      });
    }
    const headingTags2 = ["h1", "h2", "h3", "h4", "h5", "legend", "button", "[role='button']"];
    for (const tag of headingTags2) {
      document.querySelectorAll(tag).forEach((h) => {
        const label = (h.textContent || "").trim();
        if (!label || label.length > 120) return;
        if (dealerNotesRe.test(label)) {
          const root = sectionRootForHeading(h);
          const body = root ? (root.innerText || root.textContent || "").trim() : "";
          const cleaned = body.replace(new RegExp("^" + label.replace(/[.*+?^${}()|[\\]\\\\]/g, "\\\\$&"), "i"), "").trim();
          if (cleaned.length > (result.domDealerNotes || "").length) {
            result.domDealerNotes = cleaned.slice(0, 4000);
          }
          return;
        }
        if (!packageSectionRe.test(label)) return;
        const sectionName = label.toLowerCase().replace(/\\s+/g, "_").slice(0, 60);
        if (!result.domPackagesSections.includes(sectionName)) {
          result.domPackagesSections.push(sectionName);
        }
        const root = sectionRootForHeading(h);
        parsePackageBlock(root, sectionName);
        let sib = h.nextElementSibling;
        for (let i = 0; i < 3 && sib; i++) {
          parsePackageBlock(sib, sectionName);
          sib = sib.nextElementSibling;
        }
      });
    }
  } catch (ePkg) {}
  try {
    const tabCands = Array.from(
      document.querySelectorAll("a, button, [role='tab'], [data-tab], [data-toggle]")
    ).filter((el) => {
      const t = (el.textContent || "").trim().toLowerCase();
      if (!t || t.length > 28) return false;
      return (
        t === "photos" ||
        t === "pictures" ||
        t === "images" ||
        t === "gallery" ||
        /^photo(s)?$/i.test(t)
      );
    });
    const visible = tabCands.filter((el) => {
      try {
        const st = window.getComputedStyle(el);
        return st.display !== "none" && st.visibility !== "hidden" && el.offsetParent !== null;
      } catch (e) {
        return false;
      }
    });
    if (visible.length === 1) {
      visible[0].click();
      result.galleryExtractDebug.photosTabClicked = true;
    }
  } catch (e) {}
  try {
    window.scrollTo(0, Math.min(3200, (document.body && document.body.scrollHeight) || 0));
  } catch (e) {}
  const imgSeen = new Set();
  function isLikelyVdpJunkImage(el) {
    let cur = el;
    for (let d = 0; d < 10 && cur; d++) {
      const cls = (cur.className && String(cur.className)) || "";
      const cid = (cur.id && String(cur.id)) || "";
      const role = (cur.getAttribute && cur.getAttribute("role")) || "";
      const t = (cls + " " + cid + " " + role).toLowerCase();
      if (
        /(cfx|cfximg|ipacket|i-packet|i_packet|vpp|vehicle-?protec|warrantytile|vpp-|-vpp-|-vpp|carfax-?widget|kbb-?widget|dealer-?feature-?ti|dealer-?ti|value-?your-?trade|as-?is-?disclaim|asistile|recall-?polic|financ|about-?us-?|warranty-?ext|warranty-?flyer|vdp-?tile|quick-?link|vehicle-?broch|apply-?fin|ipocket|i-pocket|mlp-?image|fandi|fi_badge|plan-?overview|certified|bmw-?certified|m-?performance|brand-?logo|dealer-?logo|marketing|promo|social|banner)/i.test(
          t
        )
      ) {
        return true;
      }
      cur = cur.parentElement;
    }
    return false;
  }
  const imgSelectorsSpecific = [
    ".vehicle-image-gallery img",
    ".vehicle-photos img",
    ".gallery img",
    "[class*='photo-gallery'] img",
    "[class*='vehicle-photo'] img",
    "[class*='image-gallery'] img",
    "[class*='media-gallery'] img",
  ];
  for (const sel of imgSelectorsSpecific) {
    try {
      document.querySelectorAll(sel).forEach((el, idx) => {
        if (idx > 160 || result.domGalleryUrls.length >= 96) return;
        if (isLikelyVdpJunkImage(el)) return;
        const s =
          el.getAttribute("src") ||
          el.getAttribute("data-src") ||
          el.getAttribute("data-lazy-src") ||
          el.getAttribute("data-original") ||
          "";
        const t = (s || "").trim();
        if (!/^https?:\/\//i.test(t)) return;
        if (!/\.(jpe?g|png|webp|gif)(\?|$)/i.test(t)) return;
        if (imgSeen.has(t)) return;
        imgSeen.add(t);
        result.domGalleryUrls.push(t.slice(0, 900));
      });
    } catch (e) {}
    if (result.domGalleryUrls.length >= 80) break;
  }
  result.galleryExtractDebug.domImgSample = result.domGalleryUrls.length;
  const jSeen = new Set();
  function pushJsonImg(s) {
    if (!s || typeof s !== "string") return;
    const t = s.trim();
    if (!/^https?:\/\//i.test(t)) return;
    if (!/\.(jpe?g|png|webp|gif)(\?|$)/i.test(t)) return;
    if (jSeen.size >= 96) return;
    if (jSeen.has(t)) return;
    jSeen.add(t);
    result.jsonGalleryUrls.push(t.slice(0, 900));
  }
  function walkJsonImg(o, d) {
    if (d > 14 || !o || typeof o !== "object") return;
    if (Array.isArray(o)) {
      for (const x of o) walkJsonImg(x, d + 1);
      return;
    }
    for (const [k, v] of Object.entries(o)) {
      const lk = String(k).toLowerCase().replace(/_/g, "");
      if (
        /photo|image|media|gallery|spin|thumb|picture|carousel|viewer|asset/i.test(lk) &&
        (typeof v === "string" || Array.isArray(v) || (v && typeof v === "object"))
      ) {
        if (typeof v === "string") pushJsonImg(v);
        else if (Array.isArray(v)) {
          for (const it of v) {
            if (typeof it === "string") pushJsonImg(it);
            else if (it && typeof it === "object") {
              const u =
                it.url || it.URL || it.uri || it.src || it.href || it.large || it.full || it.xlarge;
              if (typeof u === "string") pushJsonImg(u);
            }
          }
        } else if (v && typeof v === "object") {
          const u = v.url || v.URL || v.uri || v.src;
          if (typeof u === "string") pushJsonImg(u);
        }
      }
      if (v && typeof v === "object") walkJsonImg(v, d + 1);
    }
  }
  try {
    if (window.dataLayer && Array.isArray(window.dataLayer)) {
      for (let i = 0; i < Math.min(14, window.dataLayer.length); i++) {
        walkJsonImg(window.dataLayer[i], 0);
      }
    }
  } catch (e) {}
  for (const node of result.inlineJsonHits || []) {
    walkJsonImg(node, 0);
  }
  for (const node of result.ldJsonVehicle || []) {
    walkJsonImg(node, 0);
  }
  function pushPriceHint(raw, source) {
    if (raw === null || raw === undefined) return;
    let num = null;
    if (typeof raw === "number" && isFinite(raw)) {
      num = raw;
    } else if (typeof raw === "string") {
      const t = raw.replace(/[$,]/g, "").trim();
      if (!t || /call|contact|request|quote|inquire/i.test(t)) return;
      const m = t.match(/(\\d{3,7})(?:\\.\\d{2})?/);
      if (m) num = parseFloat(m[1]);
    }
    if (num === null || !isFinite(num) || num < 500 || num > 2500000) return;
    result.vdpPriceHints.push({ value: num, raw: String(raw).slice(0, 60), source: String(source || "?") });
  }
  function walkDataLayerPrice(obj, depth, seen) {
    if (depth > 14 || !obj || typeof obj !== "object" || seen.has(obj)) return;
    seen.add(obj);
    const keys = [
      "internetPrice",
      "InternetPrice",
      "salePrice",
      "SalePrice",
      "sellingPrice",
      "price",
      "Price",
      "vehiclePrice",
      "askingPrice",
      "listPrice",
      "retailPrice",
      "finalPrice",
      "cashPrice",
      "msrp",
      "MSRP",
      "primaryPrice",
      "advertisedPrice",
    ];
    for (const k of keys) {
      if (Object.prototype.hasOwnProperty.call(obj, k)) pushPriceHint(obj[k], "dataLayer:" + k);
    }
    for (const v of Object.values(obj)) {
      if (v && typeof v === "object") walkDataLayerPrice(v, depth + 1, seen);
    }
  }
  try {
    if (window.dataLayer && Array.isArray(window.dataLayer)) {
      const seen = new WeakSet();
      for (let i = 0; i < Math.min(40, window.dataLayer.length); i++) {
        walkDataLayerPrice(window.dataLayer[i], 0, seen);
      }
    }
  } catch (e3) {}
  function offersFromLd(node) {
    const out = [];
    if (!node || typeof node !== "object") return out;
    const o = node.offers || node.offer;
    if (!o) return out;
    return [].concat(o);
  }
  for (const node of result.ldJsonVehicle || []) {
    for (const off of offersFromLd(node)) {
      if (!off || typeof off !== "object") continue;
      const p = off.price || off.Price || (off.priceSpecification && off.priceSpecification.price);
      pushPriceHint(p, "json_ld_offer");
    }
    const p2 = node.price || node.Price;
    if (p2) pushPriceHint(p2, "json_ld_product");
  }
  try {
    document.querySelectorAll('[itemprop="price"],[itemprop=price]').forEach((el, idx) => {
      if (idx > 12) return;
      const c = el.getAttribute("content");
      if (c) pushPriceHint(c, "dom_itemprop");
      else pushPriceHint((el.textContent || "").trim(), "dom_itemprop");
    });
  } catch (e4) {}
  try {
    document.querySelectorAll('meta[property="price"],meta[itemprop="price"]').forEach((el, idx) => {
      if (idx > 10) return;
      const c = el.getAttribute("content");
      if (c) pushPriceHint(c, "dom_meta_price");
    });
  } catch (e4b) {}
  const priceSelectors = [
    ".vehicle-price",
    ".internetPrice",
    ".internet-price",
    ".sale-price",
    ".final-price",
    ".primary-price",
    ".price-value",
    ".pricing-price",
    ".price-block",
    ".highlight-price",
    ".srp-price",
    ".priceDisplay",
    "#vehicle-price",
    "#price",
    "[class*='vehicle-price']",
    "[class*='asking-price']",
    "[class*='list-price']",
    "[data-vehicle-price]",
    "[data-price]",
    "[data-selling-price]",
    "[data-internet-price]",
    "[data-msrp]",
    "[data-final-price]",
    "[data-testid*='price']",
    "[class*='PriceDisplay']",
    "[class*='vehiclePrice']",
  ];
  for (const sel of priceSelectors) {
    try {
      const el = document.querySelector(sel);
      if (!el) continue;
      const t = (el.textContent || "").trim();
      if (t && t.length < 80) pushPriceHint(t, "dom_dealer:" + sel.slice(0, 40));
    } catch (e5) {}
  }
  result.domVehicleHistoryUrls = [];
  result.domStickerUrls = [];
  result.domMonroneyTextSnippets = [];
  function pushStickerUrl(raw) {
    const h = absUrl(raw);
    if (!/^https?:\/\//i.test(h)) return;
    const low = h.toLowerCase();
    if (
      low.indexOf("sticker-puller") >= 0 ||
      low.indexOf("autoipacket.com") >= 0 ||
      low.indexOf("ipacket.com") >= 0 ||
      /monroney|window-sticker|window_sticker/i.test(low) ||
      low.indexOf("/sticker/") >= 0
    ) {
      if (result.domStickerUrls.includes(h)) return;
      result.domStickerUrls.push(h.slice(0, 900));
    }
  }
  try {
    const html = (document.documentElement && document.documentElement.innerHTML) || "";
    const stickerRe = /https?:\/\/[^"'\\s<>]+sticker-puller\/download\/[^"'\\s<>]+/gi;
    let sm;
    while ((sm = stickerRe.exec(html)) !== null && result.domStickerUrls.length < 8) {
      pushStickerUrl(sm[0]);
    }
  } catch (eStickerHtml) {}
  try {
    document
      .querySelectorAll(
        'a[href*="sticker-puller"], a[href*="autoipacket"], a[href*="monroney"], iframe[src*="autoipacket"], iframe[src*="ipacket"], [data-sticker-url], [data-msrp-url]'
      )
      .forEach((el, idx) => {
        if (idx > 40 || result.domStickerUrls.length >= 8) return;
        pushStickerUrl(
          el.getAttribute("href") ||
            el.getAttribute("src") ||
            el.getAttribute("data-sticker-url") ||
            el.getAttribute("data-msrp-url") ||
            ""
        );
      });
  } catch (eStickerDom) {}
  function absUrl(href) {
    try {
      if (!href || typeof href !== "string") return "";
      const t = href.trim();
      if (!t || t.toLowerCase().indexOf("javascript:") === 0) return "";
      const u = new URL(t, document.baseURI);
      return u.href;
    } catch (eAbs) {
      return "";
    }
  }
  try {
    document
      .querySelectorAll(
        'a[href*="carfax"], a[href*="CARFAX"], a[href*="vhr.carfax"], a[href*="autocheck"], a[href*="AutoCheck"], area[href]'
      )
      .forEach((a, idx) => {
        if (idx > 70 || result.domVehicleHistoryUrls.length >= 16) return;
        const h = absUrl(a.getAttribute("href") || "");
        if (!/^https?:\/\//i.test(h)) return;
        const low = h.toLowerCase();
        if (low.indexOf("carfax") < 0 && low.indexOf("autocheck") < 0) return;
        if (result.domVehicleHistoryUrls.includes(h)) return;
        result.domVehicleHistoryUrls.push(h.slice(0, 900));
      });
  } catch (eVhr) {}
  try {
    document.querySelectorAll("[data-carfax-url], [data-carfax-href], [data-vhr-url]").forEach((el, idx) => {
      if (idx > 30 || result.domVehicleHistoryUrls.length >= 18) return;
      const raw =
        el.getAttribute("data-carfax-url") ||
        el.getAttribute("data-carfax-href") ||
        el.getAttribute("data-vhr-url") ||
        "";
      const h = absUrl(raw);
      if (!/^https?:\/\//i.test(h)) return;
      if (result.domVehicleHistoryUrls.includes(h)) return;
      result.domVehicleHistoryUrls.push(h.slice(0, 900));
    });
  } catch (eDa) {}
  let monoBudget = 0;
  const monoSelectors = [
    "[class*='monroney']",
    "[class*='Monroney']",
    "[class*='window-sticker']",
    "[class*='windowSticker']",
    "[class*='WindowSticker']",
    "[id*='monroney']",
    "[id*='Monroney']",
    "[data-widget*='sticker']",
  ];
  for (const sel of monoSelectors) {
    try {
      document.querySelectorAll(sel).forEach((el) => {
        if (monoBudget >= 5 || result.domMonroneyTextSnippets.length >= 5) return;
        const t = (el.textContent || "").trim().replace(/\\s+/g, " ");
        if (t.length < 50 || t.length > 3200) return;
        if (!/engine|trans|equip|option|msrp|vin|standard|included|warranty|drivetrain|fuel/i.test(t)) return;
        result.domMonroneyTextSnippets.push(t.slice(0, 2400));
        monoBudget++;
      });
    } catch (eM) {}
  }
  result.domLocationSnippets = [];
  const locSeen = new Set();
  function pushLocSnippet(t) {
    const s = (t || "").trim().replace(/\\s+/g, " ");
    if (!s || s.length < 4 || s.length > 220) return;
    const k = s.toLowerCase();
    if (locSeen.has(k)) return;
    locSeen.add(k);
    result.domLocationSnippets.push(s);
  }
  try {
    const locSelectors = [
      "[class*='located']",
      "[class*='dealer-loc']",
      "[class*='vehicle-loc']",
      "[class*='lot-loc']",
      "[class*='store-loc']",
      "[data-dealer-name]",
      "[data-location-name]",
      ".dealer-name",
      ".dealerName",
      ".vehicle-location",
      ".inventory-location",
    ];
    for (const sel of locSelectors) {
      document.querySelectorAll(sel).forEach((el, idx) => {
        if (idx > 40 || result.domLocationSnippets.length >= 12) return;
        const t = (el.textContent || el.getAttribute("data-dealer-name") || el.getAttribute("data-location-name") || "").trim();
        if (/locat|dealer|lot|store|at\\s/i.test(t) || t.length < 80) pushLocSnippet(t);
      });
    }
  } catch (eLoc) {}
  try {
    const bodyTxt = ((document.body && document.body.innerText) || "").slice(0, 12000);
    result.pageTextSample = bodyTxt.slice(0, 4000);
    const locRe = /located\\s+at\\s+([^\\n.]{4,120})/gi;
    let lm;
    while ((lm = locRe.exec(bodyTxt)) !== null && result.domLocationSnippets.length < 10) {
      pushLocSnippet(lm[1]);
    }
    const locRe2 = /(?:^|\\n)location\\s*:\\s*([^\\n,|.]{4,120})/gi;
    let lm2;
    while ((lm2 = locRe2.exec(bodyTxt)) !== null && result.domLocationSnippets.length < 10) {
      pushLocSnippet(lm2[1]);
    }
  } catch (eBody) {}
  try {
    function pushDescription(t) {
      const s = (t || "").trim().replace(/\\s+/g, " ");
      if (!s || s.length < 60) return;
      if (!result.domDescription || s.length > result.domDescription.length) {
        result.domDescription = s.slice(0, 4000);
      }
    }
    const descHeadingRe = /^description\b|^dealer notes\b|^seller notes\b|^dealer comments\b/i;
    const headingTags = ["h1", "h2", "h3", "h4", "h5", "h6", "legend", "label"];
    for (const tag of headingTags) {
      document.querySelectorAll(tag).forEach((h) => {
        const label = (h.textContent || "").trim();
        if (!descHeadingRe.test(label)) return;
        let block = h.nextElementSibling;
        for (let i = 0; i < 4 && block; i++) {
          const t = (block.innerText || block.textContent || "").trim();
          if (t.length > 60 && !/^description\\b/i.test(t)) {
            pushDescription(t);
            return;
          }
          block = block.nextElementSibling;
        }
        const section = h.closest("section, article, [class*='description'], [id*='description']");
        if (section) {
          const t = (section.innerText || section.textContent || "").trim();
          if (t.length > 80) pushDescription(t.replace(/^description\\s*/i, ""));
        }
      });
    }
    for (const sel of (
      ".vehicle-description, .vdp-description, [class*='vehicle-description'], "
      + "[class*='vdp-description'], #vehicle-description, .description-content, "
      + "[data-testid*='description'], [class*='Description']"
    ).split(", ")) {
      document.querySelectorAll(sel).forEach((el) => {
        const t = (el.innerText || el.textContent || "").trim();
        if (t.length > 60) pushDescription(t);
      });
    }
  } catch (eDesc) {}
  try {
    const bodyForTransit = ((document.body && document.body.innerText) || "").slice(0, 16000);
    if (
      /vehicle\\s+is\\s+currently\\s+in\\s+transit/i.test(bodyForTransit)
      || /vehicle\\s+in\\s+transit/i.test(bodyForTransit)
      || /\\bin\\s+transit\\b/i.test(bodyForTransit)
    ) {
      result.domInTransit = true;
    }
  } catch (eTransit) {}
  return result;
}
"""

GALLERY_COLLECT_URLS_JS = r"""
() => {
  const out = [];
  const seen = new Set();
  const bgRe = /url\\(\\s*['"]?([^'")\\s>]+)['"]?\\s*\\)/gi;
  const cdnQParam = /[?&](fmt|format|f_auto|w_auto|fit|q|w|h)=/i;
  function mightBeRasterUrl(low) {
    if (/\\.(jpe?g|png|webp|gif|avif)(\\?|#|$)/i.test(low)) return true;
    if (cdnQParam.test(low) && /(image|photo|media|cdn|dealer|inventory|vehicle|res\\.cloudinary|imgix|akamai|spin|impel|cfassets|photobucket)/i.test(low))
      return true;
    if (/(\\/image\\/|\\/images\\/|\\/photos\\/|\\/media\\/|cloudinary|dealerinspire|dealer\\.com|inventoryphoto|resizable)/i.test(low)) return true;
    return false;
  }
  const MIN_TO_TRUST = 2;
  function push(u) {
    if (!u || typeof u !== "string") return;
    let t = u.trim();
    if (t.startsWith("//")) t = "https:" + t;
    if (t.startsWith("http://")) t = "https://" + t.slice(7);
    if (!/^https:\/\//i.test(t)) return;
    const low = t.toLowerCase();
    if (!mightBeRasterUrl(low)) return;
    if (isLikelyJunkUrl(low)) return;
    if (seen.has(t)) return;
    seen.add(t);
    if (out.length < 220) out.push(t.slice(0, 900));
  }
  function isLikelyJunkUrl(low) {
    if (
      /(logo|icon|badge|banner|certified|cfximg|cfx\\/|\\/cfx\\/|placeholder|favicon|spinner|loading|m[-_]?logo|bmw[-_]?certified|m[-_]?performance|oem[-_]?vin[-_]?stock|generic-bmw|stackadapt|carnow|agent-0|marketing|promo|sprite|powered-by|value-your-trade|quick-link|warranty-tile|dealer-logo|brand-logo|social-share|facebook|instagram|youtube|twitter)/i.test(
        low
      )
    ) {
      return true;
    }
    if (/[?&](?:w|width|h|height)=\d{1,2}(?:&|$|\/)/i.test(low)) return true;
    return false;
  }
  function fromSrcset(ss) {
    if (!ss || typeof ss !== "string") return;
    for (const part of ss.split(",")) {
      const p = part.trim().split(/\\s+/)[0];
      if (p) push(p);
    }
  }
  function fromBackgroundString(bg) {
    if (!bg || typeof bg !== "string") return;
    const s = bg.trim();
    if (!s || /^none$|^initial$|^inherit$/i.test(s)) return;
    let m;
    const r = new RegExp(bgRe.source, "gi");
    while ((m = r.exec(s)) !== null) {
      if (m[1]) push(m[1].replace(/^["']|["']$/g, ""));
    }
  }
  function countImgs(node) {
    if (!node || !node.querySelectorAll) return 0;
    try {
      return node.querySelectorAll("img").length;
    } catch (eC) {
      return 0;
    }
  }
  function pickGalleryRoot() {
    let best = null;
    let bestN = 0;
    const dialogRows = [];
    try {
      document.querySelectorAll('[role="dialog"], [aria-modal="true"]').forEach((el) => {
        const n = countImgs(el);
        if (n >= MIN_TO_TRUST) dialogRows.push({ el, n });
      });
    } catch (eD) {}
    for (const row of dialogRows) {
      if (row.n > bestN) {
        bestN = row.n;
        best = row.el;
      }
    }
    if (best) return best;
    const fallbacks = [
      ".lightbox",
      ".media-modal",
      ".photo-viewer",
      ".gallery-modal",
      ".image-modal",
      "[class*='MuiDialog']",
      "[class*='MuiModal']",
      "[class*='dealer-image-gallery']",
      "[class*='image-gallery--']",
      "[class*='lightbox']",
      "[class*='media-modal']",
      "[class*='photo-viewer']",
      "[class*='gallery-modal']",
      "[class*='image-lightbox']",
    ];
    for (const sel of fallbacks) {
      try {
        const els = document.querySelectorAll(sel);
        for (const el of els) {
          const n = countImgs(el);
          if (n >= MIN_TO_TRUST && n > bestN) {
            bestN = n;
            best = el;
          }
        }
      } catch (eF) {}
    }
    if (best) return best;
    return null;
  }
  function isLikelyVdpJunkContext(img) {
    let cur = img;
    for (let d = 0; d < 10 && cur; d++) {
      const cls = (cur.className && String(cur.className)) || "";
      const cid = (cur.id && String(cur.id)) || "";
      const role = (cur.getAttribute && cur.getAttribute("role")) || "";
      const t = (cls + " " + cid + " " + role).toLowerCase();
      if (
        /(cfx|cfximg|ipacket|i-packet|i_packet|vpp|vehicle-?protec|warrantytile|vpp-|-vpp-|-vpp|carfax-?widget|kbb-?widget|dealer-?feature-?ti|dealer-?ti|value-?your-?trade|as-?is-?disclaim|asistile|recall-?polic|financ|about-?us-?|warranty-?ext|warranty-?flyer|vdp-?tile|quick-?link|vehicle-?broch|apply-?fin|ipocket|i-pocket|mlp-?image|fandi|fi_badge|plan-?overview|certified|bmw-?certified|m-?performance|brand-?logo|dealer-?logo|marketing|promo|social|banner)/i.test(
          t
        )
      ) {
        return true;
      }
      cur = cur.parentElement;
    }
    return false;
  }
  const picked = pickGalleryRoot();
  const roots = [];
  if (picked) {
    roots.push(picked);
  } else {
    try {
      document
        .querySelectorAll(
          ".vehicle-image-gallery, .vehicle-photos, [class*='vdp-photo'], [class*='VDP-Photo'], [class*='image-gallery__'], [class*='media-gallery'], [class*='vdp-media'], [class*='vehicle-gallery'], .vdp-gallery, [data-gallery]"
        )
        .forEach((h) => roots.push(h));
    } catch (eP) {}
  }
  if (roots.length === 0) {
    try {
      roots.push(document);
    } catch (eD) {
      return out;
    }
  }
  for (const root of roots) {
    try {
      root.querySelectorAll("img").forEach((img, idx) => {
        if (idx > 220) return;
        if (isLikelyVdpJunkContext(img)) return;
        try {
          if (img.currentSrc) push(img.currentSrc);
        } catch (e0) {}
        push(img.getAttribute("src"));
        fromSrcset(img.getAttribute("srcset"));
        const lazy = [
          "data-src",
          "data-lazy-src",
          "data-original",
          "data-lazy",
          "data-image",
          "data-zoom-src",
          "data-fullsrc",
        ];
        for (const a of lazy) push(img.getAttribute(a));
      });
    } catch (e1) {}
    try {
      root.querySelectorAll("picture source[srcset], picture source[src]").forEach((src, idx) => {
        if (idx > 80) return;
        fromSrcset(src.getAttribute("srcset"));
        push(src.getAttribute("src"));
      });
    } catch (e2) {}
  }
  return out;
}
"""
