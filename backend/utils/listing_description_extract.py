"""
Two-tier extraction from dealer listing descriptions (deterministic + optional LLM).

Outputs structured hints only — no full marketing prose. Parser version is bumped
when the output shape or heuristics change incompatibly.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any

from backend.utils.oem_option_catalog import color_phrase_candidates, resolve_catalog_name

logger = logging.getLogger(__name__)

LISTING_DESCRIPTION_PARSER_VERSION = "2"

_BOILERPLATE_LINE_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"^\s*call\s+us\b", re.I),
    re.compile(r"^\s*contact\s+us\b", re.I),
    re.compile(r"^\s*visit\s+(our|us)\b", re.I),
    re.compile(r"^\s*schedule\s+(a\s+)?test\s+drive", re.I),
    re.compile(r"^\s*financ(e|ing)\s+(available|options)\b", re.I),
    re.compile(r"^\s*disclaimer\b", re.I),
    re.compile(r"^\s*dealer\s+fee\b", re.I),
    re.compile(r"^\s*\*+\s*price\b", re.I),
    re.compile(r"^\s*\(?\d{3}[\s\-.)]{0,3}\d{3}[\s\-.]{0,3}\d{4}\s*$"),
    re.compile(r"^https?://\S+\s*$", re.I),
)


class _StripHTML(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._chunks: list[str] = []

    def handle_data(self, data: str) -> None:
        self._chunks.append(data)

    def get_text(self) -> str:
        return "".join(self._chunks)


def strip_html_to_text(raw: str) -> str:
    if not raw or not str(raw).strip():
        return ""
    s = str(raw)
    if "<" not in s or ">" not in s:
        return s
    try:
        p = _StripHTML()
        p.feed(s)
        p.close()
        return p.get_text()
    except Exception:
        return re.sub(r"<[^>]+>", " ", s)


def normalize_listing_description(raw: str) -> str:
    """Strip HTML, collapse whitespace, drop generic dealer boilerplate lines."""
    text = strip_html_to_text(raw)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t\f\v]+", " ", text)
    lines = []
    for line in text.split("\n"):
        ln = line.strip()
        if not ln:
            continue
        drop = False
        for rx in _BOILERPLATE_LINE_RES:
            if rx.search(ln):
                drop = True
                break
        if not drop:
            lines.append(ln)
    out = "\n".join(lines)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return re.sub(r" {2,}", " ", out).strip()


def _clip_evidence(s: str, max_len: int = 120) -> str:
    t = " ".join(str(s).split())
    if len(t) <= max_len:
        return t
    return t[: max_len - 1].rstrip() + "…"


def _confidence_color(hint: str | None, evidence: str | None) -> float:
    if not hint or not str(hint).strip():
        return 0.0
    if evidence and str(hint).lower() in str(evidence).lower():
        return 0.75
    return 0.55


def _extract_interior_exterior(norm: str) -> tuple[dict[str, Any], dict[str, Any]]:
    interior_hint: str | None = None
    interior_evidence: str | None = None
    exterior_hint: str | None = None
    exterior_evidence: str | None = None

    m = re.search(
        r"(?i)\binterior\s*[:\-]\s*([^\n.!]{3,80})",
        norm,
    )
    if m:
        interior_evidence = _clip_evidence(m.group(0))
        interior_hint = " ".join(m.group(1).split())[:120]

    if not interior_hint:
        m2 = re.search(
            r"(?i)\b([A-Za-z][A-Za-z\s\-]{2,42})\s+interior\b",
            norm,
        )
        if m2:
            candidate = " ".join(m2.group(1).split()).strip()
            colors = color_phrase_candidates(candidate + " " + norm)
            if colors or re.search(r"(?i)\b(leather|cloth|vinyl|suede|alcantara)\b", candidate):
                interior_evidence = _clip_evidence(m2.group(0))
                interior_hint = candidate[:120]

    if not interior_hint:
        m3 = re.search(
            r"(?i)\b(leather|premium\s+cloth|cloth|vinyl)\s+"
            r"(?:trim|upholstery|seats|interior)\b[^\n.!]{0,50}",
            norm,
        )
        if m3:
            interior_evidence = _clip_evidence(m3.group(0))
            interior_hint = " ".join(m3.group(0).split())[:120]

    mx = re.search(r"(?i)\bexterior\s*[:\-]\s*([^\n.!]{3,80})", norm)
    if mx:
        exterior_evidence = _clip_evidence(mx.group(0))
        exterior_hint = " ".join(mx.group(1).split())[:120]

    if not exterior_hint:
        m4 = re.search(
            r"(?i)\b([A-Za-z][A-Za-z\s\-]{2,40})\s+exterior\b",
            norm,
        )
        if m4:
            cand = " ".join(m4.group(1).split()).strip()
            if color_phrase_candidates(cand):
                exterior_evidence = _clip_evidence(m4.group(0))
                exterior_hint = cand[:120]

    ic = {
        "value": interior_hint,
        "confidence": _confidence_color(interior_hint, interior_evidence),
        "evidence_spans": [interior_evidence] if interior_evidence else [],
    }
    ec = {
        "value": exterior_hint,
        "confidence": _confidence_color(exterior_hint, exterior_evidence),
        "evidence_spans": [exterior_evidence] if exterior_evidence else [],
    }
    return ic, ec


def _split_package_blocks(norm: str) -> tuple[list[dict[str, Any]], list[str]]:
    """
    Identify ``… Package`` headings and bullet lines that follow.
    Also collects non-package bullet lines as standalone features.
    """
    packages: list[dict[str, Any]] = []
    standalone: list[str] = []
    lines = norm.split("\n")
    i = 0
    package_heading = re.compile(
        r"(?i)^(.{1,72}?\bpackage\b.{0,40})$",
    )
    bullet = re.compile(r"^\s*[\-*•]\s*(.+)$")

    while i < len(lines):
        line = lines[i].strip()
        hm = package_heading.match(line)
        if hm:
            name = " ".join(hm.group(1).split()).strip()
            feats: list[str] = []
            evidence: list[str] = []
            evidence.append(_clip_evidence(line))
            j = i + 1
            while j < len(lines):
                nxt = lines[j].strip()
                if not nxt:
                    j += 1
                    continue
                if package_heading.match(nxt):
                    break
                bm = bullet.match(nxt)
                if bm:
                    feat = " ".join(bm.group(1).split()).strip()
                    if feat and len(feat) > 2:
                        feats.append(feat[:200])
                        evidence.append(_clip_evidence(nxt))
                    j += 1
                    continue
                if len(nxt) < 120 and not nxt.endswith(":"):
                    feats.append(nxt[:200])
                    evidence.append(_clip_evidence(nxt))
                    j += 1
                    continue
                break
            conf = 0.45 + min(0.35, 0.05 * len(feats))
            packages.append(
                {
                    "name": name[:160],
                    "features": feats,
                    "evidence_spans": evidence[:12],
                    "confidence": min(0.95, conf),
                }
            )
            i = j
            continue

        bm = bullet.match(line)
        if bm:
            feat = " ".join(bm.group(1).split()).strip()
            if feat and len(feat) > 3 and not re.search(r"(?i)\bpackage\b", feat):
                standalone.append(feat[:200])
        i += 1

    dedup_standalone: list[str] = []
    seen: set[str] = set()
    for s in standalone:
        k = s.lower()
        if k not in seen:
            seen.add(k)
            dedup_standalone.append(s)
    return packages, dedup_standalone


_FEATURE_SECTION_INTRO = re.compile(
    r"(?i)(?:"
    r"features?\s+(?:include|includes|such\s+as|like|are|consist\s+of)"
    r"|equipped\s+with"
    r"|equipment\s+includes?"
    r"|options?\s+(?:include|includes|such\s+as|like|are)"
    r"|standard\s+features?\s+include"
    r"|highlights?\s+(?:include|includes|such\s+as|like|are)"
    r"|this\s+(?:\d{4}\s+)?(?:[\w\s\-/]+?\s+)?(?:is\s+)?(?:well\s+equipped|features|includes|offers|comes\s+with|is\s+equipped)"
    r"|well\s+equipped\s+and\s+includes?\s+these\s+features"
    r"|includes?\s+these\s+features\s+and\s+benefits"
    r"|interior\s+features?"
    r"|exterior\s+features?"
    r")"
)

_DEALER_INTRO_SENTENCE = re.compile(
    r"(?i)^this\s+\d{4}\s+.+?\b(?:well\s+equipped|includes?\s+these\s+features|features\s+and\s+benefits)\b[^.]*\.?\s*"
)

_FEATURE_KEYWORD_RE = re.compile(
    r"(?i)\b("
    r"wheel|wheels|seat|seats|sound|audio|navigation|navi|camera|cameras|sunroof|moonroof|"
    r"leather|heated|ventilated|cooled|massage|premium|surround|adaptive|blind\s+spot|"
    r"lane|park(?:ing)?|assist|burmester|mbux|harman|bose|bang\s*&?\s*olufsen|"
    r"carplay|android\s+auto|wireless|hitch|tow|lift|package|certified|panoramic|"
    r"head[-\s]?up|hud|cruise|collision|warning|monitoring|suspension|exhaust|"
    r"turbo|hybrid|electric|phev|4matic|quattro|xdrive|amg|m\s+package|"
    r"roof|liner|tonneau|step|rail|running\s+board|fender|spoiler|"
    r"display|screen|touchscreen|keyless|remote|memory|power"
    r")\b"
)

_SIZE_WHEEL_RE = re.compile(
    r"(?i)\b\d{1,2}[-\s\"']?(?:inch|in)\b"
)


def _is_plausible_feature_clause(s: str) -> bool:
    t = " ".join(str(s).split()).strip(" .;:-")
    if len(t) < 4 or len(t) > 200:
        return False
    low = t.lower()
    if re.match(r"^(call|visit|schedule|financ|contact|disclaimer|dealer)\b", low):
        return False
    if re.search(r"(?i)\b(mileage|price|msrp|stock\s*#|vin)\b", low):
        return False
    if _FEATURE_KEYWORD_RE.search(t) or _SIZE_WHEEL_RE.search(t):
        return True
    if re.search(r"(?i)\b(system|package|group|edition|technology|comfort|convenience)\b", t):
        return True
    return False


def strip_dealer_description_intro(text: str) -> str:
    """Remove marketing lead-ins like 'This 2023 … well equipped and includes these features…'."""
    # Collapse whitespace per line but keep newlines: package/bullet extraction
    # (``_split_package_blocks``) depends on line structure surviving this pass.
    t = "\n".join(
        " ".join(line.split()) for line in str(text or "").splitlines()
    ).strip()
    if not t:
        return t
    t = _DEALER_INTRO_SENTENCE.sub("", t).strip()
    t = re.sub(
        r"(?i)^(?:features?\s+and\s+benefits|equipment\s+includes?|standard\s+equipment)\s*[:\-.]?\s*",
        "",
        t,
    ).strip()
    return t


def _split_feature_clauses(text: str) -> list[str]:
    """Split comma/semicolon/and lists into individual feature clauses (respects parentheses)."""
    if not text or not str(text).strip():
        return []
    work = strip_dealer_description_intro(str(text).strip())
    work = re.sub(r"\s+", " ", work)
    work = re.sub(r"(?i)\b(?:including|such\s+as|like)\s+", "", work)
    parts: list[str] = []
    depth = 0
    buf: list[str] = []
    i = 0
    while i < len(work):
        ch = work[i]
        if ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth = max(0, depth - 1)
            buf.append(ch)
        elif depth == 0 and ch in ",;":
            part = "".join(buf).strip().strip(".")
            if part:
                parts.append(part)
            buf = []
        elif depth == 0 and work[i : i + 5].lower() == " and ":
            part = "".join(buf).strip().strip(".")
            nxt = work[i + 5 : i + 6]
            if part and nxt and (nxt.isupper() or nxt.isdigit() or nxt in "\"'("):
                parts.append(part)
                buf = []
                i += 4
            else:
                buf.append(ch)
        else:
            buf.append(ch)
        i += 1
    tail = "".join(buf).strip().strip(".")
    if tail:
        parts.append(tail)
    out: list[str] = []
    for p in parts:
        s = p.strip().strip(".")
        if _is_plausible_feature_clause(s):
            out.append(s[:200])
    return out


def _looks_like_feature_paragraph(para: str) -> bool:
    if len(para) < 40:
        return False
    commas = para.count(",")
    hits = len(_FEATURE_KEYWORD_RE.findall(para))
    return commas >= 2 and hits >= 2


def _extract_prose_features(norm: str) -> list[str]:
    """Pull equipment phrases from marketing prose (not only bullet/package blocks)."""
    out: list[str] = []
    seen: set[str] = set()

    def _add(items: list[str]) -> None:
        for raw in items:
            s = " ".join(str(raw).split()).strip()
            if not s:
                continue
            k = s.lower()
            if k in seen:
                continue
            seen.add(k)
            out.append(s[:200])

    paragraphs = re.split(r"\n\s*\n", norm)
    if len(paragraphs) <= 1 and len(norm) > 80:
        paragraphs = re.split(r"(?<=[.!?])\s+(?=[A-Z0-9\"'])", norm)

    for para in paragraphs:
        p = para.strip()
        if not p:
            continue
        m = _FEATURE_SECTION_INTRO.search(p)
        if m:
            _add(_split_feature_clauses(p[m.end() :]))
            continue
        if _looks_like_feature_paragraph(p):
            _add(_split_feature_clauses(p))

    # Inline "with X, Y, and Z" tails after equipped/includes verbs in long sentences.
    for m in re.finditer(
        r"(?i)\b(?:equipped|featuring|includes|offers|comes\s+with)\s+with\s+(.{12,220}?)(?:[.!?]|$)",
        norm,
    ):
        _add(_split_feature_clauses(m.group(1)))

    return out[:40]


def _apply_catalog_to_packages(
    packages: list[dict[str, Any]],
    *,
    make: str | None,
    year: int | None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for p in packages:
        name = str(p.get("name") or "").strip()
        feats = p.get("features") or []
        feat_list = [str(x).strip() for x in feats if str(x).strip()]
        blob = " ".join([name, *feat_list]).lower()
        hit = resolve_catalog_name(blob, make=make, year=year)
        row = dict(p)
        row["name_verbatim"] = name
        if hit:
            row["canonical_name"] = hit.canonical_name
            row["catalog_matched"] = True
            row["name"] = hit.canonical_name
            base_conf = float(row.get("confidence") or 0.5)
            row["confidence"] = min(0.95, base_conf + 0.1)
        else:
            row["canonical_name"] = None
            row["catalog_matched"] = False
            base_conf = float(row.get("confidence") or 0.5)
            row["confidence"] = min(0.85, base_conf)
        out.append(row)
    return out


def _llm_extract(norm: str, context: dict[str, Any]) -> dict[str, Any] | None:
    if (os.environ.get("LISTING_DESC_PARSE_USE_LLM") or "").strip().lower() not in (
        "1",
        "true",
        "yes",
        "on",
    ):
        return None
    api_key = (os.environ.get("ANTHROPIC_API_KEY") or "").strip()
    if not api_key:
        logger.info("ANTHROPIC_API_KEY not set; skipping LLM listing description tier")
        return None

    try:
        import requests
    except ImportError:
        logger.warning("requests not available; skipping LLM listing description tier")
        return None

    schema_hint = (
        '{"interior_color_hint":string|null,"exterior_color_hint":string|null,'
        '"packages":[{"name":string,"features":string[],"evidence_spans":string[]}],'
        '"standalone_features":string[]}'
    )
    ctx_bits = []
    for k in ("make", "model", "year", "trim", "vin"):
        v = context.get(k)
        if v is not None and str(v).strip():
            ctx_bits.append(f"{k}={str(v).strip()[:40]}")
    ctx_line = "; ".join(ctx_bits)
    prompt = (
        "Extract ONLY explicit factual claims about interior color, exterior color, "
        "and option packages from the listing text. Rules:\n"
        "- Output a single JSON object matching this shape: "
        + schema_hint
        + "\n"
        "- evidence_spans: 1–3 short verbatim snippets from the text per package (max 120 chars each).\n"
        "- Do not invent packages or features; omit unknown fields (use null or []).\n"
        "- Ignore dealer marketing fluff with no concrete equipment.\n\n"
        f"Vehicle context: {ctx_line or '(none)'}\n\nListing text:\n"
        + norm[:6000]
    )
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 512,
                "system": "You output JSON only. No prose.",
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=60,
        )
        r.raise_for_status()
        body = r.json()
        content = ((body.get("content") or [{}])[0].get("text") or "").strip()
        if content.startswith("```"):
            content = content.split("```")[1]
            if content.startswith("json"):
                content = content[4:]
        data = json.loads(content)
        if not isinstance(data, dict):
            return None
        return data
    except Exception as e:
        logger.info("LLM listing description tier skipped/failed: %s", e)
        return None


def _merge_llm_into_base(base: dict[str, Any], llm: dict[str, Any]) -> None:
    """Mutates *base* when LLM adds higher-structure packages."""
    if not llm:
        return
    for key in ("interior_color_hint", "exterior_color_hint"):
        v = llm.get(key)
        if isinstance(v, str) and v.strip():
            cur = base.get(key)
            cur_conf = 0.0
            if isinstance(cur, dict):
                cur_conf = float(cur.get("confidence") or 0)
            if cur_conf < 0.6:
                base[key] = {
                    "value": v.strip()[:120],
                    "confidence": 0.65,
                    "evidence_spans": [],
                }
    pkgs = llm.get("packages")
    if isinstance(pkgs, list) and pkgs and (not base.get("packages")):
        merged: list[dict[str, Any]] = []
        for item in pkgs:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            feats = item.get("features") if isinstance(item.get("features"), list) else []
            ev = item.get("evidence_spans") if isinstance(item.get("evidence_spans"), list) else []
            feat_clean = [str(x).strip()[:200] for x in feats if str(x).strip()]
            ev_clean = [_clip_evidence(str(x)) for x in ev if str(x).strip()][:8]
            merged.append(
                {
                    "name": name[:160],
                    "features": feat_clean,
                    "evidence_spans": ev_clean,
                    "confidence": 0.55,
                }
            )
        if merged:
            base["packages"] = merged
    sf = llm.get("standalone_features")
    if isinstance(sf, list) and sf:
        cur = list(base.get("standalone_features") or [])
        seen = {x.lower() for x in cur}
        for x in sf:
            s = str(x).strip()
            if s and s.lower() not in seen:
                cur.append(s[:200])
                seen.add(s.lower())
        base["standalone_features"] = cur


def extract_listing_description(
    description: str,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Return structured extraction including ``parser_version`` and per-field confidence.

    *context* keys: make, model, year, trim, vin (all optional).
    """
    ctx = dict(context) if context else {}
    year_int: int | None = None
    try:
        if ctx.get("year") is not None and str(ctx["year"]).strip() != "":
            year_int = int(float(str(ctx["year"])))
    except (TypeError, ValueError):
        year_int = None

    norm = normalize_listing_description(description or "")
    norm = strip_dealer_description_intro(norm) or norm
    interior, exterior = _extract_interior_exterior(norm)
    packages_raw, standalone = _split_package_blocks(norm)
    prose_features = _extract_prose_features(norm)
    if prose_features:
        seen = {s.lower() for s in standalone}
        for feat in prose_features:
            if feat.lower() not in seen:
                standalone.append(feat)
                seen.add(feat.lower())
    packages = _apply_catalog_to_packages(
        packages_raw,
        make=ctx.get("make"),
        year=year_int,
    )

    pkg_conf = 0.0
    if packages:
        pkg_conf = min(0.95, sum(float(p.get("confidence") or 0) for p in packages) / max(1, len(packages)))

    out: dict[str, Any] = {
        "parser_version": LISTING_DESCRIPTION_PARSER_VERSION,
        "parsed_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "interior_color_hint": interior,
        "exterior_color_hint": exterior,
        "packages": packages,
        "standalone_features": standalone,
        "confidence": {
            "interior": float(interior.get("confidence") or 0),
            "exterior": float(exterior.get("confidence") or 0),
            "packages": float(pkg_conf),
        },
    }

    weak = (not packages and re.search(r"(?i)\bpackage\b", norm)) or (
        packages and out["confidence"]["packages"] < 0.4
    )
    weak = weak or (not interior.get("value") and not packages and len(norm) > 80)
    sparse_features = len(standalone) + sum(len(p.get("features") or []) for p in packages) < 3
    if sparse_features and len(norm) > 120:
        weak = True

    if weak:
        llm = _llm_extract(norm, ctx)
        if llm:
            _merge_llm_into_base(out, llm)
            if out.get("packages"):
                out["packages"] = _apply_catalog_to_packages(
                    out["packages"],
                    make=ctx.get("make"),
                    year=year_int,
                )
            pk = out.get("packages") or []
            out["confidence"]["packages"] = (
                min(0.95, sum(float(p.get("confidence") or 0) for p in pk) / max(1, len(pk))) if pk else 0.0
            )

    return out


def semantic_packages_snippet(parsed: dict[str, Any], *, max_chars: int = 450) -> str:
    """Compact deduped line for embeddings — not full dealer prose."""
    parts: list[str] = []
    seen: set[str] = set()
    for p in parsed.get("packages") or []:
        if not isinstance(p, dict):
            continue
        label = str(p.get("name") or "").strip()
        if not label:
            continue
        low = label.lower()
        if low not in seen:
            seen.add(low)
            parts.append(label)
        for f in (p.get("features") or [])[:4]:
            fs = str(f).strip()
            if not fs:
                continue
            fl = fs.lower()
            if fl not in seen and len(seen) < 24:
                seen.add(fl)
                parts.append(fs)
    for s in (parsed.get("standalone_features") or [])[:6]:
        t = str(s).strip()
        if t and t.lower() not in seen:
            seen.add(t.lower())
            parts.append(t)
    text = "; ".join(parts)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rstrip() + "…"


def collect_equipment_options_from_packages(
    pj: dict[str, Any] | None,
    *,
    photo_equipment: list[str] | None = None,
    include_vision: bool = True,
) -> list[str]:
    """
    Unified buyer-facing equipment list from dealer description parse, package features,
    and optional photo-detected items.
    """
    out: list[str] = []
    seen: set[str] = set()

    def _add(label: str) -> None:
        s = str(label or "").strip()[:200]
        if not s:
            return
        k = s.lower()
        if k in seen:
            return
        seen.add(k)
        out.append(s)

    if isinstance(pj, dict):
        for x in pj.get("standalone_features_from_description") or []:
            _add(str(x))
        for entry in pj.get("packages_normalized") or []:
            if not isinstance(entry, dict):
                continue
            for feat in entry.get("features") or []:
                _add(str(feat))
            name = str(entry.get("name") or entry.get("canonical_name") or "").strip()
            if name and not entry.get("features"):
                _add(name)
        for key in ("observed_features", "possible_packages"):
            if not include_vision:
                continue
            for x in pj.get(key) or []:
                _add(str(x))
        adas = pj.get("detected_adas")
        if include_vision and isinstance(adas, list):
            try:
                from backend.vision.equipment_vision import humanize_adas_token

                for x in adas:
                    _add(humanize_adas_token(str(x)))
            except Exception:
                for x in adas:
                    _add(str(x))

    for x in photo_equipment or []:
        _add(str(x))

    return out[:56]

