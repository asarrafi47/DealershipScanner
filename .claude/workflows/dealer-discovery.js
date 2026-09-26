export const meta = {
  name: 'dealer-discovery',
  description: 'Investigate dealers the HTTP-only pipeline could not scan, learn how each site serves its inventory, fix by platform, re-verify',
  whenToUse: 'After backend/scripts/dealer_pipeline.py produced a triage.json with needs_discovery / thin dealers',
  phases: [
    { title: 'Investigate', detail: 'one probing agent per failed dealer: platform, endpoints, pagination, fields' },
    { title: 'Fix', detail: 'one builder per platform group: recipe / synth template / parser change with tests' },
    { title: 'Verify', detail: 're-run the pipeline on the group, judge the result' },
  ],
}

// args: { triage: "<path to triage.json>", dealers?: [ids], maxFixes?: number }
const TRIAGE_PATH = args && args.triage
const ONLY = (args && args.dealers) || null
const MAX_FIXES = (args && args.maxFixes) || 4

const FINDING = {
  type: 'object',
  properties: {
    dealer_id: { type: 'string' },
    reachable: { type: 'boolean' },
    platform: { type: 'string', description: 'recipe_synth fingerprint name, or a new name if unknown' },
    inventory_endpoint: { type: 'string', description: 'URL or URL template that returns the whole lot over HTTP, empty if none found' },
    method: { type: 'string' },
    pagination: { type: 'string', description: 'how pages advance: param name and shape, or none' },
    total_seen: { type: 'integer' },
    fields_present: { type: 'array', items: { type: 'string' } },
    fields_missing: { type: 'array', items: { type: 'string' } },
    needs_browser: { type: 'boolean', description: 'true only when no HTTP path to the inventory exists' },
    matches_existing_template: { type: 'boolean' },
    proposed_fix: { type: 'string', description: 'one paragraph: what code or recipe change makes this dealer scan HTTP-only' },
    evidence: { type: 'string', description: 'the exact probe requests and what came back, short' },
  },
  required: ['dealer_id', 'reachable', 'platform', 'needs_browser', 'proposed_fix'],
}

const FIX = {
  type: 'object',
  properties: {
    platform: { type: 'string' },
    changed_files: { type: 'array', items: { type: 'string' } },
    tests_run: { type: 'string' },
    tests_passed: { type: 'boolean' },
    recipes_written: { type: 'array', items: { type: 'string' } },
    summary: { type: 'string' },
    blocked: { type: 'string', description: 'why nothing could be changed, empty when a change landed' },
  },
  required: ['platform', 'tests_passed', 'summary'],
}

const VERDICT = {
  type: 'object',
  properties: {
    platform: { type: 'string' },
    dealers_ok: { type: 'array', items: { type: 'string' } },
    dealers_still_failing: { type: 'array', items: { type: 'string' } },
    triage_table: { type: 'string' },
    notes: { type: 'string' },
  },
  required: ['platform', 'dealers_ok', 'dealers_still_failing'],
}

const PLAYBOOK = `
The goal and the loop are in docs/NETWORK_SCAN_PROCESS.md: read it first. No headless browsers, ever.
Every probe you make, what came back, and your verdict go into workspace/dealer_logs/<dealer_id>/discovery.md (append a dated section).
For an "inaccurate" verdict, read workspace/dealer_logs/<dealer_id>/scan_runs.md: it lists the incomplete fields and the
dictionary-vs-dealer discrepancies with example VINs; decide per code whether the dealer feed, our parser, or the dictionary is wrong.
Playbook (how this project learns a dealer site over HTTP, from workspace/scan_lab/lab_20260923_http/NOTES.md):
1. Fetch the homepage and an inventory page with curl_cffi (impersonate="chrome"); plain requests gets 403 on Dealer Inspire.
   Use: from backend.scanner.recipe_synth import fetch_dealer_html, fingerprint_platform, synthesize_recipes, validate_recipe
2. fingerprint_platform(html, url) names the platform. Existing templates: dealer_dot_com (ws-inv-data), carscommerce
   (Dealer Inspire search API, POST with x-api-key, page/perPage, drop boolean facetFilters), dealer_on_cosmos
   (/api/vhcliaa/vehicle-pages/cosmos/srp/vehicles/{dealerId}/{pageId}?pt=N&pn=96, one pageId per SRP section found
   in /searchnew.aspx and /searchused.aspx page config), typesense (multi_search, host+key+collection in page JS),
   team_velocity (/inventory-new.json, /inventory-used.json ?page=N), dealer_eprocess, motive, overfuel, nabthat,
   chapman, jazel, dealermasters. Read backend/scanner/recipe_synth.py for each template's markers.
3. If synthesize_recipes returns candidates, validate_recipe(candidate, url, dealer_id, name) counts VINs over HTTP.
4. If nothing synthesizes: look in the HTML for JSON-LD Vehicle blocks, inline JSON (window.*, data-* attributes),
   script src hosts that look like inventory APIs (algolia, typesense, carscommerce, dealeron, /api/), and try
   obvious inventory paths (/inventory/, /new-vehicles/, /used-vehicles/, /searchused.aspx). Probe pagination by
   comparing VIN sets across candidate page params (page, pt, pg, offset, start). Note the total count field.
5. Never run a browser. Never write to the DB. Keep every probe under 30 requests, 1 request per second on
   Dealer Inspire hosts. Report exactly what you saw.
`

function key(f) { return (f.platform || 'unknown').toLowerCase() }

phase('Investigate')
const targets = await agent(
  `Read ${TRIAGE_PATH} and return the dealer ids under "needs_discovery", "thin" and "inaccurate"` +
  (ONLY ? ` restricted to this list: ${JSON.stringify(ONLY)}` : '') +
  `. Also return each dealer's url and name from dealers.json (a JSON list of {name,url,provider,dealer_id}; grep for the dealer_id, do not load the whole file into context). Return {dealers:[{dealer_id,url,name,verdict,reason}]}.`,
  { label: 'read-triage', effort: 'low', schema: { type: 'object', properties: { dealers: { type: 'array', items: { type: 'object', properties: { dealer_id: { type: 'string' }, url: { type: 'string' }, name: { type: 'string' }, verdict: { type: 'string' }, reason: { type: 'string' } }, required: ['dealer_id', 'url'] } } }, required: ['dealers'] } },
)
const dealers = (targets && targets.dealers) || []
log(`${dealers.length} dealer(s) to investigate`)
if (!dealers.length) return { investigated: 0 }

const findings = (await pipeline(
  dealers,
  d => agent(
    `Investigate how ${d.name || d.dealer_id} (${d.url}, dealer_id ${d.dealer_id}) serves its inventory so it can be scanned HTTP-only. ` +
    `The pipeline verdict was "${d.verdict}": ${d.reason || ''}.\n${PLAYBOOK}\nReturn a finding.`,
    { label: `probe:${d.dealer_id}`, phase: 'Investigate', schema: FINDING },
  ),
)).filter(Boolean)

const groups = {}
for (const f of findings) (groups[key(f)] = groups[key(f)] || []).push(f)
const order = Object.keys(groups).sort((a, b) => groups[b].length - groups[a].length)
log(`platforms: ${order.map(p => `${p}(${groups[p].length})`).join(', ')}`)
const browserOnly = findings.filter(f => f.needs_browser).map(f => f.dealer_id)
if (browserOnly.length) log(`needs a browser discovery pass on the mini: ${browserOnly.join(', ')}`)

const fixable = order.filter(p => groups[p].some(f => !f.needs_browser)).slice(0, MAX_FIXES)
if (order.length > fixable.length) log(`fixing ${fixable.length} of ${order.length} platform group(s) this run (maxFixes=${MAX_FIXES})`)

phase('Fix')
const results = await pipeline(
  fixable,
  p => agent(
    `Platform group "${p}". Findings from the investigators (JSON): ${JSON.stringify(groups[p])}\n` +
    `Make these dealers scan HTTP-only. Prefer, in order: (a) a recipe file under workspace/recipes/<dealer>.json written by ` +
    `backend/scripts/synthesize_recipes.py --dealer-id X --url U --name N --force; (b) a fix to the platform template in ` +
    `backend/scanner/recipe_synth.py or its parser under backend/parsers/; (c) a new PlatformTemplate when the platform is new and the ` +
    `finding gives a working endpoint + pagination. Every code change needs a unit test in backend/tests/ and ` +
    `.venv/bin/ruff check + .venv/bin/pytest -q on the touched test files must pass. Do not touch the DB, do not run a browser, ` +
    `do not commit. Append what you changed and why to workspace/dealer_logs/<dealer_id>/summary.md for each dealer in the group ` +
    `(issue -> evidence -> fix, in detail) and one line per lesson to workspace/dealer_logs/_learning/platform_playbook.md. Keep every shell command under 90 seconds (wrap network probes in a Python script with explicit timeouts; never run scanner.py or dealer_pipeline.py here, the verifier does that). Return as soon as tests pass. If the finding says needs_browser, do nothing and explain in "blocked".`,
    { label: `fix:${p}`, phase: 'Fix', schema: FIX },
  ),
  (fix, p) => fix && !fix.blocked ? agent(
    `Verify platform group "${p}". Run: .venv/bin/python -m backend.scripts.dealer_pipeline --dealers ` +
    `${groups[p].map(f => f.dealer_id).join(',')} --out workspace/pipeline/verify_${p} --batch ${Math.min(4, groups[p].length)} --scan-timeout 900 ` +
    `(run it with nohup in the background writing to workspace/pipeline/verify_${p}.out and poll that file every 30 s; it can take 10 minutes) ` +
    `and read workspace/pipeline/verify_${p}/triage.json. A dealer is ok when its verdict is "ok". Return the triage table verbatim in triage_table. ` +
    `Fix summary you are checking: ${fix.summary}. The pipeline appends scan_runs.md and writes scan_instructions.md for a verified run; ` +
    `if a dealer is still not ok, append the reason to its discovery.md.`,
    { label: `verify:${p}`, phase: 'Verify', schema: VERDICT },
  ) : { platform: p, dealers_ok: [], dealers_still_failing: groups[p].map(f => f.dealer_id), notes: fix ? fix.blocked : 'fix agent failed' },
)

return {
  investigated: findings.length,
  platforms: order.map(p => ({ platform: p, dealers: groups[p].map(f => f.dealer_id), needs_browser: groups[p].every(f => f.needs_browser) })),
  browser_only: browserOnly,
  fixes: results.filter(Boolean),
  findings,
}
