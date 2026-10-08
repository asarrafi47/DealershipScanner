# Brochure grid 'P' marker: data check (P0B.4)

- Unit: P0B.4 (owner-decisions-17) of `docs/REMEDIATION_PLAN_2026_10.md`. Feeds D-OD12 and the deferred enrichment follow-up F-1.
- Date: 2026-10-08, revised the same day after an adversarial review. The review found that the first pass's legend scan missed legends that are in the corpus (for example Ford's "P: PACKAGED"), so the per-OEM table, its totals and the proposed D-OD12 text were wrong. The legend scan was rebuilt over the whole corpus, and every legend number below comes from it (section 10). The P-cell counts were recomputed, and the simulation figures and store counts were re-derived from the stored simulation outputs and the dictionary tree; none of them changed. One new simulation was run (BMW and Kia lower-case p, section 6.2).
- Code: read at `afa063a22`, and all line numbers are at that revision. `brochure_trim_candidates.py` and `brochure_extract.py` are unchanged at the main checkout's HEAD (`e0c86c995`). The corpus is identical too: `git diff afa063a22 HEAD` over `brochure_text` is empty, and the working tree has no changes there.
- Corpus: `backend/dictionary/derived/brochure_text` (`dictionary_paths.py:21`). It holds 2,966 tracked files: 2,965 brochure JSON files plus `_index.jsonl`. 332 are lossless schema-2 captures (every page, plus word cells with x-extents in `pages[].lines`). The other 2,633 are legacy captures that kept only trim-hint pages (`text`, `table_lines` and `combined_trim_pages_text`).
- Method: read-only Python scripts in the session scratchpad, run at low priority. No network was used and no database was touched. Three passes:
  1. `pmarker.py` reads every brochure with `json.load` and searches every text source: `pages[].text`, `pages[].table_lines`, the layout rows in `pages[].lines`, and `combined_trim_pages_text`. It records the P cells and every printed legend entry for P, p or a P-numbered key (P1, P2, …). Line breaks are kept as markers, so a legend printed over two lines is still found.
  2. `brochure_trim_candidates.extract_trim_walk` is run as written and with 'P' read as standard. That covers the 675 brochures that contain any upper-case 'P' token (first pass), plus all 29 BMW books and the 2013 Kia Forte with a lower-case 'p' cell read as standard (this pass). These are pure function calls; nothing was persisted.
  3. `brochure_extract.extract_brochure_pdf` is run on the 104 local PDFs (`backend/data/brochures`, 25 MB or smaller) whose brochure_text has 'P'/'p' cells: once as written, and once with 'P' read as package. `persist_brochure_extract` was never called.
- Nothing was written except this file. The plan's D-OD12 row was not edited (another workflow is merging in the main checkout); section 8 gives the proposed text.

## Answer

- **Wherever an OEM prints a legend entry for an upper-case 'P', it means "not standard" (package, or in 3 books an option), with one exception: Volvo.** 416 brochures print a legend entry for P, p or a P-numbered key.
  - 401 mean package or "part of a package". 266 use the "P = …" / "P: …" form, such as Ford's "P: PACKAGED" and Toyota's "P = Available as part of a package". 135 are package keys: 132 Nissan books ("P Part of SV Premium Package", "P1 Part of …") and 3 Jaguar books.
  - 3 mean "available as an option" or "with options noted".
  - Only Volvo prints "P = Standard", because its check-mark glyph extracts as the letter 'P'. That happens in 5 brochures, and 3 of them also print a real "P = Package" on the same legend line.
- **A lower-case 'p' means standard in 6 BMW books (2010-2011) and package in 7 others:** Fiat 2013-2016, the 2013 Toyota Sequoia, and three 2014 Nissan keys. One Kia legend (2013 Forte) cannot be read from the text layer.
- **The two readers in the code:**
  - `brochure_trim_candidates` reads P as not standard. It matches all 404 package and option legends.
  - `brochure_extract` reads p/P as standard. It matches 2 Volvo books and the 6 BMW books (lower-case p).
  - 3 Volvo books (2017 S60, V60, XC60) give P both meanings. Neither reader can be right on those, so they should yield no grid, as the GM ●/● collision already does (`brochure_legend_is_ambiguous`, brochure_trim_candidates.py:1643).
- **No artifact that renders today changes under the per-legend reading or the package reading.** Under a global "P = standard" reading, the one rendered overlay that changes is `trim_adds_by_year/2026__toyota__corollahybrid`. Its trim order reverses, so LE becomes the top trim, and its adds change. That reading is wrong for this brochure: the grid prints "12.3-in. Digital Gauge Cluster | - | P | S", and page 8 lists that cluster inside the SE Premium Package.
- **brochure_extract's matrix path has no surviving output.** No overlay carries its `extracted_via_equipment_matrix` warning, and the one `brochure_facts` file came from the FCA block path. No Complete_Options `[Brochure]` row holds an add that exists only because P was read as standard.
  - A re-run on the local PDFs would differ under the package reading for 28 of the 44 PDFs that reach the matrix path. 220 standard-feature claims would be dropped, and adds would change for 6 trims (one in each of 6 brochures).
  - The per-legend reading gives the same result, because none of the 28 has a "standard" legend: 22 print a package legend and 6 print no P legend.
  - That path also invents trim names, so it is unsafe whichever P reading is chosen (section 7).
- **'S' needs the same rule.** 22 Nissan books print an S key that means a package ("S Part of Sport Package", "S Part of Special Edition Package", …). Both readers count 'S' as standard (section 7).
- **Recommendation for D-OD12:** read P per OEM from the brochure's own legend, and default to "not standard" when no legend is captured. See section 8.

## 1. The readings in the code

| Reader | Where | 'P' / 'p' means | Other relevant behaviour |
|---|---|---|---|
| `brochure_extract._STANDARD_MARKERS = {"p","s"}` | brochure_extract.py:135; `_marker_is_standard` :763-768 | **standard**, both cases. It lower-cases the marker and tests only its first character, so `P1`, `Pkg`, `SP`, `P,T` and `S` all count as standard. | Used by `_parse_matrix_table` (:859) and the text-line fallback (:918, which assigns marks to `ladder_order[-2:]`). It is reached only from `extract_brochure_pdf` when no FCA "STANDARD –" block was found (:1176-1184). Its input is the PDF, not brochure_text. |
| `brochure_trim_candidates._GRID_LETTER_MARKS` | brochure_trim_candidates.py:250; `_marker_is_standard` :782-785 | **not standard** (`"P": "neg"`) | Text `marker_grid` layout (:1083). Its marker regex `[SOPNA]` (:251-254) is case-sensitive, so a lower-case 'p' is not a marker there. |
| `brochure_trim_candidates._CELL_NEGATIVE_LETTERS` | :1124-1125; `_cell_mark` :1160-1178 | **not standard**, both cases (the cell text is upper-cased) | `cell_grid` and `coord_grid` layouts. `_FOOTNOTE_MARK_RE` (:1153) strips trailing digits first, so `P1`, `P13`, `P67` and `P250` are all read as 'P'. |

The second and third rows live in the same module and agree. The disagreement the audit found (enrich.md:43) is between the two modules.

## 2. Counts

- **P cells.** 267 brochures carry at least one P cell:
  - table cells (pdfplumber's own cells): 11,668 cells that are exactly `P`, plus 554 `P<digits>` cells that `_cell_mark` also reads as P;
  - layout marker rows of the 332 lossless files: 1,460 P tokens;
  - the text-grid view (what the `marker_grid` text parser sees: runs of 2-12 markers): 14,841 P tokens in 592 files. This view is noisier, but it is the only view of Volvo's glyph grids, which pdfplumber did not table-extract.
- **Lower-case p:** 1,055 table cells and 3,371 text-run tokens. BMW accounts for 729 and 724 of them.
- **Printed legends:** 416 brochures print a legend entry for P, p or a P-numbered key. By meaning:
  - package: 401.
    - 266 use the "P = …" / "P: …" / "P – …" form. 262 of them use an upper-case P. The other 4 use a lower-case p: Fiat 2013 500, 2015 500 and 2016 500X, and the 2013 Toyota Sequoia.
    - 135 are package keys with no "=". 132 are Nissan: "P Part of <package>", "P <name> Premium Package", "P1 Part of …", and glued forms such as "P1Part of SV Premium Package"; 3 of these print a lower-case p. The other 3 are Jaguar.
  - standard: 8. Volvo prints "P = Standard" in 2 books, and BMW prints "p Standard" in 6.
  - both meanings on one line: 3 (Volvo 2017 S60, V60, XC60);
  - option or available: 3 (Smart 2011-2012, Chrysler 2013 300);
  - unresolved: 1 (Kia 2013 Forte).
- **What the first pass missed.** It reported 322 brochures: 222 "P = …" books plus 100 Nissan keys. This pass finds 284 non-Nissan books and 132 Nissan books.
  - Each of the first pass's 222 books is also in this pass's set, so none of them was a false hit.
  - It missed 62 non-Nissan books: Toyota 34, Mazda 10, BMW 3, Acura 3, Ford 3, Chrysler 2, Fiat 2, Jaguar 2, Dodge 1, Mitsubishi 1, Volvo 1.
  - Its legend pattern needed one of a fixed list of meaning words directly after the letter, and it matched "Package" only as a whole word, so "P: PACKAGED" failed.
  - It read only `pages[].text` lines of 200 characters or fewer, and kept at most 40 of them per book.
  - Its Nissan figure came from a separate tally that this pass cannot reproduce.
- **P cells without a captured legend:** 61 of the 267 P-cell brochures have no P legend in the stored text. By OEM: Audi 18, Porsche 15, Toyota 6, Dodge 5, Lincoln 4, Volkswagen 3, Jaguar 3, Nissan 2, Jeep 2, GMC 1, Scion 1, Volvo 1.
  - In 47 of them P is a code or noise (section 5).
  - In the other 14, P sits in an equipment grid, but either the captured legend has no P entry or the legacy trim-hint capture dropped the legend page. For example, 2011__dodge__charger keeps 3 of its 24 pages. 2017__volvo__s90 keeps 6 of its 44, and its one P cell ("Power Sunroof with Sunshade P", p40) is probably Volvo's check glyph.
  - Of the other 206 P-cell brochures, 205 print a package legend and 1 (2014 Volvo XC70) prints "P = Standard".

## 3. Per-OEM table

How to read the table:

- "Tbl P / P#" counts table cells that are exactly `P`, and `P<digits>` cells. "Lay P" counts P tokens in layout marker rows (lossless files only).
- Each legend is quoted verbatim from the stored text, with `file#page`. The count after the quotes is the number of brochures that print such an entry, with their years.
- Sample rows quote stored table, layout or text cells, joined here with " / ".
- TC = brochure_trim_candidates (P not standard). BE = brochure_extract (P standard).

| OEM | Files | Files with P cells | Tbl P / P# | Lay P | Years with P cells | Printed legend for P (verbatim; brochures printing it) | P means | Reader that matches | Sample P row |
|---|---|---|---|---|---|---|---|---|---|
| Nissan | 250 | 84 | 356 / 494 | 36 | 2010-2027 | Package keys, not "P =": `P Part of 3.5 SV Premium Package` (2013__nissan__maxima#p15); `P Part of S Preferred Package LP Part of PRO-4X Luxury Package` (2012__nissan__frontier#p2); `Standard P1 Part of 2.5 S Premium Package (Coupe)` (2011__nissan__altima#p15); `Optional P Premium Package D Driver Assistance Package` (2017__nissan__murano#p8); glued `P1Part of SV Premium Package` (2023__nissan__rogue#p10); lower-case `p Part of SL Preferred Package` (2014__nissan__cube#p17). 132 brochures, 2010-2027; 82 of the 84 P-cell brochures print one | package code (P, P1-P4) | TC | 2011__nissan__altima#p15 `P1 / CP / S`; 2013__nissan__maxima#p15 `S / P` |
| Toyota | 278 | 28 | 490 / 0 | 358 | 2011-2027 | `S = Standard O = Optional — = Not available P = Available as part of a package` (2013__toyota__tundra#p27); `P = Feature is available as part of an option package.` (2011__toyota__prius#p17); `S = Standard O = Optional P = Available Package` (2021__toyota__tundra#p20); `O = Option P = Package S = Standard --- = Not Available` (2025__toyota__tacoma#p9); `(S=Standard; P=Package; --=Not Available)` (2026__toyota__crownsignia#p5); lower-case `p = Feature is available as part of an option package.` (2013__toyota__sequoia#p16). 126 brochures, 2011-2027 | package | TC | 2025__toyota__tacoma#p9 `LED Bed Lighting / P / P / P / S / S / S / S / S` |
| Dodge | 96 | 47 | 5,024 / 2 | 867 | 2006-2025 | `• = Included O = Optional P = Package — = Not available` (2018__dodge__challenger#p36); `• = Standard. P = Included in package noted in parentheses. O = Optional.` (2006__dodge__grandcaravan#p6); `• = Standard. 0 = Optional. P = Available with Package noted. F = Fleet only.` (2025__dodge__hornet#p2). 43 brochures, 2006-2025 | package | TC | 2021__dodge__challenger#p56 `P / P / P / P / P / — / — / — / —` |
| Ram | 46 | 27 | 4,299 / 0 | 0 | 2010-2023 | `S = Standard. 0 = Optional. P = Part of package. — = Not available.` (2015__ram__1500#p33); `• = Standard O = Optional P = Available within Package` (2017__ram__promaster#p20). 29 brochures, 2010-2023. (`P1`/`P2 Equipment Package` in the 2021-2023 Ram 1500 are package names, not keys.) | package | TC | 2021__ram__hd#p54 `DSE / P / — / • / — / — / —` |
| Chrysler | 36 | 11 | 793 / 0 | 0 | 2010-2022 | `• = Included. P = Available within Package noted. O = Optional.` (2019__chrysler__pacifica#p27): 15 brochures, 2010-2022. Plus `• = Included. P = Available with options noted. O = Optional.` (2013__chrysler__300#p28) | package (one: option) | TC | 2020__chrysler__pacifica#p33 `P / P / P / P / P / • / • / • / •` |
| Fiat | 17 | 8 | 339 / 0 | 69 | 2014-2023 | `• = Included. O = Optional. P = Available within Package noted. — = Not available.` (2021__fiat__500#p27); `S = Standard O = Optional — = Not available P = Included in package group` (2017__fiat__124#p21); lower-case `LEGEND: S = Standard, O = Optional, p = package, — = Not available` (2013__fiat__500#p74); `s = standard p = package — = not available` (2016__fiat__500x#p30). 15 brochures, 2013-2023 | package (P and p) | TC (BE also misreads Fiat's lower-case p) | 2017__fiat__500x#p13 `P / P / P` |
| Mazda | 114 | 3 | 15 / 0 | 3 | 2009-2010 | `S: Standard O: Optional P: Package option A: Dealer-installed accessory –: Not available` (2010__mazda__cx7#p21); `S: Standard P: Package O: Optional A: Dealer-available accessory – : Not available` (2010__mazda__mx5#p11); `S: Standard O: Optional P: Package T: Touring model GT: Grand Touring model` (2010__mazda__3#p15). 12 brochures, 2009-2011 | package | TC | 2010__mazda__cx7#p20 `– / P / P / S` |
| Jeep | 87 | 7 | 168 / 0 | 2 | 2014-2026 | `• = Included. P = Available within package noted. O = Optional. N/A = Not available.` (2011__jeep__compass#p12); `• = Standard O = Optional P = Package` (2014__jeep__cherokee#p14). 10 brochures, 2011-2022 | package | TC | 2014__jeep__cherokee#p14 `P / P / P` |
| **Volvo** | 74 | 3 | 28 / 0 | 21 | 2004-2017 | **`P = Standard \ue070 = Option T = Trim Level N/A = Not available`** (2014__volvo__xc70#p2; 2017__volvo__crosscountry#p3): the check glyph extracts as 'P'. **`P = Standard \ue070 = Option T = Trim Level P = Package N/A = Not available`** (2017__volvo__v60#p3; also 2017 s60#p5, xc60#p5): both meanings on one line. `(cid:2) = Standard (cid:3) = Option P = Package` (2004__volvo__v70#p30); `(cid:2) = Standard (cid:3) = Option T = Trim Level P = Package N/A = Not available` (2016__volvo__xc60#p3); `= Standard = Option T = Trim Level N/A = Not available P = Package` (2014__volvo__s60#p2); `P = Package N/A = Not available` (2016__volvo__v60#p2): the standard glyph does not extract. 9 brochures, 2004-2017 | per brochure: package 4, standard 2, both 3 | BE for 2014 XC70 / 2017 Cross Country; TC for 2004 V70 / 2014 S60 / 2016 V60 / 2016 XC60; neither for 2017 S60/V60/XC60 | 2014__volvo__xc70#p2 `Leather-clad steering wheel P P P` (396 P tokens in text runs); 2004__volvo__v70#p30 `Heated Front Seats (included in Climate Package …) P P (cid:2) P` |
| **BMW** | 29 | 0 | 0 (729 lower-case) | 0 | none | `p Standard` / `k Optional` / `o Included in Sport Package` / `s Included in M Sport Package` (lines of one table cell, 2010__bmw__1series#p29); `p Standard s Included in Technology Package` (2010__bmw__x5#p29). 6 brochures: 2010 1, 3 and 5 Series, X3, X5; 2011 1 Series. 5 of them print `s Standard equipment` on other pages | lower-case p = standard | BE; TC's `_cell_mark` reads 'p' as not standard | 2010__bmw__1series#p29 `Electronic throttle control p p` |
| Jaguar | 47 | 3 | 0 / 12 | 0 | 2018-2020 | `2 Standard O Option P Option as part of Pack – Not available` (2012__jaguar__xj#p65); package keys `P1 Cold Climate Package P2 Vision Package P3 Comfort & Convenience Package P4 Luxury Interior Upgrade Package` (2018__jaguar__xf#p31; `P1 Comfort Package …` in 2018__jaguar__xj#p30). 3 brochures, 2012 and 2018 | package | TC | 2018__jaguar__xf#p31 `Soft Door Close — — P3 2 P3 P3` (text run). The 2018-2020 P cells are engine names, not markers: 2018__jaguar__epace#p23 `POWER (HP) / P250 / P300` |
| Ford | 164 | 2 | 19 / 0 | 17 | 2022 | `S: STANDARD`, `O: OPTIONAL` and `P: PACKAGED`, each on its own line: 2022__ford__bronco#p15 prints `S: STANDARD DIMENSIONS`, `O: OPTIONAL Base/Big Bend/` and `STANDARD FEATURES P: PACKAGED Exterior (in.) Outer Banks Badlands`; also 2022__ford__superduty#p20 and 2022__ford__mustangmache#p14. 3 brochures, 2022; both P-cell brochures print it | package | TC | 2022__ford__bronco#p15 `Liftgate flood lights / P / S / S` (layout row) |
| Mitsubishi | 43 | 2 | 77 / 0 | 62 | 2011-2013 | `S = Standard O / P = Option or Package — = Not available A = Port/Dealer Accessory` (2013__mitsubishi__outlander#p26); `S = Standard P = Package – = Not Available A = Accessory Option` (2013__mitsubishi__outlandersport#p9); `S = Standard P = Factory Package - = Not available O = Option A = Accessory` (2011__mitsubishi__lancer#p19). 3 brochures, 2011-2013 | package | TC | 2013__mitsubishi__outlander#p26 `In-dash 6-disc CD/MP3 compatible changer / — / P / — / S / S` |
| Acura | 67 | 0 | 0 | 0 | none | `P = Premium Package, T = Technology Package, AS = A-Spec` (2019__acura__ilx#p17; also 2020 and 2021 ILX). 3 brochures, 2019-2021 | package | TC (BE would read the grid's `P,T` cells as standard: it tests the first character) | 2019__acura__ilx#p17 `Front Passenger's 4-Way Power Seat with Power Lumbar Support P,T` (the cells are `P,T`, so they are not counted as P cells) |
| Saab | 4 | 0 | 0 | 0 | none | `S= Standard, O = Option, NC = No Charge option, P = Package, — = Not available` (2011__saab__95#p30). 3 brochures, 2011-2012 | package | TC | (P only in text runs: 91 tokens) |
| Smart | 3 | 0 | 0 | 0 | none | `l Standard equipment P Available as an option` (2011__smart__fortwo#p42; also 2012). 2 brochures | option | TC | n/a |
| Kia | 101 | 0 | 0 | 0 | none | `! p sTanDaRD OPTiOnaL — nOT avaiLaBLE` (2013__kia__forte#p12): the glyph order cannot be recovered from the text layer. 1 brochure | unresolved | n/a | 2013__kia__forte#p12 `p p p` |
| Audi | 94 | 18 | 59 / 0 | 0 | 2012-2014 | No P entry. The legend is `■ Standard \uf0a8 Optional — N/A` (PUA glyph; 2012__audi__a3#p29) and `… SP Sport plus package …` (2013__audi__a8#p21) | not a marker: fragments of letter-split words and package codes | neither applies | 2013__audi__a8#p21 `S / P / S / P / SP` |
| Porsche | 51 | 15 | 1 / 42 | 8 | 2006-2019 | No P entry. The legend is `– not available I number/extra-cost option • standard equipment ■ available at no extra cost` (2015__porsche__911#p65) | not a marker: option numbers `P01`, `P06`, `P13` | neither applies | 2015__porsche__911#p66 `Sports bucket seats with memory package … / • / … / P01` |
| GMC | 94 | 1 | 0 / 4 | 0 | 2011 | `A-Available S-Standard —-Not Available` (2011__gmc__canyon#p14; the stored text separates the words with control characters) | not a marker: code `P67` | neither applies | 2011__gmc__canyon#p15 `— / — / P67 / — / — / P67 / —` |
| Lincoln, Volkswagen, Scion | 67 / 86 / 28 | 4 / 3 / 1 | 0 | 8 / 8 / 1 | 2009-2023 | none (VW prints `(S = standard, O = optional)`, e.g. 2022__volkswagen__atlas#p5) | noise or codes: letters of letter-spaced headlines (2022 Lincoln), unexplained `P P` cells (2015 VW GTI), option codes `P01`/`P41` (2022-2023 VW Taos) | n/a | 2015__volkswagen__golfgti#p8 `… differential lock / P P / P P / P P` (layout row) |

No other OEM in the corpus has a P table or layout cell, or a P legend entry. In 17 other OEMs, 'P' appears only as a stray letter in text runs: 276 tokens in all, led by Buick (56), Cadillac (45) and Bentley (36). Hyundai's `SEL P=Hybrid SEL Premium Trim` (2022__hyundai__santafe#p8) names a trim column, not a marker.

## 4. P cells by OEM and model year

The format is `year: brochures / P cells`. Cells are table cells, plus `P<digits>`; for lossless files the count is the larger of the table and layout counts. This pass recomputed these figures, and they are identical to the first pass.

- Dodge: 2006: 1/39, 2008: 2/154, 2009: 1/28, 2010: 8/232, 2011: 3/66, 2016: 7/798, 2017: 6/545, 2018: 6/914, 2019: 6/1152, 2020: 3/465, 2021: 3/614, 2025: 1/32
- Ram: 2010: 2/163, 2011: 1/85, 2013: 2/265, 2015: 3/536, 2016: 2/155, 2017: 2/220, 2018: 3/456, 2019: 2/282, 2020: 3/365, 2021: 3/668, 2022: 2/704, 2023: 2/400
- Nissan: 2010: 2/32, 2011: 2/105, 2012: 2/43, 2013: 9/174, 2014: 8/132, 2015: 9/149, 2016: 5/20, 2017: 3/26, 2018: 4/10, 2019: 7/18, 2020: 3/17, 2021: 4/9, 2022: 5/20, 2023: 6/14, 2024: 4/10, 2025: 4/15, 2026: 5/47, 2027: 2/13
- Chrysler: 2010: 1/8, 2015: 1/73, 2016: 1/13, 2019: 2/195, 2020: 2/202, 2021: 2/171, 2022: 2/131
- Toyota: 2011: 1/9, 2012: 2/34, 2013: 2/34, 2014: 2/11, 2015: 2/14, 2016: 3/22, 2017: 1/9, 2018: 1/7, 2025: 6/304, 2026: 6/136, 2027: 2/9
- Fiat: 2014: 1/64, 2017: 4/118, 2019: 1/59, 2022: 1/85, 2023: 1/13
- Jeep: 2014: 1/57, 2015: 3/86, 2022: 1/25, 2026: 2/2
- Mitsubishi: 2011: 1/30, 2013: 1/62
- Audi: 2012: 7/20, 2013: 10/38, 2014: 1/1
- Porsche: 2006: 1/7, 2010: 1/3, 2012: 1/3, 2013: 2/5, 2014: 3/8, 2015: 2/6, 2016: 2/4, 2017: 1/3, 2018: 1/3, 2019: 1/1
- Volvo: 2004: 1/23, 2014: 1/4, 2017: 1/1. Volvo's glyph grids sit in text runs, not table cells: 613 P tokens in 17 files.
- Ford: 2022: 2/19
- Mazda: 2009: 1/3, 2010: 2/12
- Jaguar: 2018: 1/4, 2019: 1/4, 2020: 1/4 (all `P250`/`P300` engine names)
- Volkswagen: 2015: 1/6, 2022: 1/1, 2023: 1/1
- Lincoln: 2022: 4/8
- GMC: 2011: 1/4
- Scion: 2009: 1/1

## 5. Which reading matches each legend

| Legend family | Brochures | `brochure_extract` (P = standard) | `brochure_trim_candidates` (P = not standard) |
|---|---|---|---|
| Upper-case P with "=", ":" or a dash meaning package, part of, available within, included in, available as part of a package, packaged, factory package, option or package, or "<name> Package". Toyota 125, Dodge 43, Ram 29, Chrysler 15, Fiat 12, Mazda 12, Jeep 10, Volvo 4 (2004 V70, 2014 S60, 2016 V60, 2016 XC60), Acura 3, Ford 3, Mitsubishi 3, Saab 3 | 262 | wrong | right |
| p = package, lower case (Fiat 2013 500, 2015 500, 2016 500X; Toyota 2013 Sequoia) | 4 | wrong (case-folds) | right (`_cell_mark` upper-cases) |
| Package keys with no "=". Nissan 132: "P Part of <package>", "P <name> Premium Package", "P1 Part of …", glued "P1Part of …"; 3 of them lower-case. Jaguar 3: "P Option as part of Pack", "P1 Cold Climate Package … P7 Black Package" | 135 | wrong | right (`_cell_mark` reads P1-P7 as P) |
| P = available as an option / with options noted (Smart 2011-2012, Chrysler 2013 300) | 3 | wrong | right |
| P = Standard, check glyph (Volvo 2014 XC70, 2017 Cross Country) | 2 | right | wrong |
| P = Standard and P = Package on one line (Volvo 2017 S60, V60, XC60) | 3 | ambiguous | ambiguous; should refuse the grid |
| p = Standard, lower case (BMW 2010 1, 3 and 5 Series, X3, X5; 2011 1 Series) | 6 | right | wrong in `cell_grid`/`coord_grid`; ignored by `marker_grid` |
| Kia 2013 Forte `! p` legend | 1 | unresolved | unresolved |
| **All brochures that print a P/p legend entry** | **416** | | |
| P cells, no P legend, and P is a code or noise. Codes: Audi 18, Porsche 15, Jaguar E-Pace 3, Volkswagen Taos 2 (`P01`, `P41`), GMC 1. Noise: Lincoln 4, Jeep 2026 Cherokee 1 (letter-spaced "S P E C I F I C A T I O N S"), Jeep 2026 Gladiator 1 (a stray `• P` cell), Volkswagen 2015 GTI 1, Scion 1 | 47 | wrong whenever it lands in a grid column | harmless (not standard) |
| P cells in an equipment grid, no P legend captured: Toyota 6, Dodge 5, Nissan 2 (2010 Titan `P1`/`P2` keys, 2019 Pathfinder), Volvo 2017 S90 1 | 14 | wrong wherever P is a package there, as on the 2026 Corolla Hybrid, whose page 8 lists its P items in the SE Premium Package | right under the default, except for the S90 if its P is the check glyph |

## 6. Overlays and adds whose standard-feature sets would change

### 6.1 Artifacts on disk today

| Store | Size | Which P reader produced it | Changes under D-OD12 = per legend | = package | = standard |
|---|---|---|---|---|---|
| `derived/trim_adds_by_year` with `source: brochure_text_quoted` (the only admissible overlay source: `LADDER_BULLET_STORES` brochure_extract.py:1378, gate :2131) | 27 overlays | `brochure_trim_candidates.extract_trim_walk` (layouts: cell_grid 10, adds_to_block 9, marker_grid 4, column_block 4) | 0 | 0 | **1: `2026__toyota__corollahybrid`** (cell_grid, 6 P cells). Today it reads LE → SE → XLE with adds SE 10, XLE 3; a re-run reproduces that exactly. Under P = standard it becomes XLE → SE → LE with adds SE 8, LE 4. |
| `derived/trim_adds_by_year`, every other source (brochure_llm 2,468, promoted_brochure_auto 277, brochure_llm_web 219, manual_brochure_review 123, brochure_llm_knowledge 83) | 3,170 | none: no letter-marker reader. None carries `extracted_via_equipment_matrix`; their warnings are only `models_column_matrix` 270, `dot_feature_matrix` 7, `inline_trim_bullet_rows` 5 (brochure_promote, glyph/column parsers) | 0 | 0 | 0 |
| `derived/trim_candidates` (`source: brochure_text_auto`, all `draft`) | 2,656 | the removed `_draft_adds_from_text` drafter, which reads no markers | 0 | 0 | 0 |
| `derived/brochure_facts` | 1 file (Jeep 2016 Grand Cherokee) | brochure_extract FCA block path (`warnings: []`), not the matrix path | 0 | 0 | 0 |
| `options/raw/<Make>/<year>_<Make>_<Model>_Complete_Options.csv` `[Brochure]` rows (`_write_marketing_trim_csv_rows`) | 767 of 7,996 CSVs carry them | brochure_extract when persisted | 0 | 0: none of the 44 matrix-path brochures has a CSV row holding an add that exists only under P = standard (6 of their CSVs have `[Brochure]` rows; 0 matching adds) | 0 |

### 6.2 What a re-extraction would produce

`brochure_trim_candidates.extract_trim_walk`, over the 675 brochures with any upper-case P token (as written vs P = standard):

| Brochure | Layout | Legend says | Standard sets (P = standard vs today) | Adds |
|---|---|---|---|---|
| 2006__dodge__grandcaravan | coord_grid | P = included in package | SE +6 (e.g. "Door locks — power (included with Popular Equipment Group)") | SXT −6 |
| 2026__toyota__corollacrosshybrid | cell_grid | S/O legend; P only on "PACKAGES BY GRADE" p11 | SE +3 / −3, XSE +2 / −2 (package lines such as "Cold Weather Package—includes …" become standard) | XSE +1 / −1 |
| 2026__toyota__corollahybrid | cell_grid | S/O legend; P on the SE Premium Package items | LE +2 / −2; rung order reverses | LE +4, SE +1 / −3, XLE −3 |
| 2026__toyota__crownsignia | cell_grid | `(S=Standard; P=Package; --=Not Available)` | Limited +2 / −2 | Limited +6 |

The other 671 brochures do not change. Most P-heavy books, such as the FCA buyer's guides, are read by `adds_to_block`, and grid output is used only when no adds block was found (brochure_trim_candidates.py:2123, :2135).

The four that change print a package legend (2006 Grand Caravan, 2026 Crown Signia) or no P legend (2026 Corolla Hybrid, Corolla Cross Hybrid), so the per-legend reading changes none of them. The five Volvo books whose legend gives P a standard meaning are all among the 675, and none of them changes. For lower-case p, `sim_lower_p.py` reran the walk on all 29 BMW books and the 2013 Kia Forte, with a cell that is exactly 'p' read as standard. None of the 30 yields a grid under either reading, so none changes.

`brochure_extract.extract_brochure_pdf`, over the 104 local PDFs with P/p cells (529 s of wall time in total). 44 reach the equipment-matrix path. Under P = package, 28 of them change, all as removals: 220 standard-feature claims dropped and 0 added. The per-legend reading gives the same 28 changes, because 22 of them print a package legend and 6 print no P legend. Adds change for:

- 2017 Fiat 500X: Touring −1;
- 2021 Nissan Altima: "Pp11" −1;
- 2023 Mazda3: "Fwd" −1;
- 2026 Toyota Crown Signia: Limited −5;
- 2025 and 2026 Toyota Tacoma: SR5 −1 each.

The largest standard-set changes are:

- Dodge 2021 Challenger: Sport and Limited −17 each;
- 2021 Durango and 2025 Hornet: −13 per trim;
- 2017 Fiat 500X: −10 per trim;
- Toyota 2027 Land Cruiser: Sport −8.

These outputs are not on disk anywhere (6.1).

## 7. Side findings (for F-1, not decided here)

- **'S' is not always standard.**
  - 22 Nissan brochures print an S key of the form "S Part of <package>". 17 use an upper-case `S`. 4 use a lower-case `s` (the 2010, 2011 and 2014 370Z, and the 2011 Juke). 1 uses `S1`/`S2` (2017 Sentra).
  - The packages are the Sport Package (2010, 2011, 2012 and 2014 370Z; 2011 and 2012 Juke), a trim's Sport Package (2011 and 2012 Altima; 2013 and 2014 Maxima), the Sport Value Package (2014 and 2015 Altima; 2014-2017 Versa; 2014 Versa Note), a Style Package (2016 and 2017 Sentra), a Special Edition (2013 Rogue) and the Split Bench Seat Package (2025 and 2026 Armada).
  - Examples: `S Part of Special Edition Package` (2013__nissan__rogue#p15); `S Part of Split Bench Seat Package (Platinum, Platinum Reserve)` (2025__nissan__armada#p8).
  - Nissan prints standard as a glyph. `_cell_mark` upper-cases a cell and strips its digits, so `s`, `S1` and `S2` all read as 'S', and both readers count 'S' as standard.
  - BMW's 2010-2011 "p Standard" key also uses a lower-case `s` for a package: "Included in M Sport Package" (2010 1, 3 and 5 Series; 2011 1 Series) or "Included in Technology Package" (2010 X5). That is 5 of the 6 books.
  - The Nissan count covers only the explicit "Part of" wording. It leaves out S as a trim name inside other keys, as in "P Part of S Preferred Package".
  - The per-legend rule should cover S as well as P.
- **Footnote stripping turns codes into markers.** `_cell_mark` strips trailing digits, so Porsche option numbers (`P01`, `P13`), GM codes (`P67`), Jaguar engine names (`P250`, `P300`), and the package keys of Nissan (`P1`-`P4`) and Jaguar (`P1`-`P7`) all read as 'P'. Today that is harmless, because P is not standard. A global "P = standard" reading would turn every such column into standard equipment.
- **The brochure_extract matrix path invents trims.**
  - Its text-line fallback assigns columns to `ladder_order[-2:]`. That gives "R/T Scat Pack" and "Daytona Scat Pack" for a 2006 Grand Caravan, 2008 Caliber and 2008 Dakota, and "Touring"/"XLE" for the Fiat 500X and 124 Spider.
  - It also accepts doubled-glyph headers as trim names ("Pp11", "Ssvv", "Pprroo--44Xx®®").
  - `persist_brochure_extract` writes `trim_adds_by_year/<key>.json` unconditionally (brochure_extract.py:1258-1259). A `process_brochure_queue.py` run without `--dry-run` would therefore replace rendered `brochure_text_quoted` overlays with source-less overlays that the provenance gate refuses, which would blank those rungs.
  - 11 of the 27 rendered overlays belong to the 44 matrix-path PDFs, among them the 2020 Charger and Durango, the 2021 Challenger and Durango, and the 2019 Titan XD.
- **BMW's lower-case 'p'** is read as not standard by `cell_grid`/`coord_grid`. No current output depends on it: none of the 29 BMW books yields a grid under either reading.

## 8. D-OD12: proposed update (not applied; the plan file is owned by the integration step)

Proposed row text for Section 7 of the plan:

> D-OD12 | Brochure grid 'P' marker: standard or package? | per OEM from each brochure's printed legend / package / standard | **Per legend, default not-standard (P0B.4, 2026-10-08).** 416 brochures print a P/p legend entry: 401 mean package or "part of package" (266 as "P = …"/"P: …", e.g. Ford "P: PACKAGED"; 135 as Nissan/Jaguar keys, e.g. "P Part of SV Premium Package", "P1 Cold Climate Package"); 3 mean "available as an option". Only Volvo prints "P = Standard" (a check glyph extracting as 'P'): 2014 XC70, 2017 Cross Country. 2017 S60/V60/XC60 print both meanings and must yield no grid, as the GM ●/● rule does. Lower-case 'p' = Standard in 6 BMW books (2010-2011) and package in 7 (Fiat, 2013 Sequoia, three 2014 Nissan keys); the 2013 Kia Forte legend is unreadable. With no captured legend (61 of 267 P-cell books), P is not standard; in Audi, Porsche, GMC, Jaguar E-Pace and VW Taos books it is a code, not a marker. 'S' needs the same rule: 22 Nissan books print "S Part of <package>" (17 as 'S', 4 as 's', 1 as 'S1'/'S2'), for Sport, Sport Value, Style, Special Edition and Split Bench Seat packages; BMW 2010-2011 uses 's' for "Included in M Sport/Technology Package". Rendered overlays that change: 0 under this option; 1 (2026 Corolla Hybrid) under "standard". The `brochure_extract` matrix reader (P = standard) has no surviving output; retire it or align it in F-1 | none |

## 9. Accept check

| Accept item | Status |
|---|---|
| A per-OEM table with legend text, counts and cited sample files | Done: sections 3 and 4, with section 2 for totals and section 5 for the legend families. |
| D-OD12 updated | Proposed text in section 8. The plan was not edited, because this unit may write only this file while another workflow merges in the main checkout. The integration step applies the row. |
| Nothing else written | Only this file. Scratch scripts and outputs live in the session scratchpad (first pass: `p0b4/`; this pass: `p0b4fix/`). The first pass's worktree `/private/tmp/claude-501/phase0/p0b4_wt` was removed. This pass created no worktree: it exported the code with `git archive afa063a22` into the scratchpad. `git status` was the same before and after. |

## 10. Reproducing

The code is exported with `git archive afa063a22 <backend code dirs> | tar -x -C <scratch>/code`; the first pass used a detached worktree at the same commit. Then run:

1. `pmarker.py <brochure_text dir> pm.jsonl`. It records the P cells (same definitions as the first pass's `scan_p.py`/`analyze.py`) and every legend entry for P, p and P-numbered keys. The legend patterns cover:
   - "P = X", "P: X" and "P\n= X" (one line break allowed around "=" or ":");
   - "P – X" and "P | X" on one line;
   - bare "P X" on one line, kept only when legend words ("Standard", "Optional", "Not available", "No-cost") or another key appear within 100 characters. Nissan "P Part of …" and upper-case "P … Premium … Package" keys need no such context. A bare "P <name> Package" is also dropped when it reads as a grid row label;
   - glued Nissan keys such as "P1Part of …" and "P2SR Premium Package".

   Here X is a package, option or standard meaning.
2. `aggregate.py pm.jsonl sim.jsonl`. It builds every total, the per-OEM and per-year tables, the legend classes, the S keys, and the join with the first pass's walk simulation. Its manual corrections are listed at the top of the script, and each was checked against the stored text:
   - It drops 9 entries that are not legends: the Ram 1500 package names "P1/P2 Equipment Package", "P<digit> Standard" (a glyph legend word next to a key), body text in the 2011 Altima, and two grid rows (2010 MX-5, 2017 Fiat 500X). Each of those books keeps its real legend.
   - It adds 1: the 2011 Rogue's case-inverted "p pREMIUM pACKAGE (sV)".
   - It overrides 1: Kia 2013 Forte becomes unresolved.
3. `details.py`, `diff_first_pass.py`, `extract_join.py`, `verify_sec6.py` and `verify_61.py`. They give the letter-case split, the books without a legend, the comparison with the first pass, the legend class of the 28 changed PDFs, the section 6.2 figures from the stored simulation outputs, and the section 6.1 store counts.
4. `legend_scan.py` + `recall.py`, a recall check. It runs a broad scan for any standalone P/p followed by a meaning word.
   - Its first run surfaced the bare "P … Premium … Package" keys (2012 Rogue; 2023 and 2024 GT-R), which led to the rule in step 1.
   - On the final scan, it fires in 10 books where `pmarker.py` recorded no legend, and all 10 were read by hand. They are grid rows (2008 Caliber, 2018 Fiat 500, 2025 and 2026 Corolla Cross, 2026 Corolla Cross Hybrid), a package table (2012 Tundra), letter-split text (2015 Grand Caravan), a heading (2017 Volvo S90, "P STANDARD FEATURES"), Acura's `P-AWS` (2019 TLX), and the case-inverted 2011 Rogue key (added in step 2).
   - Separate sweeps for keys glued to their names, for "P … Part of" keys broken across lines, and for such keys in books with no legend found the 4 glued Nissan books (2023 Rogue, 2026 Frontier, Kicks and Sentra), now covered in step 1, and nothing else.
5. First pass, unchanged:
   - `PYTHONPATH=<code> DICTIONARY_CATALOG_DB_PATH=<copy of index/dictionary_catalog.db> sim_flip.py <brochure_text dir> scan.jsonl sim.jsonl` runs `extract_trim_walk` with P read as written and as standard.
   - `sim_extract.py out.jsonl @pdf_list.txt` runs `extract_brochure_pdf` as written. If the matrix path ran, it runs again with `_STANDARD_MARKERS={"s"}`.
   - `csv_check.py` looks for Complete_Options `[Brochure]` rows that hold P-only adds.
6. `PYTHONPATH=<code> DICTIONARY_ROOT=<repo>/backend/dictionary DICTIONARY_CATALOG_DB_PATH=<catalog copy> python3 -B sim_lower_p.py <brochure_text dir> out.jsonl <29 BMW stems> 2013__kia__forte` runs `extract_trim_walk` with a cell that is exactly 'p' read as standard. A control run on 2026__toyota__corollahybrid (cell_grid) and 2021__dodge__challenger (adds_to_block) shows that the exported code does produce grids.

Every script only reads. The catalog DB was copied so that nothing opened the checkout's file, and `-B` kept bytecode out of every tree. The scripts were not kept in the repo.
