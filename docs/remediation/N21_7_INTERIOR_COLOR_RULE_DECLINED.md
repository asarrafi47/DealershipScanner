# N21.7 Dealer-relative interior-color rule: declined for now (D-N33)

**Decision.** D-N33 (accepted 2026-10-18, `OWNER_DECISIONS_LOG.md`): not now; revisit after N21. N21.7 ships no code. Interior color stays in `_NON_ACTIONABLE` (`backend/utils/listing_completeness.py`), so a blank interior color never makes a listing incomplete.

**What was declined.** Counting a blank interior color as a gap at a dealer that publishes it on 75% or more of its own rows (INCOMPLETE finding 2.22: about 5,100 blanks at 296 dealers on the 09-28 mirror; the brief's figure is 4,573 at 258 dealers).

**Why not now.** The blanks are mostly rows the VDP prefetch never reached (same dealers as finding 2.6; the prefetch already fetches on a blank interior color, `backend/scanner/vdp/prefetch.py`). N18 (prefetch order and budget) and N21.5/N21.6 shrink that population first; a per-dealer rule would otherwise flag cars the next scan fills anyway, and would add a dealer-dependent definition of "complete" that moves with each dealer's own feed.

**Revisit trigger.** After N21.12 (prod re-measure with the new-arrival split): if interior-color blanks outside the new-arrival window at dealers publishing 75%+ are still above 1% of their active cars, reopen D-N33 and implement the rule in `listing_completeness` with the dealer share computed per scan batch (never a stored per-dealer constant).

**Do not.** Do not special-case gardenahonda-com or highcountrytoyota-com in code: they publish no interior color and are already outside any such rule.
