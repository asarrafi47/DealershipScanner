"""Named steps of :func:`backend.db.repositories.search_repo.search_cars`.

``search_repo.search_cars`` keeps its keyword signature and import path; it packs
its arguments into a :class:`~.query.SearchQuery` and runs these steps in order:

1. :func:`.where.build_where` -- the SQL ``WHERE`` text + params, one function per
   filter family (pure; no database).
2. :class:`.post_filter.PostSqlFilters` -- the Python-side filters that run after
   the SQL (paint families, interior buckets, engine displacement).
3. :mod:`.ranking` -- the ZIP origin, then the ranked id scan (nearest first inside
   a radius, otherwise cheapest first).
4. :func:`.hydrate.hydrate_in_rank_order` -- chunked ``SELECT *`` in rank order
   until ``limit`` rows survive the filters.
5. :func:`.ordering.order_results` -- the cap and the final sort.
"""
