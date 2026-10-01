"""Named steps of ``listings_repo._build_filter_options_uncached`` (the /listings facets).

One function per facet family, each taking an open cursor (``queries``) or the
rows those produce (``labels``, ``countries``); ``listings_repo`` keeps the
cache, the connection, and the final dict assembly.
"""
