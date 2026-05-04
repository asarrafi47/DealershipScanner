"""
Normalized U.S.-market vehicle reference data (SQLite) with CSV export.

Layout
------
``core/`` — paths and DB helpers  
``schema/`` — SQL DDL  
``ingestion/`` — seeds, manifests, CSV/JSON file ingest  
``csv_export/`` — flattened inventory-template CSV  
``utils/`` — MPG formatting helpers  
``quality/`` — validation and QA markdown reports  
``sources/`` — EPA and other upstream clients  
``parsers/`` — ordering-guide column conventions  
``seeds/`` — BMW bootstrap JSON  
CLI entrypoint: ``python -m vehicle_reference.cli``.
"""
