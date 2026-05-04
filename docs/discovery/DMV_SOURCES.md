# DMV / state licensee data — attribution & pilots

The discovery pipeline’s **DMV tier** reads machine-readable dealer lists that **you** obtain under each jurisdiction’s terms. This repo does not redistribute proprietary state bulk files by default.

## Pilot: North Carolina (`NC`)

- **Loader:** `backend.discovery.dmv.states.nc`
- **CSV location (optional):** `data/discovery/dmv/NC/dealers.csv`, or path in env `DISCOVERY_DMV_NC_CSV`
- **Columns:** See `backend/discovery/dmv/states/generic_csv.py` for accepted header aliases (`business_name`, `street_address`, `city`, `state`, `zip_code`, optional `website`).
- **License / attribution:** Obtain current licensee data from official NC sources (e.g. public records, agency-published spreadsheets). Before publishing or redistributing derived datasets, confirm compliance with North Carolina public-records and copyright rules applicable to the specific file you use. Document the **exact download URL**, **retrieval date**, and **terms** next to your copy of the file (recommended: a short `LICENSE.txt` beside `dealers.csv`).

## Adding another state

1. Implement `load_<state>_records(project_root) -> list[DMVRecord]` under `backend/discovery/dmv/states/`.
2. Register it in `backend/discovery/dmv/registry.py` `STATE_LOADERS`.
3. Add a subsection here with **official source**, **license**, and **attribution** requirements.
