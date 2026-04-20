"""Flatten normalized reference tables to the inventory CSV template."""

from vehicle_reference.csv_export.flat_export import CSV_COLUMNS, export_to_csv, iter_export_rows

__all__ = ["CSV_COLUMNS", "export_to_csv", "iter_export_rows"]
