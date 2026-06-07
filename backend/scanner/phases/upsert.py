"""Async wrapper for scanner inventory upserts."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backend.scanner.inventory_write import InventoryWriteCoordinator


async def upsert_vehicles_for_dealer(
    coordinator: InventoryWriteCoordinator,
    vehicles: list[dict],
) -> int:
    return await coordinator.upsert_vehicles(vehicles)
