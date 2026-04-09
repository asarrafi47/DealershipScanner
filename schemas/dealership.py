"""Pydantic models for dealership registry (developer dashboard)."""

from __future__ import annotations

from pydantic import BaseModel, Field, HttpUrl, field_validator


class DealerCreate(BaseModel):
    """Validated payload for inserting a dealership row into inventory.db."""

    name: str = Field(..., min_length=1, max_length=500)
    website_url: HttpUrl
    city: str = Field(..., min_length=1, max_length=200)
    state: str = Field(..., min_length=2, max_length=2)

    @field_validator("name", "city")
    @classmethod
    def strip_text(cls, v: str) -> str:
        s = v.strip()
        if not s:
            raise ValueError("cannot be empty")
        return s

    @field_validator("state")
    @classmethod
    def state_usps(cls, v: str) -> str:
        s = v.strip().upper()
        if len(s) != 2 or not s.isalpha():
            raise ValueError("state must be a 2-letter code")
        return s

    def row_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "website_url": str(self.website_url).rstrip("/"),
            "city": self.city,
            "state": self.state,
        }
