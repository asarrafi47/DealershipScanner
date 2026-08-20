"""
When may a corpus statistic overrule a price printed on a photograph?

Only when the car's own sticker does not vouch for it. `reconciles_exactly` is the gate,
and it has now been caught deleting verified prices twice by two independent audits:

    881839  M Sport Package             $2,550
    888335  Rear Climate Control        $900
    884344  M Sport Package             $2,550
    881828  Convenience Package         $1,400

All four were plainly printed, all four were arithmetic-checked by the reader that
submitted them, and all four were removed by the scrub within the same command that wrote
them. The second failure pair happened because the function's own comment described a
destination-excluded branch that the code did not contain -- and since nearly every real
Monroney itemizes freight, the exemption almost never fired at all.
"""

from __future__ import annotations

import pytest

from backend.scripts.image_batch import (
    _DESTINATION_BAND,
    _is_mandatory_fee,
    reconciles_exactly,
)


def sticker(base, total, *prices):
    return {
        "base_msrp": base,
        "sticker_msrp": total,
        "priced_options": [{"name": f"opt{i}", "price": p} for i, p in enumerate(prices)],
    }


class TestDestinationFoldedIn:
    def test_exact_sum_is_exempt(self):
        assert reconciles_exactly(sticker(47_500, 57_240, 8_565, 1_175))

    def test_a_dollar_of_rounding_is_tolerated(self):
        assert reconciles_exactly(sticker(50_000, 55_000.5, 5_000))


class TestDestinationItemizedSeparately:
    """The real shape of a BMW Monroney: freight is printed, but it is not equipment, so
    readers are told to keep it out of `priced_options`. base + options can then never
    close to zero -- which is exactly why the exemption never fired."""

    def test_car_884344_m_sport_package(self):
        # base 47,500 + options 8,565 + destination 1,175 = 57,240 printed
        assert reconciles_exactly(sticker(47_500, 57_240, 8_565))

    def test_car_881828_convenience_package(self):
        # base 50,900 + options 3,665 + destination 1,175 = 55,740 printed
        assert reconciles_exactly(sticker(50_900, 55_740, 3_665))

    def test_the_band_edges_are_inclusive(self):
        lo, hi = _DESTINATION_BAND
        assert reconciles_exactly(sticker(50_000, 50_000 + lo))
        assert reconciles_exactly(sticker(50_000, 50_000 + hi))


class TestNoExemption:
    def test_a_gap_too_large_for_freight_is_not_vouched_for(self):
        """A $9,000 hole means options are missing, not that freight was itemized."""
        assert not reconciles_exactly(sticker(50_000, 59_000))

    def test_a_gap_too_small_for_freight_is_not_vouched_for(self):
        assert not reconciles_exactly(sticker(50_000, 50_400))

    def test_parts_exceeding_the_whole_are_never_exempt(self):
        """Options summing past the printed total means a price is on the wrong row --
        the one case that cannot be a real sticker."""
        assert not reconciles_exactly(sticker(50_000, 55_000, 9_000))

    def test_no_base_price_means_nothing_to_reconcile_against(self):
        assert not reconciles_exactly(sticker(None, 55_000, 4_000))
        assert not reconciles_exactly(sticker(0, 55_000, 4_000))

    def test_unparseable_prices_do_not_silently_count_as_zero(self):
        s = sticker(50_000, 55_740)
        s["priced_options"] = [{"name": "mystery", "price": "call for pricing"}]
        assert not reconciles_exactly(s)


class TestFormatting:
    def test_base_may_arrive_as_a_formatted_string(self):
        assert reconciles_exactly(sticker("$50,900", 55_740, 3_665))


class TestMandatoryFees:
    """A freight charge stored as a purchasable option poisons the price book -- 14
    "Destination Charge" observations and 8 package_values rows leaked through before this
    filter existed."""

    @pytest.mark.parametrize(
        "name",
        [
            "Destination Charge",
            "Destination and Handling",
            "DESTINATION CHARGE",
            "Freight",
            "Documentation fee",
            # Car 888263. The reader hedged its own label, and the phrase-matching version
            # of the term list let a $1,350 freight charge through as an option.
            "Destination/handling charge (printed on sticker as 'Refrigerant')",
        ],
    )
    def test_fees_are_refused(self, name):
        assert _is_mandatory_fee(name)

    @pytest.mark.parametrize(
        "name",
        [
            "Harman Kardon surround sound system",
            "Premium Package",
            "Refrigerant",                       # a real no-charge line on every BMW sticker
            '19" Individual Y-Spoke Bicolor wheels',
            "Black All Weather Floor Mats",
        ],
    )
    def test_real_options_are_kept(self, name):
        assert not _is_mandatory_fee(name)
