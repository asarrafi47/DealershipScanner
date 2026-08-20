"""
Which registered store does a photographed dealer name refer to?

Every test here is a failure that actually happened. Choosing the destination is the step
that changes what a shopper sees -- a wrong answer files a car at a real dealership a
thousand miles from where it stands, and it looks like data rather than like a bug. Three
separate versions of this matcher shipped and each one moved cars to the wrong rooftop, so
the rules are pinned rather than trusted.

The registry rows below are the real ones, names verbatim.
"""

from __future__ import annotations

import pytest

from backend.scripts.apply_attribution_moves import (
    choose_destination,
    group_vocabulary,
    names_a_specific_rooftop,
    is_unusable_destination,
)

# (registry_id, name, website, latitude) -- the shape choose_destination consumes.
REGISTRY = [
    (1, "Hendrick Automotive Group", "https://www.hendrickcars.com", 35.2),
    (2, "Hendrick Porsche", "https://www.hendrickporsche.com", 35.2),
    (3, "Hendrick BMW", "https://www.hendrickbmw.com", 35.2),
    (4, "Hendrick BMW Northlake", "https://www.hendrickbmwnorthlake.com", 35.3),
    (5, "BMW of South Austin", "https://www.bmwofsouthaustin.com", 30.2),
    (6, "BMW of Austin", "https://www.bmwofaustin.com", 30.4),
    (7, "Hendrick Honda", "https://www.hendrickhonda.com", 35.2),
    (8, "Hendrick Honda Easley", "https://www.hendrickhondaeasley.com", 34.8),
    (9, "Stevenson-Hendrick Honda Wilmington", "https://www.shhondawilmington.com", 34.2),
    (10, "Stevenson-Hendrick Honda Jacksonville", "https://www.shhondajax.com", 34.7),
    (11, "Hendrick Dodge Chrysler Jeep", "https://www.hendrickdcj.com", 35.4),
    (12, "Hunter Dodge Chrysler Jeep RAM FIAT", "https://www.hunterdcjr.com", 35.6),
    (13, "Hendrick Lexus Charlotte", "https://www.hendricklexuscharlotte.com", 35.2),
    (14, "Jaguar Land Rover", "https://www.jlrnewportbeach.com", 33.6),
    (15, "Chevrolet", "", None),
    (16, "Hendrick Subaru Hoover", "https://www.hendricksubaruhoover.com", 33.4),
    (17, "Gwinnett Place Honda", "https://www.gwinnettplacehonda.com", 34.0),
    (18, "Darrell Waltrip Automotive Group", "https://www.dwauto.com", 36.0),
]

VOCAB = group_vocabulary(name for _, name, _, _ in REGISTRY)


def pick(observed: str) -> str | None:
    row, _ = choose_destination(observed, REGISTRY, VOCAB)
    return None if row is None else row[1]


def reason(observed: str) -> str:
    return choose_destination(observed, REGISTRY, VOCAB)[1]


class TestUnusableDestinations:
    def test_group_row_is_never_a_destination(self):
        """`_STOP` deletes "automotive" and "group" before any token test can see them.

        The first implementation checked the TOKENS against a set of group words, so
        "Hendrick Automotive Group" arrived as {hendrick} and looked like an ordinary store.
        It tied with every real Hendrick rooftop and blocked 40+ correct moves.
        """
        assert is_unusable_destination("Hendrick Automotive Group")
        assert is_unusable_destination("Darrell Waltrip Automotive Group")

    def test_bare_marque_row_is_never_a_destination(self):
        """Car 494553 was moved from California to Newport Beach because a registry row
        named just "Jaguar Land Rover" scored a perfect match against a plate frame reading
        "Jaguar Land Rover Charlotte"."""
        assert is_unusable_destination("Jaguar Land Rover")
        assert is_unusable_destination("Chevrolet")

    def test_real_rooftops_are_usable(self):
        assert not is_unusable_destination("Hendrick Porsche")
        assert not is_unusable_destination("Rick Hendrick Chevrolet Duluth")


class TestGroupPrefixIsOwnershipNotLocation:
    def test_parent_group_prefix_may_be_dropped(self):
        """"Hendrick" is who owns it; "BMW of South Austin" is where it is."""
        assert pick("Hendrick BMW of South Austin") == "BMW of South Austin"

    def test_group_row_does_not_block_its_own_rooftops(self):
        assert pick("Hendrick Porsche") == "Hendrick Porsche"


class TestLeftoverPlaceNameBlocks:
    """A leftover word that is NOT the parent group names a DIFFERENT ROOFTOP.

    These four all resolved to plain "Hendrick BMW"/"Hendrick Honda" in Charlotte, NC --
    cars sent 1,000+ miles from the store photographed beside them.
    """

    @pytest.mark.parametrize(
        "observed",
        [
            "Hendrick BMW of McKinney",       # McKinney, TX
            "Hendrick BMW Southpoint",        # Durham, NC
            "Hendrick BMW Mall of Georgia",   # Buford, GA
            "Hendrick Honda Hickory",         # Hickory, NC
            "Hendrick Honda Bradenton",       # Bradenton, FL
            "Rick Hendrick BMW",              # Charleston, SC
            "Stevenson-Hendrick BMW",         # Wilmington, NC
        ],
    )
    def test_unregistered_rooftop_does_not_fall_back_to_the_family_row(self, observed):
        assert pick(observed) is None


class TestAmbiguityIsRefused:
    def test_two_cities_one_name(self):
        """"Stevenson-Hendrick Honda" is a Wilmington store AND a Jacksonville store."""
        assert pick("Stevenson-Hendrick Honda") is None
        assert reason("Stevenson-Hendrick Honda") == "ambiguous"

    def test_exact_match_beats_a_longer_sibling(self):
        """"Hendrick Honda" IS a store, even though "Hendrick Honda Easley" also scores
        1.0 against it."""
        assert pick("Hendrick Honda") == "Hendrick Honda"
        assert reason("Hendrick Honda") == "exact"


class TestMarqueCounting:
    def test_the_right_family_beats_the_longer_franchise_list(self):
        """Raw token overlap prefers "Hunter Dodge Chrysler Jeep RAM FIAT" (5 marques
        shared) over "Hendrick Dodge Chrysler Jeep", which is the actual store."""
        assert pick("Hendrick Chrysler Dodge Jeep Ram Fiat") == "Hendrick Dodge Chrysler Jeep"

    def test_house_name_does_not_match_a_franchise_name(self):
        """"Hendrick Motors of Charlotte" is the group's Mercedes-Benz store. `_STOP` drops
        "motors", leaving {hendrick, charlotte} -- identical to what "Hendrick Lexus
        Charlotte" reduces to. A Mercedes was about to be filed at the Lexus store."""
        assert pick("Hendrick Motors of Charlotte") is None


class TestSpellingVariation:
    def test_hyphen_and_space_tokenise_alike(self):
        """`_tokens` strips punctuation rather than splitting on it, so the registry's
        "Stevenson-Hendrick" became one token sharing nothing with the plate frame's
        "Stevenson Hendrick"."""
        assert pick("Stevenson Hendrick Honda Wilmington") == "Stevenson-Hendrick Honda Wilmington"

    def test_parenthetical_ownership_note_is_ignored(self):
        assert pick("BMW of South Austin (Hendrick Automotive Group)") == "BMW of South Austin"

    def test_word_order_does_not_matter(self):
        assert pick("Hendrick Honda Gwinnett Place") == "Gwinnett Place Honda"


class TestGroupBrandingIsNotARooftop:
    """A `conflicting` verdict claims a photograph named a DIFFERENT store. Group branding
    cannot support that: BMW of Dallas legitimately carries AutoNation branding, and a
    HendrickCars.com plate frame is on cars at 90+ stores. 21 conflicts were recorded on
    exactly that evidence."""

    @pytest.mark.parametrize(
        "observed",
        [
            "HendrickCars.com",
            "Hendrick Cars.com",
            "Hendrick (HendrickCars.com)",
            "Hendrick Cars (HendrickCars.com)",
            "AutoNation (1Price Pre-Owned Vehicles)",
            "AutoNation (specific rooftop not named)",
            "1Price Pre-Owned Vehicles",
            "Group 1 Automotive",
            # No "Darrell Waltrip Automotive Group" row exists -- a group is usually not a
            # dealership, only its rooftops are -- so {darrell, waltrip} can never be
            # learned from the registry. The name says what it is in words; three cars
            # asserted they were at a holding company until it was read.
            "Darrell Waltrip Automotive Group",
            "Sonic Automotive Group",
        ],
    )
    def test_group_branding_names_no_rooftop(self, observed):
        assert not names_a_specific_rooftop(observed, VOCAB)

    @pytest.mark.parametrize(
        "observed",
        [
            "Hendrick Porsche",          # the group owns exactly one Porsche store
            "Hendrick BMW Northlake",
            "Hendrick Chevrolet (Naples, FL)",
            "BMW of South Austin",
            "Stevenson-Hendrick Honda Wilmington",
            # An ownership footnote in parentheses is not the store's identity. Testing the
            # raw string for group words would reject this rooftop by its own annotation.
            "BMW of South Austin (Hendrick Automotive Group)",
        ],
    )
    def test_a_marque_or_a_place_does_name_a_rooftop(self, observed):
        """The marque is the whole signal here, unlike everywhere else in this module.
        Stripping it judged "Hendrick Porsche" and "HendrickCars.com" alike and would have
        discarded 9 correct conflicts."""
        assert names_a_specific_rooftop(observed, VOCAB)
