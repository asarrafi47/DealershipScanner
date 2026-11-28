"""Every outside party that receives data about a site visitor, in one place.

The Privacy Policy (``frontend/templates/privacy.html``) renders its "Who receives your data" list from
:data:`RECIPIENTS`, and ``backend/tests/test_data_recipients.py`` checks two things against the code:

* every outbound host or SDK found in the web app's request path and browser pages maps to a recipient
  here (or to :data:`NOT_USER_DATA`, with the reason it carries nothing about a visitor), and
* every host and SDK listed here still appears in the code, so the list does not keep stale names.

When code starts sending visitor data somewhere new, add the recipient here; the policy page follows.
Each sentence must say what the code does, no more. Text in [BRACKETS] is a placeholder for the owner.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Recipient:
    name: str
    purpose: str
    data: str
    when: str
    # Host names the code contacts (server side) or the browser loads from; matched by the test.
    hosts: tuple[str, ...] = ()
    # Python modules whose import means data goes to this recipient (SDK clients, smtplib).
    modules: tuple[str, ...] = ()


RECIPIENTS: tuple[Recipient, ...] = (
    Recipient(
        name="Railway",
        purpose="hosts the site, its databases and its logs.",
        data="Everything the site stores and every request it serves.",
        when="Always.",
    ),
    Recipient(
        name="Cloudflare",
        purpose="the network that delivers the site, and the Turnstile check against automated sign-ups.",
        data=(
            "Your network address and the requests your browser makes to the site. On pages with the "
            "Turnstile check (phone sign-in and dealership sign-up), the check reads signals from your "
            "browser, and our server sends Cloudflare your network address with the check's result token."
        ),
        when="Every visit; Turnstile only on the pages that show it.",
        hosts=("challenges.cloudflare.com",),
    ),
    Recipient(
        name="Anthropic",
        purpose="the model behind the car assistant, the compare assistant and the sidebar assistant.",
        data=(
            "The messages you type to an assistant, with the listing and dealership details needed to "
            "answer and any web search text it found. We do not send your name, email address, phone "
            "number or location with them."
        ),
        when="When you send a message to an assistant.",
        modules=("anthropic",),
    ),
    Recipient(
        name="DuckDuckGo",
        purpose="web search for the car, compare and sidebar assistants.",
        data=(
            "A search query built from the listing's year, make, model and trim (in the compare assistant, "
            "also the other cars' year, make and model), plus a fixed topic phrase or, when your question "
            "matches none, up to the first six words of your message. Our server sends it, so DuckDuckGo sees our address, "
            "not yours. The assistant may then open pages from the results; those sites get the page "
            "address from our server and nothing about you."
        ),
        when=(
            "When your question needs a web look-up. By default in production only for signed-in users."
        ),
        hosts=("html.duckduckgo.com",),
    ),
    Recipient(
        name="Google",
        purpose=(
            "finding dealerships near a place (Google Places), and Sign in with Google if you choose it."
        ),
        data=(
            "For the dealership finder: the centre point of your search, which is the location your "
            "browser shares if you allow it, or the centre of the ZIP code or city you typed. Our server "
            "sends it, so Google does not get your network address with it. For Sign in with Google: "
            "Google tells us your Google account id, your email address and whether it is verified, and "
            "your name and profile picture address; we keep the account id and the email address."
        ),
        when="When you search for dealerships near you; when you sign in with Google.",
        hosts=(
            "places.googleapis.com",
            "accounts.google.com",
            "oauth2.googleapis.com",
            "openidconnect.googleapis.com",
        ),
    ),
    Recipient(
        name="Apple",
        purpose="Sign in with Apple if you choose it.",
        data=(
            "Apple tells us your Apple account id and email address (which can be an Apple relay address), "
            "and can send your name the first time; we keep the account id and the email address."
        ),
        when="When you sign in with Apple.",
        hosts=("appleid.apple.com",),
    ),
    Recipient(
        name="Stripe",
        purpose="payments for paid plans.",
        data=(
            "Your email address, our id for your account (and your dealership's, for a dealership plan) "
            "and any promotion code you use. You enter card details on Stripe's own checkout page; we "
            "never see them."
        ),
        when="Only if paid plans are turned on and you start a checkout.",
        modules=("stripe",),
    ),
    Recipient(
        name="Resend",
        purpose="sends account emails such as email confirmation, password reset and change notices.",
        data="Your email address and the text of the email.",
        when=(
            "When we send you an account email. [OWNER: name the provider here if mail goes through "
            "another SMTP provider instead of Resend.]"
        ),
        modules=("resend", "smtplib"),
    ),
    Recipient(
        name="Twilio",
        purpose="sends and checks sign-in codes by text message, and looks up a number's line type.",
        data="Your phone number, the code you type and the language of the message.",
        when="Only when you sign in with a phone number.",
        modules=("twilio",),
    ),
    Recipient(
        name="NHTSA",
        purpose="the U.S. government vehicle database we use to look up recalls.",
        data=(
            "A VIN you type on the recall look-up page, and the make, model and year it decodes to. Our "
            "server sends them, so NHTSA does not get your network address."
        ),
        when="When you look up recalls by VIN.",
        hosts=("api.nhtsa.gov", "vpic.nhtsa.dot.gov"),
    ),
    Recipient(
        name="OpenStreetMap, CARTO and Esri",
        purpose="the map pictures (tiles) on the dealership finder, dealership and car pages.",
        data=(
            "Your browser loads the tiles straight from them, so they get your network address, browser "
            "details and the map area shown, which can be near your location."
        ),
        when="When a page shows a map.",
        hosts=("tile.openstreetmap.org", "basemaps.cartocdn.com", "server.arcgisonline.com"),
    ),
    Recipient(
        name="Dealership websites and their image hosts",
        purpose="the photos of the cars listed, and window-sticker images.",
        data=(
            "Your browser loads listing photos straight from the dealership's website or image service, "
            "so it gets your network address, browser details and our site's address."
        ),
        when="When a page shows listing photos.",
    ),
    Recipient(
        name="Carmaker window-sticker sites",
        purpose=(
            "the original window sticker of a car (Ford, Lincoln, GM and Stellantis brand sites, and "
            "window-sticker services)."
        ),
        data=(
            "Our server asks them for a car's sticker by the listing's VIN, which carries nothing about "
            "you. If you open the sticker link, your browser goes to that site with the VIN in the address."
        ),
        when="When a car page offers its window sticker.",
        hosts=(
            "windowsticker.forddirect.com",
            "windowsticker.lincolnvehicles.com",
            "cws.gm.com",
            "gmwindowsticker.com",
            "djapi.autoipacket.com",
            "webicon.autoipacket.com",
        ),
    ),
    Recipient(
        name="Sites you open from a link",
        purpose=(
            "Carfax (vehicle history), NHTSA (recalls), Google Maps, Apple Maps and Waze (directions)."
        ),
        data=(
            "Only if you click the link: the address of the page you open, which can include the car's "
            "VIN or the dealership's location."
        ),
        when="When you click one of those links.",
        hosts=(
            "www.carfax.com",
            "www.nhtsa.gov",
            "www.google.com",
            "maps.google.com",
            "maps.apple.com",
            "waze.com",
        ),
    ),
)


# Hosts found in the scanned code that never receive data about a visitor, with the reason.
NOT_USER_DATA: dict[str, str] = {
    "www.openstreetmap.org": "attribution link on maps",
    "carto.com": "attribution link on maps",
    "www.cloudflare.com": "source comment for the Cloudflare address list",
    "api.cloudflare.com": "source comment for the Cloudflare address list",
    "search.brave.com": "browser search mode, used only by an offline script, never by the assistants",
    "www.motortrend.com": "guide pages used only by offline research scripts",
    "www.caranddriver.com": "guide pages used only by offline research scripts",
    "www.carwow.co.uk": "guide pages used only by offline research scripts",
    "www.motorpoint.co.uk": "guide pages used only by offline research scripts",
    "fueleconomy.gov": "catalog data download, no visitor data",
    "kmac-vault-production.up.railway.app": "secret store for the server, no visitor data",
    "kmac-vault.railway.internal": "secret store for the server, no visitor data",
    "host.docker.internal": "local development secret store",
    "www.longotoyota.com": "example in a docstring",
    "www.w3.org": "SVG namespace in a stylesheet",
    "www.example-dealer.com": "placeholder text in an operator form",
    "www.example-bmw.com": "placeholder text in an operator form",
    "www.other-dealer.com": "placeholder text in an operator form",
}


def recipient_names() -> tuple[str, ...]:
    return tuple(r.name for r in RECIPIENTS)
