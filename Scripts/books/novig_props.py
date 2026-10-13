"""Novig weekly NFL player props: an exchange, read as the price a taker can get.

Ported from ``NHL-Data/src/nhl/sources/novig.py`` -- the client, the NOVIG-V3 signer,
the websocket snapshot and :func:`take` are that module's, trimmed to props. See it
for the API's shape; the parts that matter here:

* **Every rung is its own two-way market.** ``"Jake Ferguson 30.5 RECEIVING_YARDS"``
  with outcomes ``Over 30.5`` / ``Under 30.5``, one market per strike -- probed
  2026-10-13, 580 receiving-yard markets across week 6's 14 games, on every game
  rather than the half that FanDuel and DraftKings had posted by Tuesday.
* **Every order buys**, so the price to take a side is read off the *opposite*
  outcome's resting bids, walked to :data:`MIN_STAKE` of exposure (:func:`take`). A
  side that cannot fill it is omitted rather than quoted at a price nobody can take.
* **No fee pregame** (``fee.charged == "WHEN_LIVE"``), so the taker's two prices sum
  to one plus the bid-ask spread, and the proportional de-vig removes exactly that.

How the rungs become :data:`~Scripts.books.weekly_props.RAW_SCHEMA` rows: the rung
whose two sides are most balanced is the player's **line** (``OverUnder``), and the
over side of every rung is a **ladder** step at ``k = floor(strike) + 1``
(``Values``) -- which is what gives the derivation a dispersion. ``TOUCHDOWNS`` is
the anytime ladder: strike 0.5 is "1+", 1.5 is "2+".

**Two routes.** Public (``/v3/public/...``) is throttled to ~2 requests a second and
needs one request per market's book, so a full slate is ~15 minutes and is cut off
at :data:`TIME_BUDGET_S`, most-used stats first. With ``NOVIG_KEY_ID`` and
``NOVIG_PEM`` (a path to the key) set, one websocket ``snapshot`` returns every book
in seconds; a refused key falls back to public.
"""

import base64
import hashlib
import json
import logging
import math
import os
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote, unquote

import polars as pl
import requests

from Scripts.books.weekly_props import game_lookup, raw_frame

logger = logging.getLogger(__name__)

BOOK = "Novig"
DIR = "Novig"
PREFIX = "NV"
HOST = os.getenv("NOVIG_HOST", "https://api.novig.com")
LEAGUE = "NFL"
SCHEME = "NOVIG-V3"
UNRESERVED = "-._~"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"

#: Dollars of exposure a prop price must fill, as in NHL-Data.
MIN_STAKE = 50.0
PUBLIC_PAUSE = 1 / 1.8
SIGNED_PAUSE = 1 / 14.0
TIME_BUDGET_S = 900.0
PAGE_LIMIT = 100
WS_TIMEOUT_S = 30.0
MAX_WS_MARKETS = 8000
BOOK_DEPTH = 20
PREGAME = "OPEN_PREGAME"

#: ``marketType`` -> ESPN stat, in fetch priority (what fantasy scoring weighs most).
PROP_TYPES: Dict[str, str] = {
    "TOUCHDOWNS": "anytimeTouchdown",
    "RECEIVING_YARDS": "receivingYards",
    "RUSHING_YARDS": "rushingYards",
    "PASSING_YARDS": "passingYards",
    "RECEPTIONS": "receivingReceptions",
    "PASSING_TOUCHDOWNS": "passingTouchdowns",
    "INTERCEPTIONS_THROWN": "passingInterceptions",
    "RUSHING_ATTEMPTS": "rushingAttempts",
    "PASSING_COMPLETIONS": "passingCompletions",
    "PASSING_ATTEMPTS": "passingAttempts",
}
PRIORITY = list(PROP_TYPES)


# --- signing (NOVIG-V3), as in NHL-Data -----------------------------------------------

def canonical_query(raw: str) -> str:
    """The query re-encoded, pairs sorted bytewise by name then value."""
    if not raw:
        return ""
    pairs = []
    for token in raw.split("&"):
        name, _, value = token.partition("=")
        pairs.append((quote(unquote(name), safe=UNRESERVED), quote(unquote(value), safe=UNRESERVED)))
    return "&".join(f"{n}={v}" for n, v in sorted(pairs, key=lambda p: (p[0].encode(), p[1].encode())))


def string_to_sign(timestamp_ms: int, method: str, path: str, query: str = "", body: bytes = b"") -> str:
    """The six-line NOVIG-V3 canonical string."""
    return "\n".join([SCHEME, str(timestamp_ms), method.upper(), path, canonical_query(query),
                      hashlib.sha256(body).hexdigest()])


class Signer:
    """Signs NOVIG-V3 strings with an Ed25519 or P-256 PKCS#8 private key."""

    def __init__(self, key_id: str, pem: bytes):
        from cryptography.hazmat.primitives.serialization import load_pem_private_key
        self.key_id = key_id
        self._key = load_pem_private_key(pem, None)

    def sign(self, text: str) -> str:
        from cryptography.hazmat.primitives.asymmetric import ec, ed25519
        from cryptography.hazmat.primitives.hashes import SHA256
        if isinstance(self._key, ed25519.Ed25519PrivateKey):
            raw = self._key.sign(text.encode())
        elif isinstance(self._key, ec.EllipticCurvePrivateKey):
            raw = self._key.sign(text.encode(), ec.ECDSA(SHA256()))
        else:
            raise ValueError("Novig keys are Ed25519 or P-256")
        return base64.b64encode(raw).decode()

    def headers(self, method: str, path: str, query: str = "") -> Dict[str, str]:
        ts = int(time.time() * 1000)
        return {"Novig-Key-Id": self.key_id, "Novig-Timestamp": str(ts),
                "Novig-Signature": self.sign(string_to_sign(ts, method, path, query))}


def make_signer() -> Optional[Signer]:
    """A signer from ``NOVIG_KEY_ID`` and ``NOVIG_PEM`` (a file path), or None."""
    key_id, pem_path = os.getenv("NOVIG_KEY_ID"), os.getenv("NOVIG_PEM")
    if not key_id or not pem_path:
        return None
    try:
        with open(os.path.expanduser(pem_path), "rb") as fh:
            return Signer(key_id, fh.read())
    except (OSError, ValueError) as exc:
        logger.warning("[novig] key not usable, reading the public routes: %s", exc)
        return None


class NovigError(RuntimeError):
    """The exchange refused or could not be reached."""


class Client:
    """GETs against the catalog: signed when a signer is given, public otherwise."""

    def __init__(self, signer: Optional[Signer] = None):
        self.signer = signer
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA, "Accept": "application/json"})

    @property
    def pause(self) -> float:
        return SIGNED_PAUSE if self.signer else PUBLIC_PAUSE

    def get_json(self, path: str, params: Optional[dict] = None, retries: int = 4) -> Any:
        if not self.signer:
            path = path.replace("/v3/", "/v3/public/", 1)
        query = "&".join(f"{quote(str(k), safe=UNRESERVED)}={quote(str(v), safe=UNRESERVED)}"
                         for k, v in (params or {}).items() if v is not None)
        url = HOST + path + (f"?{query}" if query else "")
        for attempt in range(retries + 1):
            time.sleep(self.pause)
            headers = self.signer.headers("GET", path, query) if self.signer else {}
            try:
                resp = self.session.get(url, headers=headers, timeout=(5, 20))
            except requests.RequestException as exc:
                if attempt == retries:
                    raise NovigError(f"GET {path} failed ({exc})") from exc
                time.sleep(2.0 ** attempt)
                continue
            if resp.status_code == 429 and attempt < retries:
                time.sleep(float(resp.headers.get("Retry-After") or 1.0) * (attempt + 1))
                continue
            if resp.status_code in (401, 403, 423, 429, 451):
                raise NovigError(f"GET {path} refused with {resp.status_code}: {resp.text[:200]}")
            if resp.status_code >= 500 and attempt < retries:
                time.sleep(2.0 ** attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        raise NovigError(f"GET {path} gave up after {retries} retries")

    def pages(self, path: str, params: dict) -> List[dict]:
        items, after = [], None
        while True:
            page = self.get_json(path, {**params, "limit": PAGE_LIMIT, "after": after}) or {}
            items.extend(page.get("items") or [])
            after = page.get("next")
            if not after or not page.get("items"):
                return items


def snapshot_books(client: Client, markets: List[dict]) -> Dict[str, Any]:
    """Every book of ``markets``' events from websocket ``snapshot`` requests (signed only)."""
    import websocket

    if client.signer is None:
        raise NovigError("the websocket needs a key")
    counts: Dict[str, int] = {}
    for m in markets:
        counts[m["eventId"]] = counts.get(m["eventId"], 0) + 1
    chunks: List[List[str]] = [[]]
    size = 0
    for event_id, n in counts.items():
        if chunks[-1] and size + n > MAX_WS_MARKETS:
            chunks.append([])
            size = 0
        chunks[-1].append(event_id)
        size += n
    url = HOST.replace("https://", "wss://", 1) + "/v3/ws"
    headers = [f"{k}: {v}" for k, v in client.signer.headers("GET", "/v3/ws").items()]
    ws = websocket.create_connection(url, header=headers, timeout=WS_TIMEOUT_S)
    books: Dict[str, Any] = {}
    try:
        for nonce, chunk in enumerate([c for c in chunks if c], start=1):
            ws.send(json.dumps({"nonce": nonce, "snapshot": {"events": {e: "book" for e in chunk}}}))
            deadline = time.monotonic() + WS_TIMEOUT_S
            reply: dict = {}
            while time.monotonic() < deadline:
                message = ws.recv()
                reply = json.loads(message.decode() if isinstance(message, bytes) else message or "{}")
                if reply.get("nonce") == nonce or "code" in reply or "error" in reply:
                    break
            if "snapshot" not in reply:
                raise NovigError(f"snapshot refused: {json.dumps(reply)[:200]}")
            books.update({mid: entry["book"] for mid, entry in reply["snapshot"].items()
                          if isinstance(entry, dict) and entry.get("book")})
    finally:
        ws.close()
    return books


def fetch(sched: pl.DataFrame) -> dict:
    """This week's pregame NFL events, their prop markets, and as many books as fit."""
    client = Client(make_signer())
    now = datetime.now(timezone.utc)
    last = max(sched["officialDate"].to_list())
    window = {"league": LEAGUE, "startsAfter": int(now.timestamp() * 1000),
              "startsBefore": int((datetime.combine(last, datetime.min.time(), timezone.utc)
                                   + timedelta(days=2)).timestamp() * 1000)}
    try:
        events = [e for e in client.pages("/v3/catalog/events", window) if e.get("status") == PREGAME]
    except NovigError as exc:
        if client.signer is None:
            raise
        logger.warning("[novig] signed read refused, using the public routes: %s", exc)
        client.signer = None
        events = [e for e in client.pages("/v3/catalog/events", window) if e.get("status") == PREGAME]
    ids = {e["eventId"] for e in events}
    markets = [m for m in client.pages("/v3/catalog/markets", window)
               if m.get("eventId") in ids and m.get("status") == "OPEN"
               and m.get("marketType") in PROP_TYPES]
    books: Dict[str, Any] = {}
    if client.signer and markets:
        try:
            books = snapshot_books(client, markets)
            print(f"  novig: websocket snapshot, {len(books)} of {len(markets)} books")
        except Exception as exc:  # any refused or failed upgrade means "use REST"
            logger.warning("[novig] websocket snapshot failed, reading books over REST: %s", exc)
    deadline = time.monotonic() + TIME_BUDGET_S
    todo = sorted((m for m in markets if m["marketId"] not in books),
                  key=lambda m: (PRIORITY.index(m["marketType"]), int(m.get("startsTs") or 0)))
    for market in todo:
        if time.monotonic() > deadline:
            print(f"  novig: time budget spent, {len(markets) - len(books)} books left unread")
            break
        try:
            books[market["marketId"]] = client.get_json(
                f"/v3/catalog/markets/{market['marketId']}/book", {"depth": BOOK_DEPTH})
        except NovigError as exc:
            logger.warning("[novig] book requests stopped after %d: %s", len(books), exc)
            break
        except requests.HTTPError:
            continue  # closed since the catalog read
    print(f"  novig: {len(events)} events, {len(markets)} prop markets, {len(books)} books "
          f"({'signed' if client.signer else 'public'})")
    return {"events": events, "markets": markets, "books": books}


# --- pricing ---------------------------------------------------------------------------

def take(bids: Iterable[dict], min_stake: float = MIN_STAKE) -> Optional[float]:
    """The probability a taker pays for ``min_stake`` of exposure against the opposite bids.

    A bid at ``q`` on one outcome lets a taker buy the other at ``1 - q``. Bids are
    walked best first until the target fills -- ``min_stake`` on an underdog, enough
    to win it on a favourite. None when the book cannot fill it.
    """
    levels = sorted(((float(b["price"]), float(b.get("qty") or 0)) for b in bids or []
                     if b and b.get("price") is not None), reverse=True)
    levels = [(q, n) for q, n in levels if 0.0 < q < 1.0 and n > 0]
    if not levels:
        return None
    depth = sum(n * (1.0 - q) for q, n in levels) / 100.0
    best = 1.0 - levels[0][0]
    target = min_stake * max(1.0, best / (1.0 - best))
    if depth < target:
        return None
    staked = payout = 0.0
    for q, n in levels:
        cost = 1.0 - q
        stake = min(n * cost / 100.0, target - staked)
        if stake <= 0:
            break
        staked += stake
        payout += stake / cost
    return staked / payout


def player_name(market: dict) -> str:
    """``"Jake Ferguson 30.5 RECEIVING_YARDS"`` -> ``"Jake Ferguson"``."""
    text = str(market.get("description") or "")
    return re.sub(rf"\s+(?:[-+]?\d+(?:\.\d+)?\s+)?{re.escape(str(market.get('marketType')))}\s*$",
                  "", text).strip()


def _sides(market: dict) -> Optional[tuple]:
    by = {str(o.get("name") or "").split(" ")[0].lower(): o["outcomeId"] for o in market.get("outcomes") or []}
    return (by["over"], by["under"]) if "over" in by and "under" in by else None


def normalize(payload: dict, sched: pl.DataFrame) -> pl.DataFrame:
    """One pull to :data:`~Scripts.books.weekly_props.RAW_SCHEMA` rows. No network."""
    find = game_lookup(sched)
    games = {}
    for event in payload.get("events") or []:
        away, sep, home = str(event.get("description") or "").partition(" @ ")
        game_id = find(away, home) if sep else None
        if game_id:
            games[event["eventId"]] = game_id

    # (game, player, stat) -> [(strike, p_over, p_under)]
    rungs: Dict[tuple, List[tuple]] = {}
    for market in payload.get("markets") or []:
        game_id = games.get(market.get("eventId"))
        book = (payload.get("books") or {}).get(market.get("marketId"))
        sides = _sides(market)
        if not game_id or not book or not sides or market.get("strike") is None:
            continue
        orders = book.get("orders") or {}
        over, under = sides
        p_over = take(orders.get(under) or [])
        p_under = take(orders.get(over) or [])
        if p_over is None:
            continue
        key = (game_id, player_name(market), PROP_TYPES[market["marketType"]])
        rungs.setdefault(key, []).append((float(market["strike"]), p_over, p_under))

    rows: List[dict] = []
    for (game_id, name, stat), ladder in rungs.items():
        base = {"book": BOOK, "NFL_game_id": game_id, "player_name": name, "espn_stat": stat}
        for strike, p_over, _ in ladder:
            rows.append({**base, "value": float(math.floor(strike) + 1), "type": "Over",
                         "odds": 1.0 / p_over, "impProb": p_over, "prop_source": "Values"})
        paired = [r for r in ladder if r[2] is not None]
        if stat != "anytimeTouchdown" and paired:
            strike, p_over, p_under = min(paired, key=lambda r: abs(r[1] / (r[1] + r[2]) - 0.5))
            for side, p in (("Over", p_over), ("Under", p_under)):
                rows.append({**base, "value": strike, "type": side, "odds": 1.0 / p,
                             "impProb": p, "prop_source": "OverUnder"})
    return raw_frame(rows)
