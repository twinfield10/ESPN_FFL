"""Render the "connect your league" guide -- HTML and a PDF to hand someone.

Adding a league that belongs to somebody else needs six things out of their ESPN
account, and four of them are not obvious: the league id hides in a query string,
the two cookies live three clicks into DevTools, and team identity here is the
*manager* name rather than the team name (see ``app/auth.py``). Asking for that
over text costs a round trip every time. This renders it once, illustrated, as a
PDF that can be sent to anyone.

The DevTools panels are drawn, not screenshotted. Nothing here can be captured
from somebody else's account without logging into it, and a drawing can dim the
forty rows that do not matter and ring the one that does. Every value shown is
invented -- see ``EXAMPLE_SWID`` -- so the guide is safe to forward.

Two details in here are load-bearing and both are easy to get wrong by hand:

* ``SWID`` keeps its braces. ``espn_api``'s ``base_league`` puts the value into
  the cookie jar verbatim, nothing strips or re-adds them, and every league in
  ``config.yaml`` stores it braced.
* ``espn_s2`` must stay URL-encoded, which means the cookie preview pane's
  *Show URL-decoded* checkbox has to stay unchecked. See ``config.example.yaml``.

Unlike the reports in ``docs/``, this page loads no webfonts -- it has to render
the same on a machine with no network -- and it carries real print rules, so a
panel is never split from the step that explains it.

Usage::

    python -m Scripts.guide_credentials
    python -m Scripts.guide_credentials --out /tmp/preview.html --no-pdf
"""

import argparse
import html
from pathlib import Path
from typing import List, Optional

from Scripts.paths import REPO_ROOT

#: Where the rendered guide lands. The PDF sits beside it under the same stem.
OUTPUT_PATH = REPO_ROOT / "docs" / "guides" / "connect-your-espn-league.html"

#: Invented, and deliberately unlike anything real: a valid-shaped GUID that is
#: not any SWID in config.yaml. Never paste a live value in here.
EXAMPLE_SWID = "{A1B2C3D4-5E6F-7890-ABCD-EF1234567890}"
EXAMPLE_LEAGUE_ID = "123456789"
EXAMPLE_S2 = ("AEBq7mXk%2FvR3TnGxLd0pYzW8sCu1JhNfA6QeDbMi5rKoP9t")

#: Menlo's advance width at 1em, used to place highlight rects behind runs of
#: monospace text without measuring glyphs at render time.
MONO_ADVANCE = 0.6021

MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"
SANS = ('-apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, '
        "sans-serif")

STYLE = f"""
:root {{
  --ink: #1f2328;
  --muted: #5b6470;
  --line: #d8dde3;
  --mark: #b3261e;
  --mark-soft: #fdecea;
  --warn-bg: #fff8e6;
  --warn-line: #e8c96a;
  --paper: #ffffff;
}}
* {{ box-sizing: border-box; }}
body {{
  font-family: {SANS};
  color: var(--ink);
  background: var(--paper);
  margin: 0 auto;
  padding: 28px 24px 48px;
  max-width: 760px;
  font-size: 15px;
  line-height: 1.55;
  -webkit-print-color-adjust: exact;
  print-color-adjust: exact;
}}
h1 {{ font-size: 30px; line-height: 1.15; margin: 0 0 6px; letter-spacing: -0.01em; }}
h2 {{
  font-size: 18px;
  margin: 0 0 10px;
  letter-spacing: -0.005em;
  break-after: avoid;
  page-break-after: avoid;
}}
p {{ margin: 0 0 12px; }}
.lede {{ font-size: 16px; color: var(--muted); margin: 0 0 22px; }}
code {{
  font-family: {MONO};
  font-size: 0.92em;
  background: #f3f5f7;
  border: 1px solid var(--line);
  border-radius: 4px;
  padding: 1px 5px;
}}
.step {{
  break-inside: avoid;
  page-break-inside: avoid;
  margin: 0 0 26px;
  padding: 0 0 0 46px;
  position: relative;
}}
.step .n {{
  position: absolute;
  left: 0;
  top: -2px;
  width: 32px;
  height: 32px;
  border-radius: 50%;
  background: var(--ink);
  color: #fff;
  font-size: 16px;
  font-weight: 700;
  display: flex;
  align-items: center;
  justify-content: center;
}}
figure {{ margin: 12px 0 0; break-inside: avoid; page-break-inside: avoid; }}
figure svg {{ width: 100%; height: auto; display: block; }}
figcaption {{ font-size: 13px; color: var(--muted); margin-top: 6px; }}
.box {{
  border: 1px solid var(--line);
  border-radius: 8px;
  padding: 14px 16px;
  margin: 0 0 22px;
  break-inside: avoid;
  page-break-inside: avoid;
}}
.box h3 {{ font-size: 15px; margin: 0 0 8px; }}
.box p:last-child, .box ul:last-child {{ margin-bottom: 0; }}
.warn {{ background: var(--warn-bg); border-color: var(--warn-line); }}
.flag {{
  color: var(--mark);
  font-weight: 600;
}}
ul {{ margin: 0 0 12px; padding-left: 20px; }}
li {{ margin: 0 0 5px; }}
.form {{
  border: 2px solid var(--ink);
  border-radius: 8px;
  padding: 16px 18px;
  font-family: {MONO};
  font-size: 13px;
  line-height: 2.5;
  break-inside: avoid;
  page-break-inside: avoid;
}}
.form .row {{ border-bottom: 1px dotted #9aa4b0; display: block; }}
.form .label {{ color: var(--muted); }}
.sig {{ font-size: 13px; color: var(--muted); border-top: 1px solid var(--line);
        margin-top: 28px; padding-top: 12px; }}
@page {{
  size: Letter;
  margin: 14mm 15mm;
}}
@media print {{
  body {{ padding: 0; max-width: none; font-size: 12.5px; }}
  h1 {{ font-size: 26px; }}
}}
"""


# --------------------------------------------------------------------------- #
# Drawing helpers
#
# ``Scripts.lab.report.esc`` is the same one-liner, but importing it drags the
# experiment registry and the usage features into a static guide, so this uses
# ``html.escape`` directly.
# --------------------------------------------------------------------------- #

def esc(value) -> str:
    """HTML-escape a value, so a drawn label never breaks the markup."""
    return html.escape(str(value))


def mono_w(text: str, size: float) -> float:
    """Width of ``text`` set in the monospace stack at ``size`` px.

    Args:
        text: The unescaped string that will be drawn.
        size: Font size in user units.

    Returns:
        float: Advance width, for placing a highlight behind a run of text.
    """
    return len(text) * size * MONO_ADVANCE


def svg_text(x: float, y: float, text: str, *, size: float = 12,
             fill: str = "#3c4043", family: str = SANS, weight: str = "normal",
             anchor: str = "start", fit: bool = False) -> str:
    """One run of text inside a panel.

    Args:
        fit: Pin the run to the width ``mono_w`` predicts. Which monospace face
            actually resolves varies by machine, so anything drawn *around* a
            run -- a ring on a brace, a highlight behind the league id -- would
            otherwise drift by a character. Pinning makes the arithmetic exact
            by construction rather than by luck.
    """
    length = (f' textLength="{mono_w(text, size)}" lengthAdjust="spacing"'
              if fit else "")
    return (f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" '
            f'fill="{fill}" font-weight="{weight}" text-anchor="{anchor}"'
            f'{length}>{esc(text)}</text>')


def callout(x1: float, y1: float, x2: float, y2: float, label: str, *,
            anchor: str = "middle", label_x: Optional[float] = None,
            label_y: Optional[float] = None, lines: Optional[List[str]] = None
            ) -> str:
    """An arrow from (x1, y1) to (x2, y2) with a label at the tail.

    The tail is where the reader's eye starts, so the label sits there and the
    head lands on the thing being named.
    """
    parts = [f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
             f'stroke="var(--mark)" stroke-width="1.8" marker-end="url(#arw)"/>']
    lx = x1 if label_x is None else label_x
    ly = (y1 + 16) if label_y is None else label_y
    for i, line in enumerate(lines or [label]):
        parts.append(svg_text(lx, ly + i * 14, line, size=11.5,
                              fill="var(--mark)", weight="600", anchor=anchor))
    return "".join(parts)


def panel(view_w: int, view_h: int, body: str) -> str:
    """Wrap panel drawing in a sized, responsive ``<svg>``."""
    return (f'<svg viewBox="0 0 {view_w} {view_h}" role="img" '
            f'xmlns="http://www.w3.org/2000/svg">{body}</svg>')


def window_chrome(w: int, title: str) -> str:
    """The rounded browser/panel frame: traffic lights and a tab."""
    return (
        f'<rect x="1" y="1" width="{w - 2}" height="{146}" rx="10" fill="#fff" '
        f'stroke="#d8dde3"/>'
        f'<path d="M1,11 a10,10 0 0 1 10,-10 h{w - 22} a10,10 0 0 1 10,10 v23 '
        f'h-{w - 2} z" fill="#dfe1e5"/>'
        '<circle cx="20" cy="18" r="5" fill="#ff5f57"/>'
        '<circle cx="38" cy="18" r="5" fill="#febc2e"/>'
        '<circle cx="56" cy="18" r="5" fill="#28c840"/>'
        '<rect x="80" y="7" width="210" height="27" rx="6" fill="#fff"/>'
        + svg_text(94, 25, title, size=11, fill="#5b6470")
    )


def dim_rows(x: float, y: float, w: float, count: int, gap: int = 14) -> str:
    """Grey filler bars, so a panel reads as part of a busy screen."""
    return "".join(
        f'<rect x="{x}" y="{y + i * gap}" width="{w - (i % 3) * 40}" '
        f'height="6" rx="3" fill="#e8eaed"/>' for i in range(count))


# --------------------------------------------------------------------------- #
# The panels
# --------------------------------------------------------------------------- #

def panel_url() -> str:
    """The league page, with the id ringed inside the address bar."""
    size = 12.5
    head, mid, tail = ("fantasy.espn.com/football/league?",
                       f"leagueId={EXAMPLE_LEAGUE_ID}", "&seasonId=2026")
    x0 = 52.0
    x1 = x0 + mono_w(head, size)
    x2 = x1 + mono_w(mid, size)
    body = (
        window_chrome(720, "My Fantasy League")
        + '<rect x="1" y="35" width="718" height="45" fill="#f1f3f4"/>'
        + '<rect x="20" y="46" width="680" height="26" rx="13" fill="#fff" '
          'stroke="#dadce0"/>'
        # padlock
        + '<rect x="32" y="56" width="10" height="8" rx="1.5" fill="#5b6470"/>'
        + '<path d="M34.5,56 v-2.5 a2.5,2.5 0 0 1 5,0 V56" fill="none" '
          'stroke="#5b6470" stroke-width="1.4"/>'
        + f'<rect x="{x1 - 2.5}" y="50" width="{mono_w(mid, size) + 5}" '
          'height="18" rx="3" fill="#ffe8a3"/>'
        + svg_text(x0, 63, head, size=size, family=MONO, fill="#5b6470",
                   fit=True)
        + svg_text(x1, 63, mid, size=size, family=MONO, fill="#1f2328",
                   weight="700", fit=True)
        + svg_text(x2, 63, tail, size=size, family=MONO, fill="#5b6470",
                   fit=True)
        + dim_rows(40, 96, 200, 2)
        + '<rect x="470" y="90" width="210" height="28" rx="6" fill="#f3f5f7"/>'
        + callout(x1 + 60, 124, x1 + 60, 76,
                  "", label_x=x1 + 60, label_y=142,
                  lines=["this number is your league ID —",
                         "easiest is to send me the whole address"])
    )
    return panel(720, 166, body)


def panel_inspect() -> str:
    """Right-click anywhere, then Inspect."""
    items = [("Back", False), ("Forward", False), ("Reload", False),
             ("Save page as…", False), ("Print…", False),
             ("View page source", False), ("Inspect", True)]
    y = 58
    parts = ['<rect x="1" y="1" width="718" height="263" rx="8" fill="#fafbfc" '
             'stroke="#d8dde3"/>',
             dim_rows(40, 30, 260, 5),
             # cursor
             '<path d="M150,44 l0,20 l5,-5 l4,9 l4,-2 l-4,-8 l7,0 z" '
             'fill="#1f2328" stroke="#fff" stroke-width="1"/>',
             '<rect x="174" y="44" width="232" height="215" rx="8" '
             'fill="#00000010"/>',
             '<rect x="170" y="40" width="232" height="215" rx="8" fill="#fff" '
             'stroke="#cfd4da"/>']
    for label, is_target in items:
        if label == "Save page as…" or label == "View page source":
            parts.append(f'<line x1="178" y1="{y - 12}" x2="394" '
                         f'y2="{y - 12}" stroke="#e4e7eb"/>')
        if is_target:
            parts.append(f'<rect x="172" y="{y - 15}" width="228" height="22" '
                         'fill="#e8f0fe"/>')
        parts.append(svg_text(190, y, label, size=12.5,
                              fill="#1f2328" if is_target else "#3c4043",
                              weight="700" if is_target else "normal"))
        y += 25 if label not in ("Reload", "Print…") else 37
    parts.append(callout(540, 226, 412, 226, "", label_x=552, label_y=222,
                         anchor="start",
                         lines=["the one at the bottom", "of the menu"]))
    return panel(720, 265, "".join(parts))


def panel_tabs() -> str:
    """The DevTools tab bar, with Application selected."""
    tabs = [("Elements", 62), ("Console", 136), ("Sources", 206),
            ("Network", 276), ("Performance", 346), ("Memory", 440),
            ("Application", 512)]
    parts = ['<rect x="1" y="1" width="718" height="131" rx="8" fill="#fff" '
             'stroke="#d8dde3"/>',
             '<rect x="1" y="1" width="718" height="35" fill="#f1f3f4"/>',
             '<line x1="1" y1="36" x2="719" y2="36" stroke="#dadce0"/>',
             '<path d="M20,14 l0,12 l3,-3 l2.5,5 l2.5,-1 l-2.5,-5 l4,0 z" '
             'fill="#5b6470"/>',
             '<rect x="36" y="13" width="9" height="12" rx="1.5" fill="none" '
             'stroke="#5b6470" stroke-width="1.4"/>',
             '<rect x="504" y="4" width="94" height="28" rx="4" '
             'fill="#e8f0fe"/>',
             '<rect x="504" y="32" width="94" height="3" fill="#1a73e8"/>']
    for label, x in tabs:
        selected = label == "Application"
        parts.append(svg_text(x, 23, label, size=12.5,
                              fill="#1a73e8" if selected else "#5b6470",
                              weight="700" if selected else "normal"))
    parts.append(svg_text(618, 23, "»", size=15, fill="#5b6470",
                          weight="700"))
    parts.append(dim_rows(24, 56, 300, 4))
    parts.append(callout(551, 96, 551, 44, "", label_x=430, label_y=112,
                         lines=["click Application — if you cannot see it,",
                                "it is hiding under the »"]))
    return panel(720, 133, "".join(parts))


def panel_tree() -> str:
    """Storage &gt; Cookies &gt; https://fantasy.espn.com in the left sidebar."""
    rows = [(16, 58, "Application", "700", "#1f2328"),
            (28, 82, "▸  Manifest", "normal", "#3c4043"),
            (28, 104, "▸  Service workers", "normal", "#3c4043"),
            (16, 132, "Storage", "700", "#1f2328"),
            (34, 156, "▸  Local storage", "normal", "#3c4043"),
            (34, 178, "▸  Session storage", "normal", "#3c4043"),
            (34, 200, "▾  Cookies", "700", "#1f2328"),
            (52, 222, "https://fantasy.espn.com", "700", "#1a73e8"),
            (52, 244, "https://www.espn.com", "normal", "#3c4043")]
    parts = ['<rect x="1" y="1" width="718" height="268" rx="8" fill="#fff" '
             'stroke="#d8dde3"/>',
             '<rect x="1" y="1" width="718" height="30" fill="#f1f3f4"/>',
             svg_text(16, 21, "Application", size=12, fill="#1a73e8",
                      weight="700"),
             '<rect x="1" y="31" width="262" height="238" fill="#fbfcfd"/>',
             '<line x1="263" y1="31" x2="263" y2="269" stroke="#e4e7eb"/>',
             '<rect x="1" y="208" width="262" height="22" fill="#e8f0fe"/>']
    for x, y, label, weight, fill in rows:
        parts.append(svg_text(x, y, label, size=12, fill=fill, weight=weight))
    parts.append(dim_rows(288, 60, 380, 6, gap=18))
    parts.append(callout(470, 219, 278, 219, "", label_x=482, label_y=215,
                         anchor="start",
                         lines=["this one — the cookies", "live under here"]))
    return panel(720, 270, "".join(parts))


def _cookie_table(rows: List[tuple], highlight: int, height: int) -> List[str]:
    """Shared header and body for the two cookie-table panels."""
    parts = [f'<rect x="1" y="1" width="718" height="{height - 2}" rx="8" '
             'fill="#fff" stroke="#d8dde3"/>',
             '<rect x="1" y="1" width="718" height="28" fill="#f1f3f4"/>',
             '<line x1="1" y1="29" x2="719" y2="29" stroke="#dadce0"/>',
             '<line x1="168" y1="1" x2="168" y2="29" stroke="#dadce0"/>',
             '<line x1="556" y1="1" x2="556" y2="29" stroke="#dadce0"/>',
             svg_text(16, 20, "Name", size=11.5, fill="#5b6470", weight="700"),
             svg_text(180, 20, "Value", size=11.5, fill="#5b6470",
                      weight="700"),
             svg_text(568, 20, "Domain", size=11.5, fill="#5b6470",
                      weight="700")]
    y = 29
    for i, (name, value, domain) in enumerate(rows):
        target = i == highlight
        if target:
            parts.append(f'<rect x="1" y="{y}" width="718" height="26" '
                         'fill="#fff3cd"/>')
        ink = "#1f2328" if target else "#9aa4b0"
        parts.append(svg_text(16, y + 18, name, size=12, family=MONO, fill=ink,
                              weight="700" if target else "normal", fit=True))
        parts.append(svg_text(180, y + 18, value, size=12, family=MONO,
                              fill=ink, weight="700" if target else "normal",
                              fit=True))
        parts.append(svg_text(568, y + 18, domain, size=11.5, family=MONO,
                              fill="#9aa4b0"))
        y += 26
    return parts


def panel_swid() -> str:
    """The SWID row, with both braces ringed."""
    rows = [("region", "usa", ".espn.com"),
            ("s_ecid", "MCMID%7C1938442", ".espn.com"),
            ("SWID", EXAMPLE_SWID, ".espn.com"),
            ("espn_s2", "AEBq7mXk%2FvR3TnGx…", ".espn.com"),
            ("nol_fpid", "tt2xq9v0s1", ".espn.com")]
    parts = _cookie_table(rows, highlight=2, height=215)
    size, x0 = 12, 180.0
    y = 29 + 2 * 26 + 14          # baseline of the SWID value
    open_cx = x0 + mono_w("{", size) / 2
    close_cx = x0 + mono_w(EXAMPLE_SWID, size) - mono_w("}", size) / 2
    for cx in (open_cx, close_cx):
        parts.append(f'<ellipse cx="{cx}" cy="{y - 4}" rx="8" ry="11" '
                     'fill="none" stroke="var(--mark)" stroke-width="1.8"/>')
    # Straight down from the closing brace. The two rows it crosses are empty
    # at this x, so the arrow never runs through somebody else's value.
    parts.append(callout(close_cx, 188, close_cx, y + 10, "",
                         label_x=close_cx, label_y=206, anchor="middle",
                         lines=["copy the curly braces too — "
                                "they are part of the value"]))
    return panel(720, 215, "".join(parts))


def panel_s2() -> str:
    """The espn_s2 row plus the preview pane and its decode checkbox."""
    rows = [("SWID", EXAMPLE_SWID[:18] + "…", ".espn.com"),
            ("espn_s2", EXAMPLE_S2[:30] + "…", ".espn.com"),
            ("nol_fpid", "tt2xq9v0s1", ".espn.com")]
    parts = _cookie_table(rows, highlight=1, height=285)
    parts.append('<line x1="1" y1="118" x2="719" y2="118" stroke="#dadce0"/>')
    parts.append('<rect x="1" y="118" width="718" height="165" fill="#fbfcfd"/>')
    parts.append(svg_text(16, 138, "Cookie Value", size=11.5, fill="#5b6470",
                          weight="700"))
    parts.append('<rect x="16" y="148" width="688" height="46" rx="4" '
                 'fill="#fff" stroke="#e4e7eb"/>')
    parts.append(svg_text(26, 168, EXAMPLE_S2, size=11.5, family=MONO,
                          fill="#1f2328"))
    parts.append(svg_text(26, 186, "5wQhTzVrB%2FnKdLp8XsYmE3aGc1UoJ9f",
                          size=11.5, family=MONO, fill="#1f2328"))
    parts.append('<rect x="18" y="208" width="13" height="13" rx="2" '
                 'fill="#fff" stroke="#9aa4b0" stroke-width="1.5"/>')
    parts.append(svg_text(40, 219, "Show URL-decoded", size=12, fill="#3c4043"))
    parts.append('<ellipse cx="24.5" cy="214.5" rx="13" ry="13" fill="none" '
                 'stroke="var(--mark)" stroke-width="1.8"/>')
    parts.append(callout(60, 252, 28, 231, "", label_x=74, label_y=250,
                         anchor="start",
                         lines=["leave this box UNCHECKED —",
                                "the %-signs have to stay in"]))
    return panel(720, 285, "".join(parts))


def panel_members() -> str:
    """League members: the manager name, not the team name."""
    rows = [("Thunder Cats", "Jordan Ellis"),
            ("Gridiron Gang", "Sam Ortiz"),
            ("Waiver Wire Warriors", "Alex Kim")]
    parts = ['<rect x="1" y="1" width="718" height="168" rx="8" fill="#fff" '
             'stroke="#d8dde3"/>',
             svg_text(20, 28, "League Members", size=14, weight="700",
                      fill="#1f2328"),
             '<rect x="1" y="42" width="718" height="28" fill="#f1f3f4"/>',
             '<line x1="1" y1="70" x2="719" y2="70" stroke="#dadce0"/>',
             '<rect x="330" y="42" width="200" height="127" '
             'fill="#fff3cd" opacity="0.6"/>',
             svg_text(20, 61, "TEAM", size=11, fill="#5b6470", weight="700"),
             svg_text(344, 61, "MANAGER", size=11, fill="#1f2328",
                      weight="700")]
    y = 70
    for team, manager in rows:
        parts.append(svg_text(20, y + 22, team, size=12.5, fill="#9aa4b0"))
        parts.append(svg_text(344, y + 22, manager, size=12.5, fill="#1f2328",
                              weight="700"))
        parts.append(f'<line x1="1" y1="{y + 32}" x2="719" y2="{y + 32}" '
                     'stroke="#f0f2f4"/>')
        y += 32
    parts.append(callout(600, 104, 470, 92, "", label_x=612, label_y=100,
                         anchor="start",
                         lines=["this name, not", "the team name"]))
    return panel(720, 170, "".join(parts))


# --------------------------------------------------------------------------- #
# The page
# --------------------------------------------------------------------------- #

#: One arrowhead, defined once and referenced by every panel in the document.
DEFS = ('<svg width="0" height="0" aria-hidden="true" '
        'xmlns="http://www.w3.org/2000/svg"><defs>'
        '<marker id="arw" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="6" markerHeight="6" orient="auto-start-reverse">'
        '<path d="M0,0 L10,5 L0,10 z" fill="var(--mark)"/>'
        '</marker></defs></svg>')


def step(number: int, title: str, body: str, figure: str = "",
         caption: str = "") -> str:
    """One numbered step, kept whole across a page break by the print rules."""
    fig = ""
    if figure:
        fig = (f"<figure>{figure}"
               + (f"<figcaption>{caption}</figcaption>" if caption else "")
               + "</figure>")
    return (f'<section class="step"><div class="n">{number}</div>'
            f"<h2>{title}</h2>{body}{fig}</section>")


FORM_FIELDS = (
    ("The league's web address", "paste the whole thing"),
    ("League name", ""),
    ("Your manager name, spelled exactly as ESPN has it", ""),
    ("First season this league was on ESPN", "a year, e.g. 2019"),
    ("SWID", "with the { } braces"),
    ("espn_s2", "the long one"),
)


def form_block() -> str:
    """The copy-paste block that comes back filled in."""
    rows = "".join(
        f'<div class="row"><span class="label">{esc(label)}'
        + (f" <em>({esc(hint)})</em>" if hint else "")
        + ":</span></div>"
        for label, hint in FORM_FIELDS)
    return f'<div class="form">{rows}</div>'


def render() -> str:
    """Assemble the guide.

    Returns:
        str: A complete, self-contained HTML document -- no webfonts, no
        images, nothing fetched at open time.
    """
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Connect your ESPN league</title>
<style>{STYLE}</style>
</head>
<body>
{DEFS}
<h1>Connect your ESPN league</h1>
<p class="lede">Six things out of your ESPN account and I can pull your league
into the projections. It takes about five minutes, once, and there is a form at
the end to fill in and send straight back.</p>

<div class="box warn">
  <h3>Before you start</h3>
  <p>You need <strong>a computer</strong> — laptop or desktop — <strong>and the
  Chrome browser</strong>. A phone cannot do this; the tool we need does not
  exist in the mobile app or in mobile Safari. Microsoft Edge works identically
  if that is what you have.</p>
  <p style="margin-bottom:0">Log in at <code>espn.com</code> first and open the
  league you want connected, so you are looking at your own team.</p>
</div>

{step(1, "Open your league and look at the address bar",
      "<p>With your league open, the web address at the top has a long number "
      "in it. That number is the league's ID. You do not have to pick it out — "
      "<strong>select the whole address and copy it</strong>, and I will take "
      "the number out of it.</p>",
      panel_url(),
      "The address bar on any page inside your league. The number after "
      "leagueId= is the one that matters.")}

{step(2, "Right-click anywhere on the page, then click Inspect",
      "<p>Right-click on an empty part of the page — not on a link or an "
      "image — and a menu appears. <strong>Inspect</strong> is at the very "
      "bottom. A second panel opens beside or underneath the page; that is "
      "normal, and nothing you do in there changes your team.</p>"
      "<p>If right-clicking does nothing, press <code>Option</code> + "
      "<code>Command</code> + <code>I</code> on a Mac, or <code>F12</code> on "
      "Windows.</p>",
      panel_inspect(),
      "The right-click menu. Inspect is always the last item.")}

{step(3, "In that new panel, click Application",
      "<p>The new panel has a row of words along its top: Elements, Console, "
      "Sources and so on. Click <strong>Application</strong>. If you cannot "
      "see it, the row has run out of space — click the "
      "<code>&#187;</code> at the end of the row and pick Application from the "
      "list that drops down.</p>",
      panel_tabs(),
      "The row of tabs across the top of the panel that just opened.")}

{step(4, "On the left, open Storage → Cookies → "
      "https://fantasy.espn.com",
      "<p>Down the left-hand side is a list. Find the <strong>Storage</strong> "
      "heading, click the little arrow next to <strong>Cookies</strong> to open "
      "it, and then click <strong>https://fantasy.espn.com</strong>. A big "
      "table fills the right-hand side.</p>",
      panel_tree(),
      "The left-hand list, with Cookies opened up.")}

{step(5, "Find the row called SWID and copy its Value",
      "<p>The table is sorted alphabetically and it is long — scroll until you "
      "find <strong>SWID</strong> in the left-hand Name column. Copy what sits "
      "next to it in the <strong>Value</strong> column.</p>"
      '<p class="flag">Copy the curly braces at each end as well. They look '
      "like punctuation but they are part of the value, and it will not work "
      "without them.</p>",
      panel_swid(),
      "It is a short code in the shape of a serial number, wrapped in "
      "{ curly braces }.")}

{step(6, "Find the row called espn_s2 and copy its Value",
      "<p>Same table, a different row: <strong>espn_s2</strong>. This one is "
      "much longer and has <code>%</code> signs scattered through it. Because "
      "it is too long to fit the column, <strong>click the row once</strong> "
      "and the whole thing appears in a panel underneath, where you can select "
      "it and copy it.</p>"
      '<p class="flag">If you see a checkbox marked “Show URL-decoded”, '
      "leave it unchecked. Ticking it quietly rewrites the value and the "
      "connection will fail.</p>",
      panel_s2(),
      "Click the espn_s2 row and the full value appears in the pane below.")}

{step(7, "Last thing: your name as ESPN spells it",
      "<p>My end matches you to your team by <strong>manager name</strong>, not "
      "by team name, and it has to match character for character — "
      "“Mike” and “Michael” are two different people to it. "
      "Open your league's <strong>Members</strong> page and tell me the name "
      "shown against your team, plus roughly what year the league started on "
      "ESPN.</p>"
      "<p>Easiest of all: take a screenshot of that Members page and send it "
      "along. That answers the league name, your manager name and the spelling "
      "in one go.</p>",
      panel_members(),
      "League → Members. The manager name is the one I need.")}

<section class="step" style="padding-left:0">
<h2>Send me these six things</h2>
<p>All in one message is easiest — the two codes are no use to me one at a
time.</p>
{form_block()}
</section>

<div class="box">
  <h3>If something looks different</h3>
  <ul>
    <li><strong>No <code>https://fantasy.espn.com</code> in the list.</strong>
    Open your league page first, reload it, then look again. Failing that, click
    <code>https://www.espn.com</code> instead — the same two rows are in there.</li>
    <li><strong>The value looks chopped off.</strong> It is — the column is too
    narrow. Click the row and copy from the pane underneath.</li>
    <li><strong>It worked and then stopped a few weeks later.</strong> Expected.
    ESPN expires these on a schedule and I will ask you for a fresh pair; it is
    the same five minutes.</li>
  </ul>
</div>

<div class="box warn">
  <h3>What you are actually sending me</h3>
  <p>Those two codes are the pass your browser shows ESPN to prove you are
  logged in. They are <strong>not</strong> your password, and they cannot be
  turned back into it — but while they last they do let me read ESPN as you, so
  treat them like a password: send them to me directly, not into a group chat,
  and do not post them anywhere public.</p>
  <p style="margin-bottom:0">They expire on their own after a few weeks. If you
  ever want to cut them off sooner, change your ESPN password and both codes
  stop working immediately.</p>
</div>

<p class="sig">Any step that does not look like the picture, send me a screenshot
of what you are seeing and I will point at the right thing.</p>
</body>
</html>
"""


def write_pdf(html_path: Path, pdf_path: Path) -> None:
    """Print the guide to PDF with headless Chromium.

    Uses the same Playwright entry point as ``Scripts.bol_widget``. The page
    size comes from the stylesheet's ``@page`` rule rather than from here, so
    the HTML stays the single source of layout.

    Args:
        html_path: The rendered HTML on disk.
        pdf_path: Where to write the PDF.
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        try:
            page = browser.new_page()
            page.goto(html_path.resolve().as_uri())
            page.emulate_media(media="print")
            page.pdf(path=str(pdf_path), print_background=True,
                     prefer_css_page_size=True)
        finally:
            browser.close()


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.guide_credentials",
        description="Render the ESPN credential guide to HTML and PDF.")
    parser.add_argument("--out", default=str(OUTPUT_PATH),
                        help=f"HTML output path (default {OUTPUT_PATH})")
    parser.add_argument("--no-pdf", action="store_true",
                        help="skip the PDF, which is the slow half")
    args = parser.parse_args(argv)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render())
    print(f"wrote {out} ({out.stat().st_size:,} bytes)")

    if not args.no_pdf:
        pdf = out.with_suffix(".pdf")
        write_pdf(out, pdf)
        print(f"wrote {pdf} ({pdf.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
