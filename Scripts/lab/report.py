"""Render ``docs/model_lab.html`` from the experiment ledger.

A generated file, committed. Generated because hand-maintained result tables drift
from the numbers they claim to report; committed because the point of it is to be
readable without running anything, and diffable so that "what changed between
drafts" is a question git can answer.

Self-contained: no CDN, no build step, no new dependency. Tables carry the numbers
because that is this repo's idiom -- every other report it produces is a fixed-width
console table -- and inline SVG carries the comparisons, where seeing eight signed
numbers at once beats reading them.

Usage:
    python -m Scripts.lab.report
    python -m Scripts.lab.report --out /tmp/preview.html
"""

import argparse
import html
import json
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

from Scripts.lab import registry as reg
from Scripts.lab.run import RESULTS_PATH
from Scripts.paths import REPO_ROOT
from Scripts.usage import features as ft

#: Where the rendered ledger lands.
OUTPUT_PATH = REPO_ROOT / "docs" / "model_lab.html"

#: Positions, in the order every table in this repo uses.
POSITIONS = list(ft.MODELLED_POSITIONS)

#: Diverging pair from the data-viz reference palette: blue and red, warm/cool poles
#: that read as opposite, with a gray midpoint. A delta is a diverging quantity --
#: it has a sign and a meaningful zero -- so it takes the diverging pair rather than
#: two categorical hues, which would read as two series rather than two directions.
POSITIVE_LIGHT, NEGATIVE_LIGHT = "#2a78d6", "#e34948"
POSITIVE_DARK, NEGATIVE_DARK = "#3987e5", "#e66767"


def esc(value) -> str:
    """HTML-escape a value, including the numbers, so nothing has to be trusted."""
    return html.escape(str(value))


def bar_chart(rows: Sequence[Tuple[str, Optional[float]]], *, unit: str = "",
              caption: str = "", decimals: int = 4, higher_is_better: bool = True
              ) -> str:
    """A horizontal diverging bar chart, as inline SVG.

    Zero is a real position on the axis rather than the left edge, because the
    quantity drawn is signed and growing every bar from the edge would make a small
    regression look like a small improvement.

    **Colour encodes good against bad, not positive against negative.** On the MAE
    charts a negative number is an improvement, so mapping the diverging pair to the
    arithmetic sign would paint every improvement in the regression colour --
    precisely inverting the one thing a reader takes from a glance. ``caption`` says
    which way round it is; the colour has to agree with the caption.

    Value labels live in their own right-hand column at a fixed x rather than riding
    the end of each bar. Attached labels collide with the category labels whenever a
    bar is short and negative, which is most of them here.

    Args:
        rows: ``(label, value)`` pairs. A None value renders as an explicit gap
            rather than a zero -- "not measured" and "measured at zero" are
            different claims.
        unit: Appended to each value label.
        caption: Rendered above the chart.
        decimals: Digits in the value labels.
        higher_is_better: False when a negative value is the good outcome.

    Returns:
        str: An ``<svg>`` element, or an empty string when there is nothing to draw.
    """
    drawable = [(label, value) for label, value in rows if value is not None]
    if not drawable:
        return ""

    bar_height, gap = 15, 9
    label_width, plot_width, value_width = 150, 250, 84
    height = len(rows) * (bar_height + gap) + gap
    width = label_width + plot_width + value_width
    zero_x = label_width + plot_width / 2

    span = max(abs(value) for _, value in drawable) or 1.0
    scale = (plot_width / 2 - 4) / span

    parts = [
        f'<svg class="chart" viewBox="0 0 {width} {height}" width="{width}" '
        f'height="{height}" role="img" aria-label="{esc(caption)}">',
        # The zero rule, under the marks, in the baseline ink.
        f'<line x1="{zero_x:.1f}" y1="{gap / 2:.1f}" x2="{zero_x:.1f}" '
        f'y2="{height - gap / 2:.1f}" class="axis" />',
    ]

    for index, (label, value) in enumerate(rows):
        y = gap + index * (bar_height + gap)
        baseline_y = y + bar_height * 0.78
        parts.append(
            f'<text x="{label_width - 12}" y="{baseline_y:.1f}" '
            f'class="tick" text-anchor="end">{esc(label)}</text>')

        if value is None:
            parts.append(
                f'<text x="{zero_x + 8:.1f}" y="{baseline_y:.1f}" '
                f'class="muted">not measured</text>')
            continue

        length = max(abs(value) * scale, 1.0)
        x = zero_x if value >= 0 else zero_x - length
        good = (value >= 0) if higher_is_better else (value <= 0)
        fill = "var(--positive)" if good else "var(--negative)"
        # 3px rounded data-end; the zero end stays square against the axis, which is
        # what anchors the mark to the baseline rather than floating it.
        parts.append(
            f'<rect x="{x:.1f}" y="{y}" width="{length:.1f}" '
            f'height="{bar_height}" rx="3" fill="{fill}">'
            f'<title>{esc(label)}: {value:+.{decimals}f}{esc(unit)}</title></rect>')
        parts.append(
            f'<text x="{width - 6}" y="{baseline_y:.1f}" class="tick" '
            f'text-anchor="end">{value:+.{decimals}f}{esc(unit)}</text>')

    parts.append("</svg>")
    caption_html = f'<p class="caption">{esc(caption)}</p>' if caption else ""
    return f'<figure>{caption_html}{"".join(parts)}</figure>'


def table(headers: Sequence[str], rows: Sequence[Sequence[str]],
          align_right_from: int = 1) -> str:
    """A plain table, numbers right-aligned and tabular."""
    # Built with concatenation rather than a nested f-string: a backslash inside an
    # f-string expression is a syntax error before Python 3.12, and this repo runs
    # on 3.11.
    num = ' class="num"'
    head = "".join(
        "<th" + (num if i >= align_right_from else "") + f">{esc(h)}</th>"
        for i, h in enumerate(headers))
    body = "".join(
        "<tr>" + "".join(
            "<td" + (num if i >= align_right_from else "") + f">{cell}</td>"
            for i, cell in enumerate(row)) + "</tr>"
        for row in rows)
    return (f'<div class="scroll"><table><thead><tr>{head}</tr></thead>'
            f'<tbody>{body}</tbody></table></div>')


def fmt(value: Optional[float], decimals: int = 4, signed: bool = False) -> str:
    """Format a number, or an em dash when it is absent."""
    if value is None:
        return '<span class="muted">—</span>'
    return f"{value:+.{decimals}f}" if signed else f"{value:.{decimals}f}"


def baseline_section(base: Dict) -> str:
    """Where the model stands today, before anything in this document."""
    rows = []
    for position in POSITIONS:
        usg = base["spearman"].get(position)
        naive = base["naive_spearman"].get(position)
        rows.append([
            position,
            str(base["spearman_n"].get(position, "")),
            fmt(usg), fmt(naive),
            fmt(None if usg is None or naive is None else usg - naive, signed=True),
            fmt(base["top_n"].get(position), 3),
            fmt(base["naive_top_n"].get(position), 3),
        ])
    spearman = table(
        ["position", "n", "USG", "naive", "delta", "top-N USG", "top-N naive"], rows)

    mae_rows = []
    for stat, entry in base["mae"].items():
        naive = entry.get("naive")
        delta = (100 * (entry["usg"] - naive) / naive) if naive else None
        mae_rows.append([
            stat, str(entry["n"]), f"{entry['usg']:.2f}",
            f"{naive:.2f}" if naive else "—",
            fmt(delta, 1, signed=True) + "%" if delta is not None else "—",
        ])
    mae = table(["stat", "n", "USG MAE", "naive MAE", "delta"], mae_rows)

    arms = base.get("arms", {})
    arm_text = ", ".join(f"{k} {v:,}" for k, v in sorted(arms.items()))
    return f"""
<section id="where-we-sit">
  <h2>Where we sit</h2>
  <p>The season head predicts <code>expected&nbsp;games × per-game&nbsp;volume ×
  efficiency&nbsp;rate</code> through three ordinary-least-squares heads, and is one
  of three sources in the blend at a third of the weight each. Everything below is
  measured on the walk-forward in <code>Scripts/usage/backtest.py</code>: for each
  season <em>S</em> from {esc(base['seasons'][0])} to {esc(base['seasons'][-1])},
  train on {esc(base['seasons'][0] - 3)}…<em>S</em>−1 and predict <em>S</em>, scored
  through one league's real scoring rules.</p>
  <p class="note">Veteran rows only, throughout. The naive baseline is last season's
  production carried forward, which is zero for every rookie by construction, so
  pooling rookies in would credit the model for <em>covering</em> them rather than
  for projecting anyone more accurately.</p>
  <h3>Ordering, against the naive carry-forward</h3>
  {spearman}
  <h3>Per-stat mean absolute error</h3>
  {mae}
  <p class="note">Coverage: {esc(f"{base['projected']:,}")} of
  {esc(f"{base['rostered']:,}")} rostered player-seasons
  ({100 * base['projected'] / base['rostered']:.1f}%) got a projection. By arm:
  {esc(arm_text)}.</p>
</section>"""


def rule_section() -> str:
    """The decision rule, stated before any result that it judged."""
    return f"""
<section id="rule">
  <h2>The decision rule</h2>
  <p>Written down in <code>Scripts/lab/registry.py</code> <strong>before</strong> the
  experiments ran, and applied mechanically. A threshold chosen after seeing the
  numbers is not a threshold, and a model assembled by keeping whatever happened to
  look good on one walk-forward acquires six features that each add 0.003 and
  together add nothing.</p>
  <p>A feature is merged only if all three hold:</p>
  <ol>
    <li>Mean within-position Spearman gain ≥
      <strong>{MIN_GAIN:+.4f}</strong>.</li>
    <li>No single position loses more than
      <strong>{MAX_LOSS:.4f}</strong> — a board is read one position at a time, so a
      good average bought by wrecking tight ends is not a good average.</li>
    <li>Top-N hit rate falls at no more than
      <strong>{MAX_REG}</strong> position.</li>
  </ol>
  <p class="note">For scale: the depth chart, the only feature that has ever moved
  this model, was worth +0.048 R² on veteran carries and +0.05 to +0.07 Spearman.
  The rejected coach priors managed at most 0.0012 in either direction. The bar is
  set low on purpose — it is there to exclude noise, not to demand another depth
  chart.</p>
</section>"""


def ledger_section(results: Dict) -> str:
    """The summary table: one row per experiment, with its verdict."""
    base = results["baseline"]
    rows = []
    for name, entry in sorted(results.items()):
        if name == "baseline":
            continue
        deltas = [entry["spearman"][p] - base["spearman"][p]
                  for p in POSITIONS
                  if p in entry["spearman"] and p in base["spearman"]]
        mean = sum(deltas) / len(deltas) if deltas else None
        call = entry.get("verdict", "—")
        badge = ("merge" if call == "merge" else "reject")
        rows.append([
            f'<a href="#{esc(name)}"><code>{esc(name)}</code></a>',
            esc(entry["source"]),
            fmt(mean, signed=True),
            f'<span class="badge {badge}">{esc(call)}</span>',
        ])
    return f"""
<section id="ledger">
  <h2>Candidate ledger</h2>
  {table(["experiment", "data source", "mean Spearman Δ", "verdict"], rows, 2)}
</section>"""


def experiment_section(name: str, entry: Dict, base: Dict) -> str:
    """One experiment in full: hypothesis, charts, subpopulations, verdict."""
    spearman_rows = [
        (position, entry["spearman"][position] - base["spearman"][position])
        if position in entry["spearman"] and position in base["spearman"]
        else (position, None)
        for position in POSITIONS
    ]
    mae_rows = [
        (stat, 100 * (entry["mae"][stat]["usg"] - base["mae"][stat]["usg"])
         / base["mae"][stat]["usg"])
        if stat in entry["mae"] and base["mae"].get(stat, {}).get("usg")
        else (stat, None)
        for stat in base["mae"]
    ]

    sub_rows = []
    for slice_name, values in (entry.get("subpopulations") or {}).items():
        reference = (base.get("subpopulations") or {}).get(slice_name, {})
        delta = (values["usg"] - reference["usg"]) if reference else None
        sub_rows.append([
            slice_name.replace("_", " "),
            f"{values['n']:,}",
            fmt(values["usg"]),
            fmt(reference.get("usg")),
            fmt(delta, signed=True),
        ])
    subpopulations = table(
        ["subpopulation", "n", "this run", "baseline", "delta"], sub_rows, 1
    ) if sub_rows else ""

    call = entry.get("verdict", "—")
    badge = "merge" if call == "merge" else "reject"
    note = f'<p class="note">{esc(entry["note"])}</p>' if entry.get("note") else ""
    added = [r for r in entry["volume_regressors"]
             if r not in base["volume_regressors"]]
    added_html = (f'<p>Regressors added: '
                  f'{", ".join(f"<code>{esc(r)}</code>" for r in added)}.</p>'
                  if added else "")
    kwargs = entry.get("feature_kwargs") or {}
    kwargs_html = (f'<p>Feature settings: <code>{esc(json.dumps(kwargs))}</code>.</p>'
                   if kwargs else "")

    return f"""
<section id="{esc(name)}" class="experiment">
  <h3><code>{esc(name)}</code>
    <span class="badge {badge}">{esc(call)}</span></h3>
  <p class="hypothesis"><strong>Hypothesis.</strong> {esc(entry["hypothesis"])}</p>
  <p><strong>Source.</strong> {esc(entry["source"])}</p>
  {added_html}{kwargs_html}{note}
  <div class="charts">
    {bar_chart(spearman_rows,
               caption="Within-position Spearman, change against the baseline. "
                       "Higher is better.")}
    {bar_chart(mae_rows, unit="%", decimals=2, higher_is_better=False,
               caption="Per-stat mean absolute error, % change against the "
                       "baseline. Lower is better, so blue is still the "
                       "improvement.")}
  </div>
  {f"<h4>By subpopulation</h4>{subpopulations}" if subpopulations else ""}
  <p class="verdict"><strong>{esc(call.upper())}</strong> —
  {esc(entry.get("verdict_reason", ""))}</p>
</section>"""


def ridge_sweep_section(results: Dict) -> str:
    """The alpha curve, which is the one finding that needs its own view.

    Two charts against the same x rather than one chart with two y-axes. A
    dual-axis chart lets the author choose where the lines cross by choosing the
    scales, which is exactly the choice a reader assumes was not made.
    """
    base = results["baseline"]
    alphas = sorted(
        (float(name.rsplit("_", 1)[1]), name)
        for name in results if name.startswith("ridge_alpha_"))
    if not alphas:
        return ""

    spearman_rows, mae_rows, table_rows = [], [], []
    for alpha, name in alphas:
        entry = results[name]
        deltas = [entry["spearman"][p] - base["spearman"][p]
                  for p in POSITIONS if p in entry["spearman"]]
        mean_spearman = sum(deltas) / len(deltas) if deltas else None
        changes = [100 * (entry["mae"][s]["usg"] - base["mae"][s]["usg"])
                   / base["mae"][s]["usg"]
                   for s in base["mae"] if s in entry["mae"]]
        mean_mae = sum(changes) / len(changes) if changes else None

        label = f"α = {alpha:g}"
        spearman_rows.append((label, mean_spearman))
        mae_rows.append((label, mean_mae))
        thin = (entry.get("subpopulations") or {}).get("thin_prior_season", {})
        thin_base = (base.get("subpopulations") or {}).get("thin_prior_season", {})
        table_rows.append([
            f"{alpha:g}",
            fmt(mean_spearman, signed=True),
            fmt(mean_mae, 2, signed=True) + "%",
            fmt(thin["usg"] - thin_base["usg"], signed=True)
            if thin and thin_base else "—",
            f'<span class="badge {"merge" if entry.get("verdict") == "merge" else "reject"}">'
            f'{esc(entry.get("verdict", "—"))}</span>',
        ])

    return f"""
<section id="ridge-sweep">
  <h2>The ridge sweep, which was the surprise</h2>
  <p>This was predicted to be a null result and it was not. Regularising the volume
  and games heads improves within-position ordering <strong>monotonically</strong>,
  at every position, all the way out to α = 300 — a larger effect than any feature
  in this document. It also makes every per-stat error monotonically worse.</p>
  <div class="charts">
    {bar_chart(spearman_rows,
               caption="Mean within-position Spearman, change against ordinary "
                       "least squares. Higher is better.")}
    {bar_chart(mae_rows, unit="%", decimals=2, higher_is_better=False,
               caption="Mean per-stat MAE, % change against ordinary least "
                       "squares. Lower is better.")}
  </div>
  {table(["alpha", "mean Spearman Δ", "mean MAE Δ",
          "thin-prior-season Δ", "verdict"], table_rows)}
  <p>Two readings, and the second is the right one.</p>
  <p>The charitable one: shrinkage helps most where the data is thinnest, which is
  what the subpopulation column shows — at α = 100 the thin-prior-season slice gains
  <strong>+0.0374</strong> against the settled slice's +0.0051. That is the ordinary
  bias-variance trade, working as advertised.</p>
  <p>The decisive one: <strong>there is no interior optimum.</strong> A real
  bias-variance sweet spot has a peak — error falls, bottoms out, then rises. This
  curve only rises, and the accuracy cost rises with it without bound. What that
  describes is not a better-fitted model but progressive reversion toward the
  positional mean, which orders players slightly better because the mean is a decent
  ranker, and prices them worse because it is a terrible estimate. Prediction spread
  at quarterback falls from 91.1 to 82.3 against a realised 118.6 — the model was
  already under-dispersed, and this makes it more so.</p>
  <p>Plan 18 already named this failure: <em>a model that quietly emits a positional
  average looks like full coverage and drags the blend toward the mean for exactly
  the players a board must differentiate.</em> <code>USG_</code> is a stat line that
  gets averaged with ESPN's and FantasyPros' before anything is priced, so an 8%
  worse yardage number is a real cost paid by every player in the blend, in exchange
  for an ordering gain inside one of three sources.</p>
  <p class="note">Two settings, α = 30 and α = 100, passed the decision rule as it
  was first <em>implemented</em> — which checked ordering and never looked at
  accuracy. The plan specified an accuracy clause and the code did not have one. The
  clause is now in <code>Scripts/lab/registry.py</code>; this is the case that found
  the gap, and it is worth recording that the gap was found by a result that looked
  like good news.</p>
</section>"""


def blend_section(ledger: Dict) -> str:
    """Whether the blend weights can be fitted. They cannot, and why."""
    block = ledger.get("blend_weights")
    if not block or not block.get("results"):
        return ""

    show = lambda w: " ".join(f"{k}={v:.2f}" for k, v in w.items())
    rows = [
        [entry["stat"],
         f"{entry['n_odd']} / {entry['n_even']}",
         f'<code>{esc(show(entry["odd_weights"]))}</code>',
         f'<code>{esc(show(entry["even_weights"]))}</code>',
         fmt(entry["max_half_disagreement"], 2),
         fmt(entry["out_of_sample_mae_change_pct"], 1, signed=True) + "%"]
        for entry in block["results"]
    ]
    total = block["results"][0]["n_total_evalset"]
    return f"""
<section id="blend-weights">
  <h2>Can the blend weights be fitted?</h2>
  <p>The weights are ESPN, FantasyPros and the usage head at a third each, set by
  hand. Fitting them against realised outcomes is the obvious improvement. The
  answer is that <strong>this data does not identify them</strong> — which is a
  more useful result than "the current weights are fine".</p>
  {table(["stat", "n odd / even", "fitted on odd weeks", "fitted on even weeks",
          "max gap", "OOS MAE"], rows, 1)}
  <p>Two halves of one season, the same quantity, and the fits do not agree.
  Receiving receptions goes from <code>PINNY=0.63</code> on odd weeks to
  <code>PINNY=0.00, FP=1.00</code> on even weeks. Out of sample the fitted weights
  beat the shipped ones by 2.1% on one stat and lose on another.</p>
  <p>Three reasons, and none of them is fixable by fitting harder:</p>
  <ul>
    <li><strong>The sample collapses.</strong> Requiring every source to be real
    rather than imputed takes {esc(f"{total:,}")} player-weeks down to 110–236 per
    half — and the survivors are the heavily-covered stars, not the population the
    weights get applied to.</li>
    <li><strong>The sources are collinear.</strong> Plan 16's G0 measured
    FantasyPros' residuals at <strong>+0.988</strong> against ESPN's. No procedure
    can say how to split weight between two near-copies; non-negative least squares
    responds by giving one of them everything, and which one depends on the half.</li>
    <li><strong>The season question is not this question.</strong> These are weekly
    rows. The open question is the <em>season</em> blend's usage weight, and there
    is no historical season blend to fit against — plan 18 records that as a
    permanent limitation of the data rather than a gap in the work.</li>
  </ul>
  <p class="note">Reproduce with <code>python -m Scripts.lab.blend</code>.</p>
</section>"""


def injury_section(ledger: Dict) -> str:
    """The injury model's walk-forward. Both heads rejected, and the reason is a metric."""
    block = ledger.get("injury")
    if not block or not block.get("folds"):
        return ""

    metrics = block["metrics"]
    verdict = block.get("verdict", {})
    rmse = metrics.get("rmse_gain_pct") or {}

    fold_rows = [
        [str(fold["season"]), f"{fold['test_episodes']:,}", f"{fold['test_rows']:,}",
         f"{fold['k']:.0f}", fmt(fold["mae"]["do_nothing"], 3),
         fmt(fold["gain_pct"].get("blind"), 2, signed=True),
         fmt(fold["gain_pct"].get("oracle"), 2, signed=True),
         fmt(fold["gain_pct"].get("hypothesised_ladder"), 2, signed=True)]
        for fold in block["folds"]
    ]

    label = {"blind": "fitted, body part only (what the live system has)",
             "oracle": "fitted, conditioned on realised duration",
             "global_only": "a single global curve, no cell structure",
             "hypothesised_ladder": "the hypothesised 0.75 / 0.75 / 0.85 / 0.92 ladder"}
    metric_rows = [
        [label[name],
         fmt(metrics.get(f"{'post_return' if name == 'blind' else name}_mae_gain_pct"),
             2, signed=True) if name == "blind"
         else fmt(metrics.get(f"{name}_mae_gain_pct"), 2, signed=True),
         fmt(rmse.get(name), 2, signed=True)]
        for name in ("hypothesised_ladder", "global_only", "oracle", "blind")
    ]
    metric_rows.append(["healthy comparables, discounted as if injured",
                        fmt(metrics.get("control_mae_change_pct"), 2, signed=True),
                        fmt(metrics.get("control_rmse_change_pct"), 2, signed=True)])

    chart = bar_chart(
        [(label[name], rmse.get(name)) for name in
         ("hypothesised_ladder", "global_only", "oracle", "blind") if name in rmse],
        unit="%",
        caption="RMSE gain over doing nothing. By MAE this ordering is exactly "
                "reversed, and that is the finding.")

    return f"""
<section id="injury">
  <h2>The injury model: fitted, walk-forwarded, and rejected</h2>
  <p>Plan 27 asks three questions an availability flag cannot: how long until he plays,
  how good he is once he does, and how likely he is to go again. The middle one is a
  fitted curve, <code>m(w) = 1 - a&middot;exp(-(w-1)/&tau;)</code>, and the global fit
  lands at <strong>a = 0.163, &tau; = 1.14</strong> &mdash; almost exactly what the plan
  predicted in writing before any code existed.</p>
  <p>It is <strong>rejected as a multiplier</strong> by the gates in
  <code>registry.py</code>, which were written first. The columns ship; nothing moves
  <code>TRUE_</code>.</p>

  <h3>Walk-forward, folds on episodes</h3>
  <p>Six appearances of one injury are one correlated observation, so folds split on
  episodes rather than weeks. The control cohort, the shrinkage strength and the
  abstention decisions are all re-derived inside each fold. Every figure is computed
  twice &mdash; <em>oracle</em> conditioning on realised duration, <em>blind</em> on body
  part alone &mdash; because at apply time duration is predicted, not observed, and only
  the blind figure faces a gate.</p>
  {table(["season", "episodes", "rows", "k", "do-nothing MAE", "blind", "oracle",
          "ladder"], fold_rows)}

  <h3>Two metrics, opposite answers</h3>
  {chart}
  {table(["candidate", "MAE gain", "RMSE gain"], metric_rows)}
  <p>Under <strong>MAE</strong> the ranking is monotone in <em>how hard each candidate
  discounts</em> &mdash; the hypothesised ladder, the most aggressive, wins outright
  &mdash; and discounting <em>healthy</em> comparables improves their MAE too. Under
  <strong>RMSE</strong> the ladder is the worst candidate and healthy comparables
  correctly get worse.</p>
  <p>The cause is structural. The prediction is a conditional <strong>mean</strong> (a
  ratio of sums) and weekly scoring is strongly right-skewed, so the conditional median
  sits well below it. MAE is minimised by the median, so it rewards <em>any</em> downward
  bias whether or not it has anything to do with injuries. RMSE is minimised by the mean,
  which is what the model estimates.</p>
  <p class="note">The gate reads MAE and has been <strong>left alone</strong> rather than
  swapped after the fact. It rejects on the false-positive clause &mdash; written
  precisely to catch "this helps healthy players too" &mdash; and fires on the right
  evidence for a slightly different reason than anticipated. Re-specifying it would not
  change the outcome: under RMSE the fitted curve gains
  {fmt(rmse.get('blind'), 2, signed=True)}% against a
  {block['gates']['MIN_POST_RETURN_MAE_GAIN_PCT']:.0f}% bar.</p>

  <h3>Well calibrated, not accurate enough</h3>
  <p>Calibration slope <strong>{metrics.get('calibration_slope')}</strong> against a
  {block['gates']['MIN_CALIBRATION_SLOPE']:.1f} floor: a cell predicted to lose 20% loses
  about 20%. The curve knows the shape. What it does not have is enough per-player
  accuracy to be worth multiplying a projection by, and those are different
  properties.</p>
  <p>The hazard fails separately, at a Brier ratio of
  <strong>{metrics.get('hazard_brier_ratio')}</strong> against
  {block['gates']['MAX_HAZARD_BRIER_RATIO']:.2f}. The weekly recurrence event is rare
  (~1% a week), so Brier is dominated by the base rate. The <em>pooled per-body-part
  rate</em> is a different quantity, judged separately, and it passes an external check
  the weekly model was never asked to: hamstring
  <strong>{fmt(metrics.get('hamstring_recurrence'), 3)}</strong> against a published
  11.9%.</p>

  <h3>Verdicts</h3>
  <ul>
    <li><strong>curve &mdash; {verdict.get('curve', '?').upper()}</strong>:
    {verdict.get('curve_reason', '')}</li>
    <li><strong>hazard &mdash; {verdict.get('hazard', '?').upper()}</strong>:
    {verdict.get('hazard_reason', '')}</li>
  </ul>
  <p class="note">Two things would change the answer and both are data rather than
  modelling. The daily ESPN injury archive started on 2026-08-18, and a season of it
  gives real severity &mdash; "Knee - ACL" where the weekly report says "Knee". The
  oracle-against-blind gap is the measured value of knowing it. And the accuracy gate
  should be re-specified on RMSE <em>before</em> the next run, not after.</p>
</section>
"""


def weekly_transfer_section(ledger: Dict) -> str:
    """Whether any of this transfers to the in-season head. Two findings invert."""
    block = ledger.get("weekly_transfer")
    if not block:
        return ""

    route_rows = [
        [entry["position"], f"{entry['n_test']:,}",
         fmt(entry["base_r2"]), fmt(entry["with_routes_r2"]),
         fmt(entry["delta"], signed=True),
         f"{entry['median_trailing_targets']:.0f}",
         f"{entry['median_trailing_routes']:.0f}"]
        for entry in block.get("routes", [])
    ]
    shrink_rows = [
        [entry["rate"], f"{entry['k']:.0f}",
         f"{entry['season_n_p95']:.0f}", f"{entry['season_prior_weight']:.1%}",
         f"{entry['weekly_n_p95']:.0f}", f"{entry['weekly_prior_weight']:.1%}"]
        for entry in block.get("shrinkage", [])
    ]
    chart = bar_chart(
        [(f"{entry['position']} (weekly)", entry["delta"])
         for entry in block.get("routes", [])],
        caption="Routes, weekly: holdout R² added on top of trailing targets. "
                "The same feature added +0.0000 mean Spearman on the season head.")

    return f"""
<section id="weekly-transfer">
  <h2>Does any of this transfer to the weekly head?</h2>
  <p>Everything above was measured on the <strong>season</strong> head. Two of the
  three failure mechanisms were arguments about sample size, and the in-season
  horizon changes the sample size by an order of magnitude — so they do not
  transfer. They <em>invert</em>.</p>

  <h3>Routes: rejected seasonally, the largest weekly effect measured here</h3>
  {chart}
  {table(["position", "n", "trailing targets only", "+ routes", "delta",
          "median t3 targets", "median t3 routes"], route_rows)}
  <p>The mechanism is in the last two columns. Over a trailing three-appearance
  window the median receiver has <strong>12 targets and 74 routes</strong> — routes
  carry roughly six times the sample per unit time, so they stabilise about two and
  a half times faster. Across a full season both are large (250+ targets, 900+
  routes) and the extra precision buys nothing, which is exactly what the season
  experiment found. Three weeks in, target counts are mostly noise and routes are
  not.</p>
  <p class="note">Plan 19 independently measured usage shares at +0.0103 for WR and
  +0.0114 for TE, and said to carry them for those two positions. The WR figure here
  lands in the same place; tight end is far better than that, because route share
  is a sharper instrument than the target share plan 19 had available.</p>

  <h3>The efficiency prior: the anti-correlation breaks</h3>
  {table(["rate", "k", "season n", "prior weight", "weekly t3 n",
          "prior weight"], shrink_rows)}
  <p>The season finding was that a fitted prior reaches the players who give it
  almost no weight, because NGS's qualifying threshold is a volume threshold and
  credibility weight falls with volume. Weekly the denominator is a three-game
  window rather than a season, so the prior carries <strong>more than double</strong>
  the weight and crosses 50% — it becomes the dominant term rather than a
  correction. And NGS coverage is best for exactly these high-volume players, so
  weekly the fitted prior would reach the rows where it now decides the number.</p>
  <p class="note">Shown at the 95th percentile of volume, not the median: the median
  rostered player is a special-teamer, and the question is about players a lineup
  actually contains.</p>

  <h3>What should still hold weekly</h3>
  <ul>
    <li><strong>Contracts</strong> — a pre-season role signal, dominated once three
    weeks of actual usage are observable. With one caveat worth testing: in weeks 1
    and 2 there <em>is</em> no trailing window, and pre-season signals are all the
    model has.</li>
    <li><strong>The ridge critique</strong> — reversion to the mean is an argument
    about what shrinkage does, not about horizon. Weekly there are also far more
    rows (42,796 player-weeks against ~6,600 player-seasons), so estimator variance
    binds even less.</li>
    <li><strong>Red-zone role</strong> — genuinely unknown, and the one worth
    testing rather than reasoning about. A trailing three-game window holds only
    about ten red-zone touches, so the sample argument that rescues routes cuts the
    other way here.</li>
  </ul>
  <p class="note">Reproduce with <code>python -m Scripts.lab.weekly</code>. These are
  feature-level measurements, not a weekly model — plan 19 is not started.</p>
</section>"""


def blend_accuracy_section(ledger: Dict) -> str:
    """The shipping blend, scored per stat -- and the one stat where it loses."""
    block = ledger.get("blend_accuracy")
    if not block:
        return ""
    season = sorted(block)[-1]
    entry = block[season]
    stats = entry["populations"]["all"]["stats"]
    sources = ("ESPN", "FP", "PINNY", "BOL")

    from Scripts.lab.accuracy import STAT_ORDER

    rows = []
    for stat in STAT_ORDER:
        entries = stats.get(stat)
        if not entries:
            continue
        cells = [stat]
        for source in sources:
            found = entries.get(source)
            cells.append(fmt(found["delta_pct"], decimals=1, signed=True) + "%"
                         if found else "—")
        rows.append(cells)

    worst = [(stat, stats[stat]["ESPN"]["delta_pct"])
             for stat in STAT_ORDER
             if stat in stats and "ESPN" in stats[stat]]
    chart = bar_chart(
        worst, unit="%", decimals=1, higher_is_better=False,
        caption="Blend versus ESPN, per stat, on the cells ESPN was real for. "
                "Negative is the blend winning — so the improvement colour is the "
                "left-hand one. One stat goes the other way.")

    defects = entry["populations"]["all"]["defects"]
    defect_rows = [[row["stat"], row["source"], f"{row['n']:,}",
                    fmt(row["delta_pct"], decimals=1, signed=True) + "%"]
                   for row in defects]

    points = entry["points"]
    point_cells = " · ".join(
        f"{k} {v:.3f}" for k, v in sorted(points["mae"].items()))

    return f"""
<section id="blend-accuracy">
  <h2>The shipping blend, scored one stat at a time</h2>
  <p>Every other scored evaluation on this page judges <strong>TOMCAT</strong>. The
  four-source blend that actually reaches the app, the Sheets and the draft board
  had never been scored against a realised stat line at all — although
  <code>lineups.parquet</code> has carried the projected line and the realised one
  side by side, for nine leagues, since 2025.</p>
  {chart}
  {table(["stat", "vs ESPN", "vs FP", "vs PINNY", "vs BOL"], rows)}
  <p>Each column is <strong>paired</strong>: the blend and that source are scored on
  the same cells, the ones the source really had a line for. Pooling instead would
  compare the blend's number on 100% of rows against FantasyPros' on 13%, which
  measures coverage and reports it as accuracy.</p>

  <h3>What the points number cannot see</h3>
  <p>Fantasy-point MAE for {esc(points['league'])}, n={points['n']:,}:
  <strong>{esc(point_cells)}</strong> — the blend beats ESPN by 2.2%, a clean win.
  Inside that win it is losing on <code>rushingTouchdowns</code> against three of
  its four inputs. Yardage carries most of the points variance, so one number cannot
  say so.</p>
  {table(["stat", "worse than", "n", "MAE delta"], defect_rows)}
  <p class="note">The bar is the lab's own
  <code>MAX_STAT_MAE_INCREASE_PCT</code> ({entry['threshold_pct']}%), applied
  mechanically. The defect survives all three populations
  (<em>all</em>, <em>team&nbsp;played</em>, <em>played</em>). Note the fourth column
  of the table above: the blend is 9.4% <em>better</em> than BetOnline on the same
  stat, which points at BetOnline's <code>anytimeTouchdown</code> split — 100% to
  rushing for QB and RB.</p>
  <p class="note">Reproduce with <code>python -m Scripts.lab.accuracy</code>.
  {entry['rows']:,} player-weeks, {len(entry['leagues'])} leagues, {esc(season)}.
  One season only: no <code>lineups</code> exists before 2025 and
  <a href="../docs/plans/25-results-backfill.md">plan 25</a> explains why none can
  be built. Cross-league agreement was checked rather than assumed — actuals and
  ESPN to 0.0000, the blend to
  {entry['worst_blend_disagreement']:.2f}.</p>
</section>"""


def persistence_section(ledger: Dict) -> str:
    """What is forecastable at all, and whether the shrinkage matches it."""
    block = ledger.get("persistence")
    if not block:
        return ""

    volume_rows = []
    for name, entry in block.get("volume", {}).items():
        strata = entry.get("strata", {})
        volume_rows.append([
            name, f"{entry['n']:,}", fmt(entry["pearson"], decimals=3),
            fmt(entry["spearman"], decimals=3),
            *(fmt(strata[s]["pearson"], decimals=3) if s in strata else "—"
              for s in ("low", "mid", "high")),
        ])

    rate_rows = []
    for name, entry in sorted(block.get("rates", {}).items(),
                              key=lambda kv: -kv[1]["pearson"]):
        shipped = entry.get("shipped_k")
        implied = entry["implied_k"]
        rate_rows.append([
            name, f"{entry['n']:,}", fmt(entry["pearson"], decimals=3),
            f"{entry['median_denominator']:.0f}", f"{implied:.0f}",
            "—" if shipped is None else f"{shipped:.0f}",
            "below floor" if shipped is not None and shipped < implied else "at/above",
        ])

    chart = bar_chart(
        [(name, entry["pearson"])
         for name, entry in sorted(block.get("rates", {}).items(),
                                   key=lambda kv: -kv[1]["pearson"])],
        unit="", decimals=3,
        caption="Year-over-year persistence of each efficiency rate. Higher is more "
                "forecastable. The touchdown rates are the least forecastable "
                "quantities measured here.")

    return f"""
<section id="persistence">
  <h2>What persists, and what the model shrinks</h2>
  <p>The question underneath the rushing-touchdown defect above is not which source
  should carry more weight. It is <strong>how much of a touchdown rate is
  forecastable at all</strong> — because averaging four sources helps where they
  carry independent signal, and there is very little signal here to be independent
  about.</p>

  <h3>Volume persists</h3>
  {table(["quantity", "n", "Pearson", "Spearman", "low third", "mid", "high third"],
         volume_rows)}
  <p>Terciles are of the <em>prior</em> season, which is the information a
  projection has. Two readings worth keeping: a low-volume back's carries carry
  almost nothing forward (+0.084), and <strong>a quarterback's attempt rate is
  nearly unpredictable among established starters</strong> — +0.093 in the top third
  against +0.540 pooled. The pooled figure is mostly separating starters from
  backups, the same shape plan 18 found for games played.</p>

  <h3>Efficiency barely does</h3>
  {chart}
  {table(["rate", "n", "Pearson", "median denominator", "implied k", "shipped k",
          "verdict"], rate_rows)}
  <p><code>implied k</code> inverts the credibility identity
  <code>n/(n+k) = r</code> at the median denominator. It is a <strong>floor</strong>:
  it assumes the underlying rate is perfectly stable, so any genuine drift only
  raises it. <strong>Every shipped constant sits below its floor</strong>, by 1.4×
  for catch rate to 4.6× for yards per attempt — the model shrinks efficiency too
  little, across the board.</p>
  <p class="note">Reproduce with <code>python -m Scripts.lab.persistence</code>.
  {block['pairs']:,} consecutive player-season pairs,
  {block['seasons'][0]}–{block['seasons'][1]}. Pairs join on
  <code>season + 1</code> and are never shifted, so a missed season contributes no
  pair across the gap. Nothing here is fitted or predicted; it is a description of
  ten seasons that feeds decisions rather than models.</p>
</section>"""


def prior_negatives_section() -> str:
    """Everything measured and rejected before this plan."""
    rows = [
        [esc(item["name"]), esc(item["measured"]), esc(item["why"]),
         f'<code>{esc(item["where"])}</code>']
        for item in reg.PRIOR_NEGATIVES
    ]
    return f"""
<section id="prior-negatives">
  <h2>Measured and rejected before this plan</h2>
  <p>Transcribed from plans 18 and 21 so this page is the single place to look. A
  negative result that lives only in a commit message gets rediscovered, and
  rediscovering one costs a day plus the chance of concluding the opposite from a
  smaller sample.</p>
  {table(["feature", "what was measured", "why it was rejected", "working"],
         rows, 99)}
</section>"""


def sources_section() -> str:
    """What the data can and cannot do."""
    rows = [
        ["Participation → routes", "2016–2025", "98–100% of players with a prior season",
         "Derived by joining <code>offense_players</code> to dropback plays. There is "
         "no routes column in public data; being on the field for a dropback is the "
         "standard approximation."],
        ["Next Gen Stats", "2016–2025", "35–75% of the relevant population",
         "The binding constraint on the efficiency work. NGS only publishes players "
         "who met its qualifying threshold, so the fitted baseline falls back to the "
         "positional constant for a large minority of rows."],
        ["Red zone (play-by-play)", "2016–2025", "79–96%",
         "Carries and targets inside the 20/10/5 plus end-zone targets, with team "
         "totals for shares."],
        ["Contracts (OverTheCap)", "signings 1992–2026", "98.1% of rostered players",
         "The only forward-looking evidence available about a player who changed "
         "teams. <code>apy_cap_pct</code> rather than dollars, so a decade of cap "
         "inflation does not become a coefficient."],
        ["PFR advanced", "2018–2025", "not pulled",
         "Two seasons short of the training window and largely duplicates NGS. Set "
         "aside on coverage, not on merit."],
        ["FTN charting", "2022–2025", "not pulled",
         "Motion, play action, RPO. Four seasons is thin for a walk-forward that "
         "trains from 2016."],
        ["NGS passing", "2016–2025", "<strong>not pulled — a gap, not a decision</strong>",
         "Completion percentage over expected is the obvious regressor for the "
         "quarterback efficiency rates, which are the only rates with no fitted "
         "baseline. Untested."],
    ]
    return f"""
<section id="sources">
  <h2>Data sources and their limits</h2>
  {table(["source", "seasons", "coverage", "notes"], rows, 99)}
  <p class="note">All pulled by <code>Rscript R/GetAdvanced.R</code>, which writes
  per-season parquet beside the existing nflverse pulls. Everything is regenerable
  and gitignored.</p>
</section>"""


MIN_GAIN = reg.MIN_MEAN_SPEARMAN_GAIN
MAX_LOSS = reg.MAX_POSITION_SPEARMAN_LOSS
MAX_REG = reg.MAX_TOP_N_REGRESSIONS

STYLE = f"""
:root {{
  color-scheme: light;
  --plane: #f9f9f7;
  --surface: #fcfcfb;
  --ink: #0b0b0b;
  --ink-2: #52514e;
  --muted: #898781;
  --grid: #e1e0d9;
  --axis: #c3c2b7;
  --border: rgba(11,11,11,0.10);
  --positive: {POSITIVE_LIGHT};
  --negative: {NEGATIVE_LIGHT};
}}
@media (prefers-color-scheme: dark) {{
  :root:where(:not([data-theme="light"])) {{
    color-scheme: dark;
    --plane: #0d0d0d; --surface: #1a1a19; --ink: #ffffff; --ink-2: #c3c2b7;
    --muted: #898781; --grid: #2c2c2a; --axis: #383835;
    --border: rgba(255,255,255,0.10);
    --positive: {POSITIVE_DARK}; --negative: {NEGATIVE_DARK};
  }}
}}
:root[data-theme="dark"] {{
  color-scheme: dark;
  --plane: #0d0d0d; --surface: #1a1a19; --ink: #ffffff; --ink-2: #c3c2b7;
  --muted: #898781; --grid: #2c2c2a; --axis: #383835;
  --border: rgba(255,255,255,0.10);
  --positive: {POSITIVE_DARK}; --negative: {NEGATIVE_DARK};
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0; padding: 2rem 1.25rem 5rem;
  background: var(--plane); color: var(--ink);
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif;
  font-size: 15px; line-height: 1.6;
}}
main {{ max-width: 60rem; margin: 0 auto; }}
h1 {{ font-size: 1.9rem; line-height: 1.2; margin: 0 0 .4rem; }}
h2 {{ font-size: 1.3rem; margin: 2.6rem 0 .6rem;
     padding-top: 1.4rem; border-top: 1px solid var(--border); }}
h3 {{ font-size: 1.05rem; margin: 1.6rem 0 .5rem; }}
h4 {{ font-size: .95rem; margin: 1.2rem 0 .4rem; color: var(--ink-2); }}
p {{ margin: .6rem 0; }}
code {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .88em; }}
a {{ color: inherit; text-decoration-color: var(--axis); }}
.lede {{ color: var(--ink-2); font-size: 1.05rem; }}
.note {{ color: var(--ink-2); font-size: .9rem; }}
.muted {{ color: var(--muted); }}
.hypothesis {{ color: var(--ink-2); }}
.scroll {{ overflow-x: auto; margin: .8rem 0; }}
table {{ border-collapse: collapse; width: 100%; font-size: .88rem;
         background: var(--surface); }}
th, td {{ padding: .42rem .7rem; text-align: left; vertical-align: top;
          border-bottom: 1px solid var(--grid); }}
th {{ color: var(--ink-2); font-weight: 600; white-space: nowrap; }}
td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums;
                  white-space: nowrap; }}
.experiment {{ background: var(--surface); border: 1px solid var(--border);
               border-radius: 10px; padding: 1rem 1.2rem; margin: 1.2rem 0; }}
.experiment h3 {{ margin-top: .2rem; }}
.charts {{ display: flex; flex-wrap: wrap; gap: 1.5rem; margin: 1rem 0; }}
figure {{ margin: 0; }}
.caption {{ font-size: .84rem; color: var(--ink-2); margin: 0 0 .3rem;
            max-width: 34rem; }}
.chart {{ max-width: 100%; height: auto; }}
.chart .tick {{ font-size: 11px; fill: var(--ink-2);
                font-variant-numeric: tabular-nums; }}
.chart .muted {{ font-size: 11px; fill: var(--muted); }}
.chart .axis {{ stroke: var(--axis); stroke-width: 1; }}
.badge {{ font-size: .72rem; text-transform: uppercase; letter-spacing: .04em;
          padding: .1rem .45rem; border-radius: 4px; border: 1px solid var(--border);
          color: var(--ink-2); vertical-align: middle; margin-left: .4rem; }}
.badge.merge {{ border-color: var(--positive); color: var(--positive); }}
.badge.reject {{ border-color: var(--negative); color: var(--negative); }}
.verdict {{ font-size: .9rem; padding-top: .6rem; border-top: 1px solid var(--grid); }}
ol, ul {{ padding-left: 1.3rem; }}
"""


def render(ledger: Dict, headline: str = "") -> str:
    """Assemble the page.

    Args:
        results: The ledger's ``experiments`` mapping.
        headline: One-paragraph summary for the top of the page.

    Returns:
        str: A complete HTML document.
    """
    results = ledger["experiments"]
    base = results["baseline"]
    experiments = "".join(
        experiment_section(name, entry, base)
        for name, entry in sorted(results.items()) if name != "baseline")
    stamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Season usage model — feature lab</title>
<style>{STYLE}</style>
</head>
<body>
<main>
  <h1>Season usage model — feature lab</h1>
  <p class="lede">{headline}</p>
  <p class="note">Generated by <code>python -m Scripts.lab.report</code> from
  <code>Scripts/lab/results.json</code> on {esc(stamp)}. Do not edit by hand —
  re-run it.</p>
  {baseline_section(base)}
  {rule_section()}
  {ledger_section(results)}
  {ridge_sweep_section(results)}
  <section id="experiments">
    <h2>Experiments in full</h2>
    {experiments}
  </section>
  {blend_accuracy_section(ledger)}
  {persistence_section(ledger)}
  {weekly_transfer_section(ledger)}
  {injury_section(ledger)}
  {blend_section(ledger)}
  {prior_negatives_section()}
  {sources_section()}
</main>
</body>
</html>
"""


DEFAULT_HEADLINE = (
    "A running record of what the season projection model is, what has been tried "
    "on it, and what happened. Negative results are the point: most of this page "
    "is things that did not work, kept so they are not tried twice."
)


def main(argv: Optional[List[str]] = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        prog="python -m Scripts.lab.report",
        description="Render the feature-research ledger to HTML.")
    parser.add_argument("--out", default=str(OUTPUT_PATH))
    parser.add_argument("--headline", default=DEFAULT_HEADLINE)
    args = parser.parse_args(argv)

    if not RESULTS_PATH.is_file():
        parser.error(f"No ledger at {RESULTS_PATH}. Run `python -m Scripts.lab.run "
                     f"--all` first.")
    with RESULTS_PATH.open() as handle:
        ledger = json.load(handle)
    if "baseline" not in ledger.get("experiments", {}):
        parser.error("The ledger has no baseline run to compare against.")

    from pathlib import Path
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(ledger, args.headline))
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
