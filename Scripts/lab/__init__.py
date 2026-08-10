"""The feature-research lab: run an experiment, record it, render the ledger.

Separate from ``Scripts/usage`` because it is a different kind of code. The model
package is shipped: it runs before a draft and its output prices real decisions.
This package is a laboratory notebook that happens to execute -- it exists so that
"we tried X and it did not work" is a row in a file rather than a memory, and so
that the same claim can be re-run a year later against a season nobody had yet.

Three modules:

* :mod:`Scripts.lab.registry` -- what an experiment *is*, and the decision rule that
  says whether it passed. The rule is written down before the experiments run.
* :mod:`Scripts.lab.run` -- executes one against the real walk-forward in
  ``Scripts.usage.backtest`` rather than a reimplementation of it.
* :mod:`Scripts.lab.report` -- renders ``docs/model_lab.html``.

See ``docs/plans/22-feature-research.md``.
"""
