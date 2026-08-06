"""Put the repo root on ``sys.path`` so ``Scripts.*`` imports resolve.

``streamlit run app/main.py`` sets ``sys.path[0]`` to ``app/``, not the repo root,
so every ``from Scripts.x import ...`` in this package would raise
``ModuleNotFoundError`` without this. Import it first from any module under
``app/`` -- including page scripts, which Streamlit executes on their own::

    import _bootstrap  # noqa: F401

Kept as a module rather than repeated inline so there is one place that knows
where the repo root is.
"""

import sys
from pathlib import Path

#: app/_bootstrap.py -> app/ -> repo root
REPO_ROOT = Path(__file__).resolve().parent.parent

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
