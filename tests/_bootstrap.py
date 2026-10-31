"""Make the plugin importable as the ``mqtt`` package under any checkout name.

Hermes loads the plugin from a directory called ``mqtt``, but CI — and anyone who
clones this repository — gets a directory named after the repo. Registering the
package with an explicit ``__path__`` keeps ``import mqtt.*`` working in both
layouts without a symlink or a renamed checkout.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

if ROOT.name == "mqtt":
    sys.path.insert(0, str(ROOT.parent))
elif "mqtt" not in sys.modules:
    package = types.ModuleType("mqtt")
    package.__path__ = [str(ROOT)]  # type: ignore[attr-defined]
    sys.modules["mqtt"] = package
