"""The manifest's Hermes gate is a floor.

Hermes checks ``requires_hermes`` before importing a plugin and skips it when the spec
rejects the running version (``hermes_cli.plugins_loader.hermes_fit_error``). An upper
bound therefore turns every newer Hermes minor into a silent outage — ``<0.23`` did
exactly that on 0.23.1 — so the declared range must stay a floor.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

PLUGIN_YAML = Path(__file__).resolve().parents[1] / "plugin.yaml"


class RequiresHermesTests(unittest.TestCase):
    def test_requires_hermes_is_a_floor_not_a_ceiling(self):
        text = PLUGIN_YAML.read_text(encoding="utf-8")
        match = re.search(r'^requires_hermes:\s*["\']?([^"\'\s#]+)', text, re.MULTILINE)
        self.assertIsNotNone(match, "plugin.yaml must declare requires_hermes")
        spec = match.group(1)
        self.assertNotIn(
            "<",
            spec,
            "requires_hermes must stay a floor: Hermes skips the plugin before import when the "
            "range rejects the running version, so a ceiling breaks every future minor release",
        )
        self.assertRegex(spec, r"^>=\d+(?:\.\d+){0,2}$", "requires_hermes must be a >= floor")
