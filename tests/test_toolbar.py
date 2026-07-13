import importlib.util
from pathlib import Path
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "delegate-status.py"
SPEC = importlib.util.spec_from_file_location("delegate_status", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class ToolbarTests(unittest.TestCase):
    def test_render_status_is_compact_and_contains_budget_and_current_task(self):
        value = {
            "name": "demo", "state": "running", "active": [{"id": "inspect", "model": "luna", "retry_count": 1}],
            "pending": 2, "blocked": 1, "next": "next", "budget": {"used_tokens": 3, "limit_tokens": 10, "reserved_tokens": 2},
        }
        rendered = MODULE.render_status(value)
        self.assertIn("demo: running", rendered)
        self.assertIn("task=inspect", rendered)
        self.assertIn("budget=3/10", rendered)
        self.assertIn("retry=1", rendered)
        self.assertLessEqual(len(MODULE.render_status(value, width=20)), 20)


if __name__ == "__main__":
    unittest.main()
