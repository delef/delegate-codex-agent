import importlib
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class PackageRenameTests(unittest.TestCase):
    def test_orchestrator_package_and_public_skill_name_are_available(self):
        package = importlib.import_module("orchestrator_agent")
        self.assertEqual(package.__name__, "orchestrator_agent")
        skill = (ROOT / "SKILL.md").read_text(encoding="utf-8")
        self.assertIn("name: orchestrator-codex-agent", skill)
        self.assertIn("$orchestrator-codex-agent", (ROOT / "agents" / "openai.yaml").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
