"""Regression: lazy db/__init__.py must not load listings on config import."""

import subprocess
import sys
import unittest


class DbLazyImportTest(unittest.TestCase):
    def test_import_config_does_not_load_queries_listings(self):
        script = (
            "import sys\n"
            "import spejder.config\n"
            "assert 'spejder.db.queries_listings' not in sys.modules\n"
            "from spejder.db import get_relevant_jobs\n"
            "assert callable(get_relevant_jobs)\n"
            "import spejder.db as db\n"
            "assert 'get_relevant_jobs' in dir(db)\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)


if __name__ == "__main__":
    unittest.main()
