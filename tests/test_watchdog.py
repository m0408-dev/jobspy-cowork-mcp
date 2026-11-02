"""Exercise watchdog decisions without touching a real Docker service."""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


@unittest.skipIf(os.name == 'nt' or not shutil.which('sh'), 'POSIX deployment test')
class WatchdogTests(unittest.TestCase):
    def test_starting_is_not_restarted_but_unhealthy_is(self):
        script = Path(__file__).resolve().parents[1] / 'deploy' / 'jobspy-watchdog.sh'
        # Production image contains the app; the deployment directory is
        # mounted for this test by the release verification command.
        self.assertTrue(script.is_file())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            fake = root / 'docker'
            fake.write_text('#!/bin/sh\nif [ "$1" = inspect ]; then printf "%s\\n" "$TEST_STATE"; else printf "%s\\n" "$*" >> "$TEST_LOG"; fi\n')
            fake.chmod(0o755)
            log = root / 'calls'
            for state, restart in [('running starting', False), ('running healthy', False),
                                   ('running unhealthy', True), ('exited unhealthy', True),
                                   ('paused healthy', False), ('restarting unhealthy', False)]:
                with self.subTest(state=state):
                    log.write_text('')
                    result = subprocess.run(['sh', str(script)], env={**os.environ,
                        'PATH': str(root) + os.pathsep + os.environ['PATH'],
                        'TEST_STATE': state, 'TEST_LOG': str(log)}, timeout=5)
                    self.assertEqual(result.returncode, 0)
                    self.assertEqual(bool(log.read_text()), restart)
