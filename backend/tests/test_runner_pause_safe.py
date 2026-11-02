"""N9.13: the batch timeout counts active time, not time the machine slept."""
from __future__ import annotations

import subprocess

from backend.scanner.pipeline import runner
from backend.scanner.pipeline.active_time import ActiveTimer


class _Clock:
    def __init__(self) -> None:
        self.t = 1000.0

    def __call__(self) -> float:
        return self.t


def test_active_timer_does_not_charge_a_pause():
    clk = _Clock()
    timer = ActiveTimer(max_step=30, mono=clk, wall=clk)
    clk.t += 10
    assert timer.tick() == 10
    clk.t += 78_000  # Mac asleep (or SIGSTOP): one huge gap between ticks
    assert timer.tick() == 40  # only max_step of it is charged
    assert round(timer.paused) == 78_000 - 30
    clk.t += 5
    assert timer.tick() == 45
    assert timer.stats() == {"wall_seconds": 78_015.0, "active_seconds": 45.0}


def _fake_popen(clk, script):
    """Popen whose wait() advances the fake clock by the next scripted step, then times out
    (or returns rc when the script is exhausted)."""
    class _P:
        killed = False

        def __init__(self, *a, **k):
            self.steps = list(script)

        def wait(self, timeout=None):
            if _P.killed:
                return -9
            if not self.steps:
                return 0
            clk.t += self.steps.pop(0)
            if not self.steps:
                return 0
            raise subprocess.TimeoutExpired("scanner", timeout)

        def kill(self):
            _P.killed = True
    return _P


def _run(monkeypatch, tmp_path, script, timeout_sec):
    clk = _Clock()
    monkeypatch.setattr(runner, "ActiveTimer", lambda: ActiveTimer(mono=clk, wall=clk))
    monkeypatch.setattr(subprocess, "Popen", _fake_popen(clk, script))
    stats: dict = {}
    rc = runner.run_http_only_scan(["a-com"], concurrency=1, log_path=tmp_path / "s.log", timeout_sec=timeout_sec, stats=stats)
    return rc, stats


def test_simulated_pause_longer_than_timeout_is_not_charged(monkeypatch, tmp_path):
    # 1 s polls, then a 21.7 h sleep, then more polls; timeout is 600 s of active time
    rc, stats = _run(monkeypatch, tmp_path, [1, 1, 78_000, 1, 1, 1], timeout_sec=600)
    assert rc == 0
    assert stats["wall_seconds"] > 78_000 and stats["active_seconds"] < 100


def test_real_overrun_still_kills(monkeypatch, tmp_path):
    rc, stats = _run(monkeypatch, tmp_path, [20] * 40, timeout_sec=600)
    assert rc == -9
    assert 600 <= stats["active_seconds"] <= 640
