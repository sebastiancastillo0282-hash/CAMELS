from __future__ import annotations

from camels.dashboard import run


def test_dashboard_stage_runs_without_crashing(stage_context) -> None:
    # The dashboard stage currently logs placeholder output; invoking it should not raise errors.
    run(stage_context)
