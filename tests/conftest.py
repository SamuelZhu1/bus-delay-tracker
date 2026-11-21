import os

import pytest

os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")
os.environ.setdefault("AWS_DEFAULT_REGION", "ca-central-1")


@pytest.fixture(scope="session")
def sim_day(tmp_path_factory):
    """A small simulated morning (10 routes, 7-9 AM) shared by tests."""
    from busdelay.sim import simulate

    out = tmp_path_factory.mktemp("sim")
    return simulate(str(out), n_routes=10, start_h=7, end_h=9, seed=3)
