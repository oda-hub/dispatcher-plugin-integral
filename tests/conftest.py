from cdci_data_analysis.pytest_fixtures import (
            app,
            dispatcher_live_fixture,
            dispatcher_test_conf,
            dispatcher_test_conf_fn,
            dispatcher_long_living_fixture,
            dispatcher_debug,
            cleanup
        )

import pytest

def pytest_addoption(parser):
    parser.addoption(
        "--runslow", action="store_true", default=False, help="run slow tests"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "dda" in item.keywords:
            item.add_marker(skip_slow)

