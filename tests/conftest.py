import os
from pathlib import Path

import pytest

import DIRAC


# Adds the --runslow command line arg based on the example in the docs
# https://docs.pytest.org/en/stable/example/simple.html
# #control-skipping-of-tests-according-to-command-line-option
def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", default=False, help="run slow tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


def check_environment():
    """Ensure the environment is safe for running tests"""
    dirac_cfg = Path(DIRAC.rootPath) / "etc" / "dirac.cfg"
    if dirac_cfg.exists():
        raise EnvironmentError(f"{dirac_cfg}")
    user_proxy_path = Path("/tmp") / f"x509up_u{os.getuid()}"
    if user_proxy_path.exists():
        raise EnvironmentError(f"Found possible proxy file at {user_proxy_path}")
    if "X509_USER_PROXY" in os.environ:
        raise EnvironmentError(f"X509_USER_PROXY is set")


check_environment()
