import subprocess

from cdci_data_analysis.pytest_fixtures import (
            app,
            dispatcher_live_fixture,
            dispatcher_live_fixture_no_debug_mode,
            dispatcher_test_conf,
            dispatcher_test_conf_fn,
            dispatcher_long_living_fixture,
            dispatcher_local_mail_server,
            dispatcher_debug,
            dispatcher_nodebug,
            cleanup
        )

import pytest
import os
import yaml

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


@pytest.fixture
def test_data_server_conf_fn(monkeypatch):
    env_conf_file_path = "./tests/temp_conf/test_data_server_conf.yaml"
    monkeypatch.setenv('CDCI_OSA_PLUGIN_CONF_FILE', env_conf_file_path)
    
    folder_path = os.path.dirname(env_conf_file_path)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    with open(env_conf_file_path, "w") as f:
        f.write("""
    instruments:
      isgri:
          dispatcher_mnt_point: .
          data_server_cache: reduced/ddcache
          # this is for now, for testing purposes, but an appropriate fixture should be defined
          dummy_cache: data/dummy_prods
          data_server_url: http://localhost:1234
          data_server_port:
    
      jemx:
          dispatcher_mnt_point: .
          data_server_cache: reduced/ddcache
          dummy_cache: data/dummy_prods_jemx
          data_server_port:
          data_server_url: http://localhost:1234
    
        """)

    yield env_conf_file_path


@pytest.fixture
def mock_isgri_dda_server(test_data_server_conf_fn):

    start_mock_dda_server('isgri', test_data_server_conf_fn)

    yield test_data_server_conf_fn


@pytest.fixture
def mock_jemx_dda_server(test_data_server_conf_fn):
    start_mock_dda_server('jemx', test_data_server_conf_fn)


def start_mock_dda_server(instrument, config_path):
    data_server_url = None

    with open(config_path, 'r') as ymlfile:
        test_config = yaml.load(ymlfile)

    if test_config is not None:
        data_server_url = test_config['instruments'][instrument]['data_server_url']

    if data_server_url is not None:
        from flask import Flask
        from urllib.parse import urlparse
        from threading import Thread

        parse_result = urlparse(data_server_url)

        app = Flask(__name__)

        @app.route("/api/<api_version>/poke", methods=['POST', 'GET'])
        def poke(api_version):
            return f"Works! api {api_version}"

        def mock_flask_thread():
            app.run(host=parse_result.hostname, port=parse_result.port)

        thread = Thread(target=mock_flask_thread, args=())
        thread.setDaemon(daemonic=True)
        thread.start()

        return thread
