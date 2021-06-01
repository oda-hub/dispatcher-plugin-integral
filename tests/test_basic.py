import logging
import requests

logger = logging.getLogger(__name__)


def test_empty_request(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    c=requests.get(server + "/run_analysis",
                   params={},
                )

    print("content:", c.text)

    jdata=c.json()

    assert c.status_code == 400

     # parameterize this
    assert jdata['installed_instruments'] == ['empty', 'empty-async', 'empty-semi-async', 'isgri', 'jemx', 'osa_fake']

    assert jdata['debug_mode'] == "yes"
    assert 'dispatcher-config' in jdata['config']

    dispatcher_config = jdata['config']['dispatcher-config']

    assert 'origin' in dispatcher_config

    assert 'sentry_url' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_port' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_host' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'secret_key' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'smtp_server_password' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'products_url' in dispatcher_config['cfg_dict']['dispatcher']

    logger.info(jdata['config'])


def test_no_debug_mode_empty_request(dispatcher_live_fixture_no_debug_mode):
    server = dispatcher_live_fixture_no_debug_mode
    print("constructed server:", server)

    c=requests.get(server + "/run_analysis",
                   params={},
                )

    print("content:", c.text)

    jdata=c.json()

    assert c.status_code == 400

     # parameterize this
    assert jdata['installed_instruments'] == ['isgri', 'jemx']

    assert jdata['debug_mode'] == "no"
    assert 'dispatcher-config' in jdata['config']

    dispatcher_config = jdata['config']['dispatcher-config']

    assert 'origin' in dispatcher_config

    assert 'sentry_url' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_port' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_host' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'secret_key' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'smtp_server_password' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'products_url' in dispatcher_config['cfg_dict']['dispatcher']

    logger.info(jdata['config'])
