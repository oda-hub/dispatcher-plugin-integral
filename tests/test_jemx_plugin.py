import pytest
import logging
import requests
import json
import time
import jwt

from cdci_data_analysis.pytest_fixtures import loop_ask, ask

logger = logging.getLogger(__name__)

default_params = dict(instrument='jemx',
                        query_status="new",
                        jemx_num='2',
                        product_type='jemx_image',
                        scw_list=['010200230010.001'],
                        osa_version='OSA10.2',
                        detection_threshold=5.0,
                        radius=15.,
                        query_type='Real')

secret_key = 'secretkey_test'
dummy_params = dict(
    query_status="new",
    query_type="Dummy",
    instrument="jemx",
    scw_list="066500220010.001",
    async_dispatcher=False
)

default_exp_time = int(time.time()) + 5000
default_token_payload = dict(
    sub="mtm@mtmco.net",
    name="mmeharga",
    roles="general",
    exp=default_exp_time,
    tem=0,
    mstout=True,
    mssub=True
)


def test_default(dispatcher_live_fixture):
    server = dispatcher_live_fixture


@pytest.mark.jemx_plugin
@pytest.mark.dependency(depends=["test_default"])
def test_jemx_dummy(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    params = {
        **dummy_params,
        "product_type": "jemx_image"
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='done', expected_job_status='done', max_time_s=5)
    logger.info(list(jdata.keys()))
    logger.info(jdata)


# TODO are those parameters ok? I am sure the values are correct or the tests are properly set
@pytest.mark.jemx_plugin
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.xfail
def test_jemx_products(dispatcher_live_fixture, product_type):
    server = dispatcher_live_fixture

    params = {
        **default_params,
        "product_type": product_type
    }

    logger.info("constructed server: %s", server)
    c = requests.get(server + "/run_analysis",
                      params=params)

    logger.info("content: %s", c.text)
    jdata = c.json()
    logger.info(list(jdata.keys()))
    logger.info(jdata)
    assert c.status_code == 200
    assert jdata['job_status'] == 'done'


@pytest.mark.jemx_plugin
@pytest.mark.jemx_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
def test_jemx_dummy_many_pointings(dispatcher_live_fixture, product_type):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": 100,
        "integral_data_rights": "public",
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='failed',
                expected_job_status='failed', max_time_s=50, expected_status_code=403)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] \
           == f"Roles [] not authorized to request the product {product_type}, ['unige-hpc-full'] roles are needed"

    params = {
        **dummy_params,
        "instrument": "jemx",
        "product_type": product_type,
        "max_pointings": 10,
        "integral_data_rights": "public",
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='done',
                expected_job_status='done', max_time_s=50, expected_status_code=200)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    params = {
        **dummy_params,
        "instrument": "jemx",
        "product_type": product_type,
        "max_pointings": 10,
        "integral_data_rights": "all-private",
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='failed',
                expected_job_status='failed', max_time_s=50, expected_status_code=403)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] \
           == f"Roles [] not authorized to request the product {product_type}, ['integral-private'] roles are needed"


@pytest.mark.jemx_plugin
@pytest.mark.jemx_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
def test_jemx_dummy_roles_private_data(dispatcher_live_fixture, product_type):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token without roles assigned
    token_payload = {
        **default_token_payload,
        "roles": []
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": 100,
        "token": encoded_token,
        "integral_data_rights": "all-private"
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='failed',
                expected_job_status='failed', max_time_s=50, expected_status_code=403)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] == f"Roles [] not authorized to request the product {product_type}, ['unige-hpc-full', 'integral-private'] roles are needed"

    # let's generate a valid token with roles added
    token_payload = {
        **default_token_payload,
        "roles": ['unige-hpc-full', 'integral-private']
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')
    params = {
        **params,
        "token": encoded_token
    }
    jdata = ask(server, params, expected_query_status='done',
                expected_job_status='done', max_time_s=50, expected_status_code=200)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] == ""


@pytest.mark.jemx_plugin
@pytest.mark.jemx_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
def test_jemx_dummy_roles_public_data(dispatcher_live_fixture, product_type):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token without roles assigned
    token_payload = {
        **default_token_payload,
        "roles": []
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": 10,
        "token": encoded_token
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='done',
                expected_job_status='done', max_time_s=50, expected_status_code=200)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] == ""

    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": 100,
        "token": encoded_token
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='failed',
                expected_job_status='failed', max_time_s=50, expected_status_code=403)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] == f"Roles [] not authorized to request the product {product_type}, ['unige-hpc-full'] roles are needed"