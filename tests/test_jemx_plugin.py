import pytest
import logging
import requests
import json
import time
import jwt

from cdci_data_analysis.pytest_fixtures import loop_ask, ask, dispatcher_fetch_dummy_products

logger = logging.getLogger(__name__)

default_params = dict(instrument='jemx',
                        query_status="new",
                        jemx_num='2',
                        product_type='jemx_image',
                        scw_list=['066500230010.001'],
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
@pytest.mark.parametrize('dummy_pack', ['default', 'empty'])
def test_jemx_dummy(dispatcher_live_fixture, dummy_pack):
    dispatcher_fetch_dummy_products(dummy_pack)

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

    logger.info(jdata["products"]["catalog"])

    if dummy_pack == "empty":
        assert len(jdata["products"]["catalog"]["cat_column_list"][0]) == 0
    else:
        #TODO:
        #assert len(jdata["products"]["catalog"]["cat_column_list"][0]) > 0
        pass
        


# TODO are those parameters ok? I am sure the values are correct or the tests are properly set
@pytest.mark.dda
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
@pytest.mark.parametrize("max_pointings", [10, 100])
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
def test_jemx_dummy_data_rights(dispatcher_live_fixture, product_type, max_pointings):
    dispatcher_fetch_dummy_products("default")

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": max_pointings,
        "integral_data_rights": "public",
    }

    if max_pointings > 50:
        expected_status_code = 403
        expected_status = 'failed'
        exit_status_message = f"Roles [] not authorized to request the product {product_type}, ['unige-hpc-full'] roles are needed"
    else:
        expected_status_code = 200
        expected_status = 'done'
        exit_status_message = ""

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status=expected_status,
                expected_job_status=expected_status, max_time_s=50, expected_status_code=expected_status_code)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] == exit_status_message

    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": max_pointings,
        "integral_data_rights": "all-private",
    }

    if max_pointings > 50:
        exit_status_message = f"Roles [] not authorized to request the product {product_type}, ['unige-hpc-full', 'integral-private'] roles are needed"
    else:
        exit_status_message = f"Roles [] not authorized to request the product {product_type}, ['integral-private'] roles are needed"

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='failed',
                expected_job_status='failed', max_time_s=50, expected_status_code=403)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] == exit_status_message


@pytest.mark.jemx_plugin
@pytest.mark.jemx_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
@pytest.mark.parametrize("roles", [[], ["integral-private"]])
def test_jemx_dummy_roles_private_data(dispatcher_live_fixture, product_type, roles):
    dispatcher_fetch_dummy_products("default")

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token without roles assigned
    token_payload = {
        **default_token_payload,
        "roles": roles
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": 10,
        "token": encoded_token,
        "integral_data_rights": "all-private"
    }

    if not roles:
        expected_status_code = 403
        expected_status = 'failed'
        exit_status_message = f"Roles [] not authorized to request the product {product_type}, ['integral-private'] roles are needed"
    else:
        expected_status_code = 200
        expected_status = 'done'
        exit_status_message = ""

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status=expected_status,
                expected_job_status=expected_status, max_time_s=50, expected_status_code=expected_status_code)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] == exit_status_message


@pytest.mark.jemx_plugin
@pytest.mark.jemx_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
@pytest.mark.parametrize("roles", [[], ["unige-hpc-full"]])
def test_jemx_dummy_roles_public_data(dispatcher_live_fixture, product_type, roles):
    dispatcher_fetch_dummy_products("default")

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    # let's generate a valid token without roles assigned
    token_payload = {
        **default_token_payload,
        "roles": roles
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": 100,
        "token": encoded_token,
        "integral_data_rights": "public"
    }

    if not roles:
        expected_status_code = 403
        expected_status = 'failed'
        exit_status_message = f"Roles [] not authorized to request the product {product_type}, ['unige-hpc-full'] roles are needed"
    else:
        expected_status_code = 200
        expected_status = 'done'
        exit_status_message = ""

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status=expected_status,
                expected_job_status=expected_status, max_time_s=50, expected_status_code=expected_status_code)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] == exit_status_message