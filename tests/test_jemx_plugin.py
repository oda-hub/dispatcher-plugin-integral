import pytest
import logging
import requests
import json
import time
import jwt
import os

from cdci_data_analysis.pytest_fixtures import loop_ask, ask, dispatcher_fetch_dummy_products

logger = logging.getLogger(__name__)

default_params = dict(instrument='jemx',
                        query_status="new",
                        jemx_num='1',
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


@pytest.mark.dda
@pytest.mark.jemx_plugin
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
@pytest.mark.dependency(depends=["test_default"])
def test_jemx_products(dispatcher_live_fixture, product_type):
    server = dispatcher_live_fixture

    params = {
        **default_params,
        "product_type": product_type,
        "swc_list": [f"0665{i:04d}0010.001" for i in range(10)]
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
@pytest.mark.parametrize("scw_list_size", [10, 100])
@pytest.mark.parametrize("integral_data_rights", [None, "public", "all-private"])
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
def test_jemx_dummy_data_rights(dispatcher_live_fixture, product_type, max_pointings, integral_data_rights, scw_list_size):
    dispatcher_fetch_dummy_products("default")

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": max_pointings,
        "integral_data_rights": integral_data_rights,
        "scw_list": [f"0665{i:04d}0010.001" for i in range(scw_list_size)]
    }

    if max_pointings > 50 or scw_list_size > 50:
        expected_status_code = 403
        expected_status = 'failed'
        if integral_data_rights == "public" or integral_data_rights is None:
            exit_status_message = f"Roles [] not authorized to request the product {product_type}, ['unige-hpc-full'] roles are needed"
        elif integral_data_rights == "all-private":
            exit_status_message = f"Roles [] not authorized to request the product {product_type}, ['unige-hpc-full', 'integral-private'] roles are needed"
    else:
        if integral_data_rights == "public" or integral_data_rights is None:
            expected_status_code = 200
            expected_status = 'done'
            exit_status_message = ""
        elif integral_data_rights == "all-private":
            expected_status_code = 403
            expected_status = 'failed'
            exit_status_message = f"Roles [] not authorized to request the product {product_type}, ['integral-private'] roles are needed"

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status=expected_status,
                expected_job_status=expected_status, max_time_s=50, expected_status_code=expected_status_code)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'] == exit_status_message


def validate_product(product_type, product):
    if product_type == "isgri_lc":
        print(product.show())
        raise product
    raise RuntimeError

@pytest.mark.odaapi
@pytest.mark.jemx_plugin
@pytest.mark.jemx_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("max_pointings", [10, 100])
@pytest.mark.parametrize("scw_list_size", [10, 100])
@pytest.mark.parametrize("integral_data_rights", [None, "public", "all-private"])
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
def test_jemx_dummy_data_rights_oda_api(dispatcher_live_fixture, product_type, max_pointings, integral_data_rights, scw_list_size):
    dispatcher_fetch_dummy_products("default")

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    import oda_api.api

    disp = oda_api.api.DispatcherAPI(
        url=dispatcher_live_fixture)

    if (integral_data_rights == "public" or integral_data_rights is None) and (max_pointings < 50 and scw_list_size < 50):
        product = disp.get_product(
            product_type="Dummy",
            instrument="jemx",
            max_pointings=max_pointings,
            integral_data_rights=integral_data_rights,
            product=product_type,
            osa_version="OSA10.2",
            scw_list=[f"0665{i:04d}0010.001" for i in range(scw_list_size)]
        )
        logger.info("product: %s", product)
        logger.info("product show %s", product.show())

        validate_product(product_type, product)

        session_id = disp.session_id
        job_id = disp.job_id

        # check query output are generated
        query_output_json_fn = f'scratch_sid_{session_id}_jid_{job_id}/query_output.json'
        # the aliased version might have been created
        query_output_json_fn_aliased = f'scratch_sid_{session_id}_jid_{job_id}_aliased/query_output.json'
        assert os.path.exists(query_output_json_fn) or os.path.exists(query_output_json_fn_aliased)
        # get the query output
        if os.path.exists(query_output_json_fn):
            f = open(query_output_json_fn)
        else:
            f = open(query_output_json_fn_aliased)

        jdata = json.load(f)

        assert jdata["status_dictionary"]["debug_message"] == ""
        assert jdata["status_dictionary"]["error_message"] == ""
        assert jdata["status_dictionary"]["job_status"] == "done"
        assert jdata["status_dictionary"]["message"] == ""    
    else:
        with pytest.raises(oda_api.api.RemoteException):
            product = disp.get_product(
                product_type="Dummy",
                instrument="jemx",
                max_pointings=max_pointings,
                integral_data_rights=integral_data_rights,
                product=product_type,
                osa_version="OSA10.2",
                scw_list=[f"0665{i:04d}0010.001" for i in range(scw_list_size)]
            )


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