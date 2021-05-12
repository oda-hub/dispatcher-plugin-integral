import pytest
import logging
import requests
import json

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

dummy_params = dict(
    query_status="new",
    query_type="Dummy",
    instrument="jemx",
    async_dispatcher=False
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
