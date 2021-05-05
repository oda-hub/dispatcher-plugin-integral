import pytest
import logging
import requests
import json

from tests.test_basic import ask

logger = logging.getLogger(__name__)

default_params = dict(instrument='jemx',
                        jemx_num='2',
                        product_type='jemx_image',
                        scw_list=['010200230010.001'],
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
@pytest.mark.depends(on=['test_default'])
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
@pytest.mark.depends(on=['test_default'])
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