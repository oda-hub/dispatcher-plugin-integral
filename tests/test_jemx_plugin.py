import pytest
import logging
import requests
import json

logger = logging.getLogger(__name__)

default_params = dict(instrument='jemx',
                        jemx_num='2',
                        product_type='jemx_image',
                        scw_list=['010200230010.001'],
                        osa_version='OSA10.2',
                        detection_threshold=5.0,
                        radius=15.,
                        query_type='Real')


def test_default(dispatcher_live_fixture):
    server = dispatcher_live_fixture

# TODO are those parameters ok? I am sure the values are correct or the tests are properly set
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