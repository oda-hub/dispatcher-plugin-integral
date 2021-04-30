import pytest
import logging
import requests
import json

logger = logging.getLogger(__name__)

default_params = dict(
    query_status="new",
    query_type="Real",
    instrument="isgri",
    product_type="isgri_image",
    osa_version="OSA10.2",
    E1_keV=20.,
    E2_keV=40.,
    T1="2008-01-01T11:11:11.0",
    T2="2009-01-01T11:11:11.0",
    max_pointings=2,
    RA=83,
    DEC=22,
    radius=6,
    async_dispatcher=False,
)

dummy_params = dict(
    query_status="new",
    query_type="Dummy",
    instrument="isgri",
    async_dispatcher=False
)


def test_default(dispatcher_live_fixture):
    server = dispatcher_live_fixture


@pytest.mark.parametrize("product_type", ['isgri_spectrum', 'isgri_image'])
@pytest.mark.depends(on=['test_default'])
def test_isgri_dummy(dispatcher_live_fixture, product_type):
    server = dispatcher_live_fixture

    params = {
        **dummy_params,
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
