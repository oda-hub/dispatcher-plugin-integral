import pytest
import logging
import requests
import json
import time
import jwt
import os
import itertools

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


@pytest.mark.jemx_plugin
def test_jemx_deny_wrong_energy_range(dispatcher_long_living_fixture):
    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    for is_ok, E1_keV, E2_keV in [
            (False, 1,30),
            (False, 3,40),
            (False, 2,40),
            (False, 3,30),
            (False, 4,20),
        ]:

        params = {
            **dummy_params,
            'E1_keV': 1,
            "product_type": "jemx_image"
        }

        if is_ok:
            expected_query_status='done'
            expected_job_status='done'
        else:
            expected_query_status='failed'
            expected_job_status='unknown'

        logger.info("constructed server: %s", server)
        jdata = ask(server, params, expected_query_status=expected_query_status, expected_job_status=expected_job_status, max_time_s=5)
        logger.info(list(jdata.keys()))        

        if is_ok:
            pass
        else:
            assert jdata['exit_status']['message'] == 'failed: please adjust request parameters: JEM-X energy range is restricted to 3 - 35 keV'


@pytest.mark.jemx_plugin
@pytest.mark.parametrize('dummy_pack', ['default', 'empty'])
def test_jemx_dummy(dispatcher_long_living_fixture, dummy_pack):
    dispatcher_fetch_dummy_products(dummy_pack, reuse=True)

    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    params = {
        **dummy_params,
        "product_type": "jemx_image"
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='done', expected_job_status='done', max_time_s=5)
    logger.info(list(jdata.keys()))
    logger.info("data: %s", str(jdata)[:1000] + "...")

    logger.info('jdata["products"]["catalog"]: %s', str(jdata["products"]["catalog"])[:1000] + "...")
    

    assert len(jdata["products"]["catalog"]["cat_column_list"][0]) == 2
    
    if dummy_pack == "empty":
        #TODO:
        #assert len(jdata["products"]["catalog"]["cat_column_list"][0]) == 0
        pass

def get_crab_scw():
    #https://www.astro.unige.ch/cdci/astrooda/dispatch-data/gw/timesystem/api/v1.0/scwlist/cons/2008-03-10T08:00:00/2008-03-30T08:00:00?&ra=83&dec=22&radius=2.0&min_good_isgri=1000
    return ["066500140010","066500640010","066500890010","066600140010","066600390010","066600420010","066600420020","066600420030","066600650010","066600900010"]


@pytest.mark.dda
@pytest.mark.jemx_plugin
@pytest.mark.parametrize("product_type", ['jemx_spectrum', 'jemx_image', 'jemx_lc'])
def test_jemx_products(dispatcher_long_living_fixture, product_type):
    server = dispatcher_long_living_fixture

    params = {
        **default_params,
        "product_type": product_type,
        "scw_list": [f"{s}.001" for s in get_crab_scw()]
    }

    logger.info("constructed server: %s", server)
    c = requests.get(server + "/run_analysis",
                      params=params)

    logger.info("content: %s...", c.text[:300])
    jdata = c.json()
    logger.info(list(jdata.keys()))

    json.dump(jdata, open("data.json", "w"))
    
    assert c.status_code == 200
    assert jdata['job_status'] == 'done'



#TODO: to dispatcher
# def validate_product(product_type, product):
#     if product_type == "jemx_lc":
#         print(product.show())
