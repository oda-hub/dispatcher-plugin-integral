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


@pytest.mark.xfail
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


def test_jemx_lc_source_name_formatting(dispatcher_long_living_fixture):
    """
    feature: source names
    feature: attribute source name
    """
    from oda_api.api import DispatcherAPI

    disp = DispatcherAPI(url="http://dispatcher.staging.internal.odahub.io")

    par_dict = {
        "src_name": "4U 1700-377",
        "RA": "270.80",
        "DEC": "-29.80",
        "T1": "2019-04-01",
        "T2": "2019-06-05",
        "T_format": "isot",
        "instrument": "jemx",
        "osa_version": "OSA11.0",
        "radius": "4",
        "max_pointings": "50",
        "integral_data_rights": "public",
        "jemx_num": "1",
        "E1_keV": "3",
        "E2_keV": "20",
        "product_type": "Real",
        "detection_threshold": "5",
        "product": "jemx_lc",
        "time_bin": "4",
        "time_bin_format": "sec",
        "catalog_selected_objects": "1,2,3",
        "selected_catalog": '{"cat_frame": "fk5", "cat_coord_units": "deg", "cat_column_list": [[0, 1, 2], ["GX 5-1", "MAXI SRC", "H 1820-303"], [96.1907958984375, 74.80066680908203, 66.31670379638672], [270.2771301269531, 270.7560729980469, 275.914794921875], [-25.088342666625977, -29.84027099609375, -30.366628646850586], [0, 1, 0], [0.05000000074505806, 0.05000000074505806, 0.05000000074505806]], "cat_column_names": ["meta_ID", "src_names", "significance", "ra", "dec", "FLAG", "ERR_RAD"], "cat_column_descr": [["meta_ID", "<i8"], ["src_names", "<U10"], ["significance", "<f8"], ["ra", "<f8"], ["dec", "<f8"], ["FLAG", "<i8"], ["ERR_RAD", "<f8"]], "cat_lat_name": "dec", "cat_lon_name": "ra"}',
    }

    data_collection_lc = disp.get_product(**par_dict)

    data_collection_lc.show()

    l_output = data_collection_lc.as_list()
    print('len(l_output): \n', len(l_output))
    assert len(l_output) == 2
    assert l_output[0]['prod_name'] == 'jemx_lc_0_H1820m303'
    assert l_output[0]['meta_data:']['src_name'] == 'H 1820-303'
    assert l_output[1]['prod_name'] == 'jemx_lc_1_MAXISRC'
    assert l_output[1]['meta_data:']['src_name'] == 'MAXI SRC'