import pytest
import logging
import requests
import json
import time
import random

from cdci_data_analysis.pytest_fixtures import loop_ask, ask

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
    scw_list="066500220010.001",
    async_dispatcher=False,
    integral_data_rights="public"
)

dummy_params = dict(
    query_status="new",
    query_type="Dummy",
    instrument="isgri",
    scw_list="066500220010.001",
    async_dispatcher=False
)


def test_default(dispatcher_live_fixture):
    server = dispatcher_live_fixture


@pytest.mark.isgri_plugin
@pytest.mark.isgri_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("product_type", ['isgri_spectrum', 'isgri_image'])
def test_isgri_dummy(dispatcher_live_fixture, product_type):
    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    params = {
        **dummy_params,
        "product_type": product_type
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='done',
                expected_job_status='done', max_time_s=5)
    logger.info(list(jdata.keys()))
    logger.info(jdata)


@pytest.mark.isgri_plugin
@pytest.mark.isgri_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("product_type", ['isgri_spectrum', 'isgri_image']) #TODO: jemx too, also lightcurve; and also allowed role passing test
def test_isgri_dummy_many_pointings(dispatcher_live_fixture, product_type):
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
                expected_job_status='failed', max_time_s=5, expected_status_code=403)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'].replace('isgri_image', 'isgri_spectrum') == "Roles [] not authorized to request the product isgri_spectrum, ['unige-hpc-full'] roles are needed"


    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": 10,
        "integral_data_rights": "public",
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='done',
                expected_job_status='done', max_time_s=5, expected_status_code=200)
    logger.info(list(jdata.keys()))
    logger.info(jdata)


    params = {
        **dummy_params,
        "product_type": product_type,
        "max_pointings": 10,
        "integral_data_rights": "all-private",
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='failed',
                expected_job_status='failed', max_time_s=5, expected_status_code=403)
    logger.info(list(jdata.keys()))
    logger.info(jdata)

    assert jdata['exit_status']['message'].replace('isgri_image', 'isgri_spectrum') == "Roles [] not authorized to request the product isgri_spectrum, ['integral-private'] roles are needed"
    
    


@pytest.mark.xfail
@pytest.mark.dda
@pytest.mark.isgri_plugin
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("selection", ["range", "280200470010.001"])
def test_isgri_image_no_pointings(dispatcher_live_fixture, selection):
    """
    this will reproduce the entire flow of frontend-dispatcher, apart from receiving callback
    """

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    if selection == "range":
        params = {
            **default_params,
            'T1': "2008-01-01T11:11:11.0",
            'T2': "2009-01-01T11:11:11.0",
            'max_pointings': 1
        }
    else:
        params = {
            **default_params,
            'scw_list': selection
        }

    jdata = ask(server, params, expected_query_status="failed",
                expected_job_status="failed", max_time_s=5)
    assert jdata["exit_status"]["debug_message"] == "{\"node\": \"dataanalysis.core.AnalysisException\", \"exception\": \"{}\", \"exception_kind\": \"handled\"}"
    assert jdata["exit_status"]["error_message"] == "AnalysisException:{}"
    assert jdata["exit_status"]["message"] == "failed: get dataserver products "


@pytest.mark.dda
@pytest.mark.isgri_plugin
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("method", ['get', 'post'])
def test_isgri_image_fixed_done(dispatcher_live_fixture, method):
    """
    something already done at backend
    """

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    params = {
        **default_params,
        'async_dispatcher': False,
    }

    jdata = ask(server, params,
                expected_query_status=["done"],
                max_time_s=50,
                method=method)
    json.dump(jdata, open("jdata.json", "w"))


@pytest.mark.dda
@pytest.mark.isgri_plugin
def test_isgri_image_fixed_done_async_postproc(dispatcher_live_fixture):
    """
    something already done at backend
    new session every time, hence re-do post-process
    """

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    params = {
        **default_params,
    }

    jdata, tspent = loop_ask(server, params)

    assert 20 < tspent < 40


@pytest.mark.dda
@pytest.mark.isgri_plugin
def test_isgri_image_random_emax(dispatcher_live_fixture):
    """
    something already done at backend
    """

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)

    try:
        emax = int(open("emax-last", "rt").read())
    except:
        emax = random.randint(30, 800)  # but sometimes it's going to be done
        open("emax-last", "wt").write("%d" % emax)

    params = {
        **default_params,
        'E2_keV': emax,
    }
    jdata, tspent = loop_ask(server, params, max_time_s=5)


@pytest.mark.dda
@pytest.mark.isgri_plugin
def test_isgri_lc(dispatcher_live_fixture):
    """
    something already done at backend
    """    

    server = dispatcher_live_fixture
    logger.info("constructed server: %s", server)  

    params = dict(
        query_status="new",
        query_type="Real",
        instrument="isgri",
        product_type="isgri_lc",
        osa_version="OSA10.2",
        E1_keV=20.,
        E2_keV=40.,
        scw_list="066500220010.001",
        session_id="TESTSESSION",
        async_dispatcher=False,
    )

    jdata, tspent = loop_ask(server, params, max_time_s=100, async_dispatcher=False)



@pytest.mark.odaapi
@pytest.mark.dda
@pytest.mark.isgri_plugin
def test_isgri_lc_odaapi(dispatcher_live_fixture):
    import oda_api.api

    product = oda_api.api.DispatcherAPI(
        url=dispatcher_live_fixture).get_product(
        query_type="Real",
        instrument="isgri",
        product="isgri_lc",
        osa_version="OSA10.2",
        E1_keV=20.,
        E2_keV=40.,
        scw_list="066500220010.001",
        session_id="TESTSESSION",
    )

    print("product:", product)

    print("product show", product.show())
    
    print("")

    print(product.show())

    print(product._p_list)

    print(product.isgri_lc_0_Crab)

    print(product.isgri_lc_0_Crab.data_unit[1])

    print(product.isgri_lc_0_Crab.data_unit[1].header)

    print(product.isgri_lc_0_Crab.data_unit[1].data)

    product.isgri_lc_0_Crab.data_unit[1].header['TTYPE8'] == 'XAX_E'


    
