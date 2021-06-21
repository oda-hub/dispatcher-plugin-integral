import pytest
import logging
import requests
import json
import time
import random
import jwt
import os
import itertools

from astropy.io import fits
import oda_api.api

from _pytest.debugging import pytestPDB
from cdci_data_analysis.pytest_fixtures import loop_ask, ask, dispatcher_fetch_dummy_products

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

secret_key = 'secretkey_test'
dummy_params = dict(
    query_status="new",
    query_type="Dummy",
    instrument="isgri",
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


def test_default(dispatcher_long_living_fixture):
    server = dispatcher_long_living_fixture


@pytest.mark.isgri_plugin
@pytest.mark.isgri_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("product_type", ['isgri_spectrum', 'isgri_image'])
def test_isgri_dummy(dispatcher_long_living_fixture, product_type):
    dispatcher_fetch_dummy_products("default")

    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    params = {
        **dummy_params,
        "product_type": product_type
    }

    logger.info("constructed server: %s", server)
    jdata = ask(server, params, expected_query_status='done',
                expected_job_status='done', max_time_s=15)
    logger.info(list(jdata.keys()))
    logger.info(jdata)


@pytest.mark.odaapi
@pytest.mark.isgri_plugin
@pytest.mark.isgri_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("product_type", ['isgri_spectrum', 'isgri_image'])
def test_isgri_dummy_oda_api(dispatcher_long_living_fixture, product_type):
    dispatcher_fetch_dummy_products("default")

    import oda_api.api

    disp = oda_api.api.DispatcherAPI(
        url=dispatcher_long_living_fixture)
    product = disp.get_product(
        product_type="Dummy",
        instrument="isgri",
        product=product_type,
        osa_version="OSA10.2",
        E1_keV=20.,
        E2_keV=40.,
        scw_list="066500220010.001",
        session_id="TESTSESSION",
    )

    logger.info("product: %s", product)
    logger.info("product show %s", product.show())

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
    assert jdata["status_dictionary"]["message"] == ""


def validate_product(product_type, product: oda_api.api.DataCollection):
    if product_type == "isgri_lc":
        print(product.show())
        print(dir(product))

        lc = product.isgri_lc_0_OAO1657m415

        print(lc)
        print(dir(lc))

        lc.write_fits_file("lc.fits")

        f = fits.open("lc.fits")

        assert len(f[1].data) == 2
        assert any(['TIMEDEL' == c.name for c in f[1].data.columns])

        # TODO: validate OGIP here  with python ogip module
    #TODO: other validations
    

@pytest.mark.xfail
@pytest.mark.dda
@pytest.mark.isgri_plugin
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("selection", ["range", "280200470010.001"])
def test_isgri_image_no_pointings(dispatcher_long_living_fixture, selection):
    """
    this will reproduce the entire flow of frontend-dispatcher, apart from receiving callback
    """

    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    if selection == "range":
        params = {
            **default_params,
            'T1': "2008-01-01T11:11:11.0",
            'T2': "2012-01-01T11:11:11.0",
            'RA': 83,
            'DEC': 22,
            'radius': 6,
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
def test_isgri_image_find_pointings(dispatcher_long_living_fixture):
    """
    this will reproduce the entire flow of frontend-dispatcher, apart from receiving callback
    """

    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    params = {
        **default_params,
        'T1': "2008-01-01T11:11:11.0",
        'T2': "2019-01-01T11:11:11.0",
        'RA': 83,
        'DEC': 22,
        'radius': 6,
        'max_pointings': 2,
        'scw_list': ''
    }
    
    jdata = ask(server, params, expected_query_status="done",
                expected_job_status="done", max_time_s=50)

    assert jdata["exit_status"]["debug_message"] == "{\"node\": \"dataanalysis.core.AnalysisException\", \"exception\": \"{}\", \"exception_kind\": \"handled\"}"
    assert jdata["exit_status"]["error_message"] == "AnalysisException:{}"
    assert jdata["exit_status"]["message"] == "failed: get dataserver products "


@pytest.mark.dda
@pytest.mark.isgri_plugin
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("method", ['get', 'post'])
def test_isgri_image_fixed_done(dispatcher_long_living_fixture, method):
    """
    something already done at backend
    """

    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    params = {
        **default_params,
        'async_dispatcher': False
    }

    jdata = ask(server, params,
                expected_query_status=["done"],
                max_time_s=50,
                method=method)


@pytest.mark.dda
@pytest.mark.isgri_plugin
def test_isgri_image_fixed_done_async_postproc(dispatcher_long_living_fixture):
    """
    something already done at backend
    new session every time, hence re-do post-process
    """

    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    params = {
        **default_params,
    }

    jdata, tspent = loop_ask(server, params)

    assert 20 < tspent < 40


@pytest.mark.dda
@pytest.mark.isgri_plugin
def test_isgri_image_random_emax(dispatcher_long_living_fixture):
    """
    something already done at backend
    """

    server = dispatcher_long_living_fixture
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
def test_isgri_lc(dispatcher_long_living_fixture):
    """
    something already done at backend
    """    

    server = dispatcher_long_living_fixture
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
def test_isgri_lc_odaapi(dispatcher_long_living_fixture):
    import oda_api.api

    product = oda_api.api.DispatcherAPI(
        url=dispatcher_long_living_fixture).get_product(
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


# TODO are the parameters for the request ok?
@pytest.mark.odaapi
@pytest.mark.dda
@pytest.mark.isgri_plugin
def test_valid_token_oda_api(dispatcher_long_living_fixture):
    import oda_api.api

    # let's generate a valid token
    token_payload = {
        **default_token_payload,
        "roles": ['unige-hpc-full', 'integral-private']
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    disp = oda_api.api.DispatcherAPI(
        url=dispatcher_long_living_fixture)
    product = disp.get_product(
        query_status="new",
        product_type="Real",
        instrument="isgri",
        product="isgri_image",
        osa_version="OSA10.2",
        E1_keV=40.0,
        E2_keV=200.0,
        scw_list="066500220010.001",
        token=encoded_token
    )

    logger.info("product: %s", product)
    logger.info("product show %s", product.show())

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
    assert jdata["status_dictionary"]["message"] == ""


# TODO are the parameters for the request ok?
@pytest.mark.odaapi
@pytest.mark.isgri_plugin
def test_invalid_token_oda_api(dispatcher_long_living_fixture):
    import oda_api.api

    # let's generate an expired token
    exp_time = int(time.time()) - 500
    # expired token
    token_payload = {
        **default_token_payload,
        "exp": exp_time
    }
    encoded_token = jwt.encode(token_payload, secret_key, algorithm='HS256')

    disp = oda_api.api.DispatcherAPI(
        url=dispatcher_long_living_fixture)
    with pytest.raises(oda_api.api.RemoteException):
        product = disp.get_product(
            query_status="new",
            product_type="Real",
            instrument="isgri",
            product="isgri_image",
            osa_version="OSA10.2",
            E1_keV=40.0,
            E2_keV=200.0,
            scw_list=[f"0665{i:04d}0010.001" for i in range(5)],
            token=encoded_token
        )


@pytest.mark.isgri_plugin
def test_isgri_deny_wrong_energy_range(dispatcher_long_living_fixture):
    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    for is_ok, E1_keV, E2_keV in [
            (False, 10,50),
            (False, 20,1000),
            (False, 10,1000),
            (False, 15,800),
            (False, 20,40),
        ]:

        params = {
            **dummy_params,
            'E1_keV': 1,
            "product_type": "isgri_image"
        }

        if is_ok:
            expected_query_status='done'
            expected_job_status='done'
        else:
            expected_query_status='failed'
            expected_job_status='unknown'

        logger.info("constructed server: %s", server)
        jdata = ask(server, params, expected_query_status=expected_query_status, expected_job_status=expected_job_status, max_time_s=50)
        logger.info(list(jdata.keys()))
        logger.info(jdata)

        if is_ok:
            pass
        else:
            assert jdata['exit_status']['message'] == 'failed: please adjust request parameters: ISGRI energy range is restricted to 15 - 800 keV'


@pytest.mark.isgri_plugin
@pytest.mark.isgri_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("list_lenght", [5, 55])
def test_scw_list_file(dispatcher_long_living_fixture, list_lenght):
    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    params = {
        **default_params,
        "query_type":"Dummy",
        "use_scws": "user_file"
    }
    # TODO a fixture is ideal for this
    scw_list = [f"0665{i:04d}0010.001" for i in range(list_lenght)]
    # generate ScWs list file
    if not os.path.exists('scws_list_files_examples'):
        os.makedirs('scws_list_files_examples')

    file_name = f'{list_lenght}_list_example'
    with open('scws_list_files_examples/' + file_name, 'w+') as outlist_file:
        outlist_file.write("\n".join(scw_list))

    list_file = open('scws_list_files_examples/' + file_name)

    if list_lenght > 50:
        expected_query_status = 'failed'
        expected_job_status = 'failed'
        expected_status_code = 403
    else:
        expected_query_status = 'done'
        expected_job_status = 'done'
        expected_status_code = 200

    # jdata = ask(server, params,
    #             expected_query_status=expected_query_status,
    #             expected_job_status=expected_job_status,
    #             max_time_s=50,
    #             method='post')

    c = requests.post(server + "/run_analysis",
                      data={
                          **params
                            },
                      files={
                          "user_scw_list_file": list_file.read()
                      }
                      )

    assert c.status_code == expected_status_code
    jdata = c.json()
    assert jdata["exit_status"]["job_status"] in expected_job_status
    assert jdata["query_status"] in expected_query_status

    outlist_file.close()
    logger.info(list(jdata.keys()))
    logger.info(jdata)
