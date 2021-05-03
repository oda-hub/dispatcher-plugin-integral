import pytest
import logging
import requests
import json
import time
import random

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


@pytest.mark.parametrize("selection", ["range", "280200470010.001"])
@pytest.mark.dda
@pytest.mark.isgri_plugin
@pytest.mark.xfail
def test_isgri_image_no_pointings(dispatcher_live_fixture, selection):
    """
    this will reproduce the entire flow of frontend-dispatcher, apart from receiving callback
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

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
    # let's make the request public for simplicity
    params.pop('token')
    c = requests.get(server + "/run_analysis",
                     params=params,
                     )

    jdata = c.json()

    assert jdata["query_status"] == "failed"
    assert jdata["exit_status"]["debug_message"] == "{\"node\": \"dataanalysis.core.AnalysisException\", \"exception\": \"{}\", \"exception_kind\": \"handled\"}"
    assert jdata["exit_status"]["error_message"] == "AnalysisException:{}"
    assert jdata["exit_status"]["message"] == "failed: get dataserver products "
    assert jdata["job_status"] == "failed"


@pytest.mark.dda
@pytest.mark.isgri_plugin
def test_isgri_image_fixed_done(dispatcher_live_fixture):
    """
    something already done at backend
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

    params = {
        **default_params,
        'async_dispatcher': False,
    }

    c = requests.get(server + "/run_analysis",
                     params=params,
                     )

    jdata = c.json()

    assert jdata["query_status"] == "done"

    json.dump(jdata, open("jdata.json", "w"))


@pytest.mark.dda
@pytest.mark.isgri_plugin
def test_isgri_image_fixed_done_async_postproc(dispatcher_live_fixture):
    """
    something already done at backend
    new session every time, hence re-do post-process
    """

    server = dispatcher_live_fixture
    print("constructed server:", server)

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
    print("constructed server:", server)

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


def ask(server, params, expected_query_status, expected_job_status=None, max_time_s=2.0, expected_status_code=200):
    t0 = time.time()
    c=requests.get(server + "/run_analysis",
                   params={**params},
                  )
    logger.info(f"\033[31m request took {time.time() - t0} seconds\033[0m")
    t_spent = time.time() - t0
    assert t_spent < max_time_s

    logger.info("content: %s", c.text[:1000])
    if len(c.text) > 1000:
        print(".... (truncated)")

    jdata=c.json()

    if expected_status_code is not None:
        assert c.status_code == expected_status_code

    logger.info(list(jdata.keys()))

    if expected_job_status is not None:
        assert jdata["exit_status"]["job_status"] in expected_job_status

    if expected_query_status is not None:
        assert jdata["query_status"] in expected_query_status

    return jdata


def loop_ask(server, params, max_time_s=2.0):
    jdata = ask(server,
                params={
                    **params,
                    'async_dispatcher': True,
                    'query_status': 'new'
                },
                expected_query_status=["submitted"],
                max_time_s=max_time_s)

    last_status = jdata["query_status"]

    t0 = time.time()

    tries_till_reset = 20

    while True:
        if tries_till_reset <= 0:
            next_query_status = "ready"
            print("\033[1;31;46mresetting query status to new, too long!\033[0m")
            tries_till_reset = 20
        else:
            next_query_status = jdata['query_status']
            tries_till_reset -= 1

        jdata = ask(server,
                    params={
                        **params,
                        "async_dispatcher": True,
                        'query_status': next_query_status,
                        'job_id': jdata['job_monitor']['job_id'],
                        'session_id': jdata['session_id']
                    },
                    expected_query_status=["submitted", "done"],
                    max_time_s=max_time_s,
                    )

        if jdata["query_status"] in ["ready", "done"]:
            logger.info("query READY:", jdata["query_status"])
            break

        logger.info("query NOT-READY:", jdata["query_status"], jdata["job_monitor"])
        logger.info("looping...")

        time.sleep(5)

    logger.info(f"\033[31m total request took {time.time() - t0} seconds\033[0m")

    return jdata, time.time() - t0

