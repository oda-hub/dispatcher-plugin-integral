import requests
import time
import logging

# logger
logger = logging.getLogger(__name__)


def test_discover_plugin():
    import cdci_data_analysis.plugins.importer as importer

    assert 'cdci_osa_plugin' in importer.cdci_plugins_dict.keys()


def run_analysis(server, params, method='get'):
    if method == 'get':
        return requests.get(server + "/run_analysis",
                            params={**params}
                            )

    elif method == 'post':
        return requests.post(server + "/run_analysis",
                             data={**params}
                             )
    else:
        raise NotImplementedError


def ask(server, params, expected_query_status, expected_job_status=None, max_time_s=2.0, expected_status_code=200,
        method='get'):
    t0 = time.time()
    c = run_analysis(server, params, method=method)

    logger.info(f"\033[31m request took {time.time() - t0} seconds\033[0m")
    t_spent = time.time() - t0
    assert t_spent < max_time_s

    logger.info("content: %s", c.text[:1000])
    if len(c.text) > 1000:
        print(".... (truncated)")

    jdata = c.json()

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

