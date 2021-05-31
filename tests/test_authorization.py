import io
import time
import yaml
import jwt
import pytest
import logging

from cdci_data_analysis.pytest_fixtures import loop_ask, ask, dispatcher_fetch_dummy_products

logger = logging.getLogger(__name__)

dummy_params = dict(
    query_status="new",
    query_type="Dummy",
    async_dispatcher=False
)

dummy_params_by_instrument = dict(
    isgri=dict(
        **dummy_params,
        instrument="isgri",
        scw_list="066500220010.001",
    ),
    jemx=dict(
        **dummy_params,
        instrument="jemx",
        scw_list="066500220010.001",
    ),
)

unauthorized_message_header = "Unfortunately, your priviledges are not sufficient to make the request for this particular product and parameter combination."

# should be in dispatcher, if used commonly
def construct_token(roles, dispatcher_test_conf, expires_in=5000):
    secret_key = dispatcher_test_conf['secret_key']

    default_exp_time = int(time.time()) + expires_in
    default_token_payload = dict(
        sub="mtm@mtmco.net",
        name="mmeharga",
        roles="general",
        exp=default_exp_time,
        tem=0,
        mstout=True,
        mssub=True
    )

    token_payload = {
        **default_token_payload,
        "roles": roles
    }

    return jwt.encode(token_payload, secret_key, algorithm='HS256')


@pytest.mark.isgri_plugin
@pytest.mark.isgri_plugin_dummy
# @pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("max_pointings,scw_list_size", [(10, 10), (10, 100), (100, 10)])
@pytest.mark.parametrize("integral_data_rights", [None, "public", "all-private"])
@pytest.mark.parametrize("product_type", ['isgri_spectrum'])
# too much repetetion, maybe run timetimes
# @pytest.mark.parametrize("product_type", ['isgri_spectrum', 'isgri_image', 'isgri_lc', 'jemx_image'])
@pytest.mark.parametrize("roles", [None, [], ["unige-hpc-full"]])
@pytest.mark.parametrize("call_mode", ["ask", "odaapi"])
def test_unauthorized(dispatcher_long_living_fixture, dispatcher_test_conf, product_type, max_pointings, integral_data_rights, scw_list_size, roles, call_mode):
    dispatcher_fetch_dummy_products("default", reuse=True)

    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    instrument = product_type.split("_")[0]

    params = {
        **dummy_params_by_instrument[instrument],
        "product_type": product_type,
        "max_pointings": max_pointings,
        "integral_data_rights": integral_data_rights,
        "scw_list": [f"0665{i:04d}0010.001" for i in range(scw_list_size)]
    }

    if roles is not None:
        params['token'] = construct_token(roles, dispatcher_test_conf)

    exit_status_message_parts = []

    if max_pointings > 50 or scw_list_size > 50:
        if roles is None or "unige-hpc-full" not in roles:
            exit_status_message_parts.append(unauthorized_message_header)
            exit_status_message_parts.append('needed to request > 50 ScW')

    if integral_data_rights == "all-private":
        exit_status_message_parts.append(unauthorized_message_header)
        exit_status_message_parts.append('private INTEGRAL data')

    if exit_status_message_parts != []:
        expected_status_code = 403
        expected_status = 'failed'
    else:
        expected_status_code = 200
        expected_status = 'done'

    logger.info("\033[31mexpected_status_code: %s\033[0m",
                expected_status_code)

    if call_mode == "ask":
        logger.info("constructed server: %s", server)
        jdata = ask(server, params, expected_query_status=expected_status,
                    expected_job_status=expected_status, max_time_s=50, expected_status_code=expected_status_code)
        logger.info(list(jdata.keys()))
        logger.info(jdata)

        for exit_status_message_part in exit_status_message_parts:
            assert exit_status_message_part in jdata['exit_status']['message']
    elif call_mode == "odaapi":
        import oda_api.api

        disp = oda_api.api.DispatcherAPI(
            url=dispatcher_long_living_fixture)

        def request():
            return disp.get_product(
                product=params['product_type'],
                product_type=params['query_type'],
                **{ k:v for k,v in params.items() if k not in ['product_type', 'query_type'] },
            )

        if exit_status_message_parts == []:
            product = request()
            logger.info("product: %s", product)
            logger.info("product show %s", product.show())
        else:
            with pytest.raises(oda_api.api.RemoteException):
                request()


# TODO are the parameters for the request ok?
@pytest.mark.odaapi
@pytest.mark.isgri_plugin
def test_invalid_token_oda_api(dispatcher_long_living_fixture, dispatcher_test_conf):
    import oda_api.api

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
            token=construct_token([], dispatcher_test_conf, expires_in=-100)
        )
