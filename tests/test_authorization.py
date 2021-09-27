import time
import jwt
import pytest
import logging

from cdci_data_analysis.analysis.hash import make_hash
from cdci_data_analysis.flask_app.dispatcher_query import InstrumentQueryBackEnd
from cdci_data_analysis.pytest_fixtures import loop_ask, ask, dispatcher_fetch_dummy_products, DispatcherJobState

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
        disp.get_product(
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


@pytest.mark.isgri_plugin
@pytest.mark.isgri_plugin_dummy
@pytest.mark.dependency(depends=["test_default"])
@pytest.mark.parametrize("list_length", [5, 55, 550])
@pytest.mark.parametrize("roles", ["", "unige-hpc-full", "unige-hpc-full, unige-hpc-extreme"])
def test_scw_list_file(dispatcher_long_living_fixture, dispatcher_test_conf, list_length, roles):
    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    params = {
        **dummy_params_by_instrument['isgri'],
        "product_type": "isgri_image",
        "query_type": "Dummy",
        "use_scws": "user_file"
    }

    params.pop('scw_list')

    token_none = (roles == '')
    if not token_none:
        params['token'] = construct_token(roles, dispatcher_test_conf)

    scw_list = [f"0665{i:04d}0010.001" for i in range(list_length)]
    scw_list_formatted = "\n".join(scw_list)

    file_path = DispatcherJobState.create_p_value_file(p_value=scw_list_formatted)
    list_file = open(file_path)

    files = {
        "user_scw_list_file": list_file.read()
    }

    # just for having the roles in a list
    roles = roles.split(',')
    roles[:] = [r.strip() for r in roles]

    expected_query_status = 'done'
    expected_job_status = 'done'
    expected_status_code = 200
    expected_message = ''
    # in case the request is successfull, then the products con be inspected
    check_product = True
    if token_none:
        if list_length > 50:
            expected_query_status = 'failed'
            expected_job_status = 'failed'
            expected_status_code = 403
            expected_message =  \
                   "Unfortunately, your priviledges are not sufficient to make the request for this particular product and parameter combination.\n"\
                    "- Your priviledge roles include []\n"\
                    "- You are lacking all of the following roles:\n"\
                    + (" - unige-hpc-extreme: it is needed to request > 500 ScW\n" if list_length > 500 else "") + \
                    f" - unige-hpc-full: it is needed to request > 50 ScW, you requested scw_list) = [ .. {list_length} items .. ]\n" \
                   "You can request support if you think you should be able to make this request."
            check_product = False
    else:
        if 'unige-hpc-extreme' not in roles:
            if list_length > 500:
                expected_query_status = 'failed'
                expected_job_status = 'failed'
                expected_status_code = 403
                expected_message =  \
                       "Unfortunately, your priviledges are not sufficient to make the request for this particular product and parameter combination.\n" \
                        f"- Your priviledge roles include {roles}\n" \
                        "- You are lacking all of the following roles:\n" \
                        + " - unige-hpc-extreme: it is needed to request > 500 ScW\n" + \
                       "You can request support if you think you should be able to make this request."
                check_product = False

    jdata = ask(server,
                params=params,
                expected_query_status=expected_query_status,
                expected_job_status=expected_job_status,
                expected_status_code=expected_status_code,
                method='post',
                files=files)

    list_file.close()

    assert jdata["exit_status"]["job_status"] in expected_job_status
    assert jdata["query_status"] in expected_query_status
    assert jdata["exit_status"]["message"] == expected_message

    if check_product:
        assert 'scw_list' in jdata['products']['analysis_parameters']
        assert jdata['products']['analysis_parameters']['scw_list'] == scw_list
        # test job_id
        job_id = jdata['products']['job_id']
        params.pop('use_scws', None)
        # adapting some values to string
        for k, v in params.items():
            params[k] = str(v)

        expected_par_dict = {
            **params,
            'detection_threshold': 7.0,
            'scw_list': scw_list,
            'sub': 'mtm@mtmco.net' if not token_none else None,
            'src_name': '1E 1740.7-2942',
            'RA': 265.97845833,
            'DEC': -29.74516667,
            'T1': '2017-03-06T13:26:48.000',
            'T2': '2017-03-06T15:32:27.000',
            'E1_keV': 20.0,
            'E2_keV': 40.0,
            'integral_data_rights': 'public',
            'osa_version': 'OSA11.1',
            'max_pointings': 50,
            'radius': 15.0,
        }

        restricted_par_dic = InstrumentQueryBackEnd.restricted_par_dic(expected_par_dict)
        calculated_job_id = make_hash(restricted_par_dic)

        assert job_id == calculated_job_id



@pytest.mark.isgri_plugin
@pytest.mark.isgri_plugin_dummy
# some combinations of data rights and IC might be disfavored, but let's not enforce it now
@pytest.mark.parametrize("roles", ["", "integral-private-qla"])
@pytest.mark.parametrize("integral_data_rights", [None, "public", "all-private"])
@pytest.mark.parametrize("osa_version", ["public-OSA10.2", "obsolete-OSA11.0", "obsolete-OSA11.0-dev210628.1813-17265", "public-OSA11.1", "invalid-OSA11.2"]) 
def test_osa_versions(dispatcher_long_living_fixture, dispatcher_test_conf, roles, integral_data_rights, osa_version):
    server = dispatcher_long_living_fixture
    logger.info("constructed server: %s", server)

    osa_version_qualifier, osa_version = osa_version.split("-", 1)

    params = {
        **dummy_params_by_instrument['isgri'],
        "product_type": "isgri_image",
        "query_type": "Dummy",
        "scw_list": [f"0665{i:04d}0010.001" for i in range(5)],
        "osa_version": osa_version,
        "integral_data_rights": integral_data_rights
    }


    token_none = (roles == '')
    if not token_none:
        params['token'] = construct_token(roles, dispatcher_test_conf)

    # just for having the roles in a list
    roles_list = [r.strip() for r in roles.split(',')]

    # ok by default
    # invalid request takes priority over unauthorized
    expected_query_status = 'done'
    expected_job_status = 'done'
    expected_status_code = 200
    expected_message = ''
    expected_error = None
    
    if integral_data_rights == "all-private":
        if "integral-private-qla" not in roles_list:
            expected_query_status = None
            expected_job_status = None
            expected_status_code = 403
            expected_message = None

    if osa_version_qualifier == "public":  
        # ok and public version restricted version  
        pass
    elif osa_version_qualifier == "private":    
        # restricted version
        if "integral-private-qla" not in roles_list:
            expected_query_status = None
            expected_job_status = None
            expected_status_code = 403
            expected_message = None
    elif osa_version_qualifier == "invalid":
        # invalid version
        expected_query_status = None
        expected_job_status = None
        expected_status_code = 400
        expected_message = None
    elif osa_version_qualifier == "obsolete":
        # invalid version
        expected_query_status = None
        expected_job_status = None
        expected_status_code = 400
        expected_error = 'RequestNotUnderstood():Please note OSA11.0 is being phased out. We consider that for all or almost all likely user requests OSA11.1 shoud be used instead of OSA11.0.'
        expected_message = None
    else:
        RuntimeError("programming error")


    jdata = ask(server,
                params=params,
                expected_query_status=expected_query_status,
                expected_job_status=expected_job_status,
                expected_status_code=expected_status_code,
                )

    if expected_message is not None:        
        assert jdata["exit_status"]["message"] == expected_message

    if expected_error is not None:        
        assert jdata["error"] == expected_error
