import pytest
import logging
import requests
import ast

logger = logging.getLogger(__name__)


def test_empty_request(dispatcher_live_fixture):
    server = dispatcher_live_fixture
    print("constructed server:", server)

    c=requests.get(server + "/run_analysis",
                   params={},
                )

    print("content:", c.text)

    jdata=c.json()

    assert c.status_code == 400

     # parameterize this
    assert jdata['installed_instruments'] == ['empty', 'empty-async', 'empty-semi-async', 'isgri', 'jemx', 'osa_fake']

    assert jdata['debug_mode'] == "yes"
    assert 'dispatcher-config' in jdata['config']

    dispatcher_config = jdata['config']['dispatcher-config']

    assert 'origin' in dispatcher_config

    assert 'sentry_url' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_port' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_host' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'secret_key' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'smtp_server_password' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'products_url' in dispatcher_config['cfg_dict']['dispatcher']

    logger.info(jdata['config'])


def test_no_debug_mode_empty_request(dispatcher_live_fixture_no_debug_mode):
    server = dispatcher_live_fixture_no_debug_mode
    print("constructed server:", server)

    c=requests.get(server + "/run_analysis",
                   params={},
                )

    print("content:", c.text)

    jdata=c.json()

    assert c.status_code == 400

     # parameterize this
    assert jdata['installed_instruments'] == ['isgri', 'jemx']

    assert jdata['debug_mode'] == "no"
    assert 'dispatcher-config' in jdata['config']

    dispatcher_config = jdata['config']['dispatcher-config']

    assert 'origin' in dispatcher_config

    assert 'sentry_url' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_port' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'logstash_host' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'secret_key' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'smtp_server_password' not in dispatcher_config['cfg_dict']['dispatcher']
    assert 'products_url' in dispatcher_config['cfg_dict']['dispatcher']

    logger.info(jdata['config'])


def test_osa_version_splitting():
    from cdci_osa_plugin.osa_common_pars import split_osa_version

    # default versions
    assert split_osa_version("OSA10.2") == ("OSA10.2", "default-isdc", [])
    assert split_osa_version("OSA11.0") == ("OSA11.0", "default-isdc", [])
    assert split_osa_version("OSA11.2-beta") == ("OSA11.2", "beta", [])

    # IC dev versions
    assert split_osa_version("OSA11.2") == ("OSA11.2", "default-isdc", [])

    assert split_osa_version("OSA11.2-devsmth") == ("OSA11.2", "devsmth", [])

    # dev may contain dashes too
    assert split_osa_version("OSA11.2-devsmth-smth-else") == ("OSA11.2", "devsmth-smth-else", [])

    # modifiers
    assert split_osa_version("OSA11.2-devsmth-smth-else--fullbkg") == ("OSA11.2", "devsmth-smth-else", ["fullbkg"])
    assert split_osa_version("OSA11.2-devsmth-smth-else--fullbkg--jemxnrt--rmfoffsetv1") == ("OSA11.2", "devsmth-smth-else", ["fullbkg", "jemxnrt", 'rmfoffsetv1'])
    assert split_osa_version("OSA11.2-devsmth-smth-else--jemxnrt") == ("OSA11.2", "devsmth-smth-else", ["jemxnrt"])

    # non-normative modiers

    with pytest.raises(RuntimeError) as e:
        split_osa_version("OSA11.2-devsmth-smth-else--jemxnrt--fullbkg")

    assert str(e.value) == ("non-normative OSA version modifier(s): 'jemxnrt--fullbkg', "
                            "expected 'fullbkg--jemxnrt'. "
                            "Modifers should be sorted and non-duplicate.")

                        
    with pytest.raises(RuntimeError) as e:
        split_osa_version("OSA11.2-devsmth-smth-else--jemxnrt--jemxnrt")

    assert str(e.value) == ("non-normative OSA version modifier(s): 'jemxnrt--jemxnrt', "
                            "expected 'jemxnrt'. "
                            "Modifers should be sorted and non-duplicate.")


    with pytest.raises(RuntimeError) as e:
        split_osa_version("OSA11.2-devsmth-smth-else--jemxnrt--unknown")

    assert str(e.value) == ("provided unknown OSA version modifier(s): 'unknown' "
                            "in version 'OSA11.2-devsmth-smth-else', "
                            "known: 'fullbkg--jemxnrt--rmfoffsetv1'")


@pytest.mark.parametrize("instrument", ['isgri', 'jemx'])
def test_instrument_description(dispatcher_live_fixture, instrument):
    import oda_api.api

    disp = oda_api.api.DispatcherAPI(url=dispatcher_live_fixture)
    jdata = disp.get_instrument_description(instrument)

    assert jdata[0][0] == {'instrumet': instrument}
    assert jdata[0][1] == {'prod_dict': {instrument + '_image': instrument + '_image_query',
                                         instrument + '_lc': instrument + '_lc_query',
                                         instrument + '_spectrum': instrument + '_spectrum_query',
                                         'spectral_fit': 'spectral_fit_query'}}

    # extract the list of queries
    expected_query_list = ['src_query',
                           instrument + '_parameters',
                           instrument + '_image_query',
                           instrument + '_spectrum_query',
                           instrument + '_lc_query',
                           'spectral_fit_query']

    returned_query_list = []
    for q in jdata[0][2:]:
        q_obj = ast.literal_eval(q)
        returned_query_list.append(q_obj[0]['query_name'])

    assert len(expected_query_list) == len(returned_query_list)
    assert all(elem in returned_query_list for elem in expected_query_list)


@pytest.mark.parametrize("instrument", ['isgri', 'jemx'])
@pytest.mark.parametrize("product_type", ['_spectrum', '_image', '_lc', 'spectral_fit'])
def test_product_description(dispatcher_live_fixture, instrument, product_type):
    import oda_api.api

    instrument_product_type = product_type
    if product_type != 'spectral_fit':
        instrument_product_type = instrument + product_type

    disp = oda_api.api.DispatcherAPI(url=dispatcher_live_fixture)
    jdata = disp.get_product_description(instrument, instrument_product_type)

    assert jdata[0][0] == {'instrumet': instrument}
    assert jdata[0][1] == {'prod_dict': {instrument + '_image': instrument + '_image_query',
                                         instrument + '_lc': instrument + '_lc_query',
                                         instrument + '_spectrum': instrument + '_spectrum_query',
                                         'spectral_fit': 'spectral_fit_query'}}

    # extract the list of expected queries
    expected_query_list = ['src_query', instrument + '_parameters', instrument_product_type + '_query']

    returned_query_list = []
    for q in jdata[0][2:]:
        q_obj = ast.literal_eval(q)
        returned_query_list.append(q_obj[0]['query_name'])

    assert len(expected_query_list) == len(returned_query_list)
    assert all(elem in returned_query_list for elem in expected_query_list)
