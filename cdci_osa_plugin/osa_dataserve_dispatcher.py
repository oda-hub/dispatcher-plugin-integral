"""
Overview
--------
   
general info about this module


Classes and Inheritance Structure
----------------------------------------------
.. inheritance-diagram:: 

Summary
---------
.. autosummary::
   list of the module you want
    
Module API
----------
"""

from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Andrea Tramacere, Volodymyr Savchenko"

#TODO: constrain dispatcher interface version

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy
import  ast

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np
import json

# Project
# relative import eg: from .mod import f
import ddaclient as dc
import  logging
from cdci_osa_plugin import conf_file as plugin_conf_file

from cdci_data_analysis.configurer import DataServerConf
from cdci_data_analysis.analysis.queries import  *
from cdci_data_analysis.analysis.job_manager import  Job
from cdci_data_analysis.analysis.io_helper import FilePath
from cdci_data_analysis.analysis.products import  QueryOutput
from cdci_data_analysis.analysis.exceptions import UnfortunateRequestResults, RequestNotUnderstood
import json
import traceback
import time
from ast import literal_eval
import  re
from contextlib import contextmanager

from astropy.coordinates import SkyCoord

# @contextmanager
# def silence_stdout():
#     new_target = open(os.devnull, "w")
#     old_target, sys.stdout = sys.stdout, new_target
#     try:
#         yield new_target
#     finally:
#         sys.stdout = old_target
#
#
#
# def redirect_out(path):
#     #print "Redirecting stdout"
#     sys.stdout.flush() # <--- important when redirecting to files
#     newstdout = os.dup(1)
#     devnull = os.open('%s/SED.log'%path, os.O_CREAT)
#     os.dup2(devnull, 1)
#     os.close(devnull)
#     sys.stdout = os.fdopen(newstdout, 'w')

#def view_traceback():
#    ex_type, ex, tb = sys.exc_info()
#    traceback.print_tb(tb)
#    del tb



logger = logging.getLogger(__name__)



class DDAException(UnfortunateRequestResults):

    def __init__(self, message='', debug_message=''):
        super(DDAException, self).__init__(message)
        self.message=message
        self.debug_message=debug_message


class DDAUnknownException(DDAException):

    def __init__(self,message='ddosa unknown exception', debug_message=''):
        super(DDAUnknownException, self).__init__(message, debug_message)


class ConfigProblem(Exception):
    pass


class OsaDispatcher(object):

    def __init__(self,
                 config=None,
                 use_dicosverer=False,
                 target=None,
                 modules=[],
                 assume=[],
                 inject=[],
                 instrument=None):

        self.target = target
        self.modules = modules
        self.assume = assume
        self.inject = inject

        self._test_products_with_astroquery = True

        config=None

        if instrument is not None and hasattr(instrument, 'data_server_conf_dict'):
            try:
                config = DataServerConf(data_server_url=instrument.data_server_conf_dict['data_server_url'],
                                       data_server_remote_cache=instrument.data_server_conf_dict['data_server_cache'],
                                       dispatcher_mnt_point=instrument.data_server_conf_dict['dispatcher_mnt_point'],
                                       dummy_cache=instrument.data_server_conf_dict['dummy_cache'],
                                       allowed_keys=["data_server_remote_cache", "dispatcher_mnt_point"],
                                       )
                
                logger.info("built config from instrument.data_server_conf_dict: %s, config: %s", instrument.data_server_conf_dict, config)

            except Exception as e:
                logger.error("problem building config with DataServerConf: %s", e)
                raise

        elif instrument is not None:
            try:
                config = instrument.from_conf_file(plugin_conf_file)
                logger.info("succeeded to instrument.from_conf_file from {plugin_conf_file}: {config}")
            except Exception as e:
                raise RuntimeError(f"failed to instrument.from_conf_file from {plugin_conf_file}: {e}")

        else:
            raise RequestNotUnderstood(message='instrument cannot be None',
                                       debug_message='instrument set to None in OsaDispatcher __init__')

        try:
            _data_server_url = config.data_server_url
            _data_server_cache = config.data_server_cache

            if _data_server_url is None or _data_server_cache is None:
                raise Exception(f"some config values are not set, loading from {config}")

        except Exception as e:
            logger.error("problem loading config with %s: %s", config, e)
            raise

        self.config(_data_server_url,_data_server_cache)


    def config(self,_data_server_url,_data_server_cache):
        logger.info("\033[31msetting config with %s to %s, %s\033[0m", self.config, _data_server_url, _data_server_cache)

        if _data_server_cache is None or _data_server_cache is None:
            raise ConfigProblem(f"problem setting config with {_data_server_cache} {_data_server_cache}")

        self.data_server_url = _data_server_url
        self.data_server_cache = _data_server_cache

    def get_exception_status_message(self,e):
        status=''
        if hasattr(e,'content'):
            try:
                content = json.loads(e.content)
                status = content['result']['status']
            except:
                pass
        return status


    def get_exceptions_message(self,e):
        message=None
        if hasattr(e,'exceptions'):
            try:
                message = json.dumps(e.exceptions[0],ensure_ascii=False)
            except :
                message = e.__repr__()
        return message


    def test_communication(self, max_trial=120, sleep_s=1,logger=None):
        print('--> start test connection to',self.data_server_url)
        remote = dc.RemoteDDA(self.data_server_url, self.data_server_cache)

        query_out = QueryOutput()


        message=''
        debug_message = ''
        connection_status_message=''
        busy_exception=False


        time.sleep(sleep_s)

        for i in range(max_trial):
            time.sleep(sleep_s)
            try:
                r = remote.poke()
                print('remote poke ok at trial',i)
                #DONE
                query_out.set_done(message=message, debug_message=str(debug_message))
                busy_exception=False
                connection_status_message='OK'
                break
            except dc.WorkerException as e:
                connection_status_message = self.get_exception_status_message(e)
                query_out.set_query_exception(e, 'test connection',message='connection_status=%s'%connection_status_message,logger=logger)
                busy_exception=True
                print('remote poke not ok, trial',i,connection_status_message)

            except Exception as e:
                connection_status_message = self.get_exception_status_message(e)
                query_out.set_query_exception(e, 'test connection',
                                              message='connection_status=%s' % connection_status_message, logger=logger)
                busy_exception = True
                print('remote poke not ok, trial', i, connection_status_message)


        if busy_exception==True:
            try:
                r = remote.poke()
            except Exception as e:
                connection_status_message = self.get_exception_status_message(e)
                # FAILED
                 #query_out.set_failed('test connection',
                 #                message='connection_status=%s' % connection_status_message,
                 #                logger=logger,
                 #                excep=e)

                run_query_message = 'Connection Error'
                debug_message = self.get_exceptions_message(e)
                # FAILED
                query_out.set_failed('test connection',
                                     message='connection_status=%s' % connection_status_message,
                                     logger=logger,
                                     excep=e,
                                     e_message=run_query_message,
                                     debug_message=debug_message)

                raise DDAException('Connection Error',debug_message)

        if connection_status_message == 'busy' or busy_exception==True:
            print('server is busy')
            # FAILED
            #query_out.set_failed('test busy',
            #                 message='connection_status=%s'%connection_status_message,
            #                 logger=logger,
            #                 excep=e)

            query_out.set_failed('test busy',
                                 message='connection_status=%s' % connection_status_message,
                                 logger=logger,
                                 excep=e,
                                 e_message='data server busy',
                                 debug_message='data server busy')

            raise DDAException('Connection Error', debug_message)



        print('--> end test busy')

        return query_out

    def test_has_input_products(self,instrument,logger=None):
        print('--> start has input_products')
        RA = instrument.get_par_by_name('RA').value
        DEC = instrument.get_par_by_name('DEC').value
        radius = instrument.get_par_by_name('radius').value
        scw_list = instrument.get_par_by_name('scw_list').value
        use_max_pointings = instrument.get_par_by_name('max_pointings').value
        osa_version = instrument.get_par_by_name('osa_version').value

        query_out = QueryOutput()


        message = ''
        debug_message = ''
        has_input_products_message=''
        prod_list=[]
        if scw_list is not None and scw_list != []:
            prod_list=scw_list

        else:
            T1_iso = instrument.get_par_by_name('T1')._astropy_time.isot
            T2_iso = instrument.get_par_by_name('T2')._astropy_time.isot

            print ('input',RA,DEC,T1_iso,T2_iso)

            if self._test_products_with_astroquery:
                # note that this just might introduce discrepancy, since it is not the exact workflow used by the backend

                from astroquery.heasarc import Heasarc
                import astroquery

                heasarc = Heasarc()


                with astroquery.heasarc.Conf.server.set_temp('https://www.isdc.unige.ch/browse/w3query.pl'):
                    T_i=heasarc.query_region(
                                SkyCoord(RA, DEC, unit="deg"), 
                                mission='integral_rev3_scw', 
                                resultmax=1000000, # all ppo
                                radius="200 deg", 
                                cache=False,
                                time=T1_iso.replace("T", " ") + " .. " + T2_iso.replace("T", " "),
                                fields='All'
                            )

            else:
                target = "ReportScWList"
                modules = ['git://rangequery/staging-1-3']


                scwlist_assumption = OsaDispatcher.get_scwlist_assumption(None, T1_iso, T2_iso, RA, DEC, radius, use_max_pointings)
                assume = ["rangequery.ReportScWList(input_scwlist=%s)"%scwlist_assumption[0],
                          scwlist_assumption[1]]


                remote = dc.RemoteDDA(self.data_server_url, self.data_server_cache)

                try:
                    product = remote.query(target=target, modules=modules, assume=assume, sync=True)
                    #DONE
                    query_out.set_done(message=message, debug_message=str(debug_message))

                    prod_list = getattr(product, 'scwidlist', None)

                    if prod_list is None:
                        raise RuntimeError(f"product.prod_list is None")

                    if len(prod_list)<1:
                        run_query_message = 'scwlist empty'
                        debug_message = ''
                        query_out.set_failed('test has input prods',
                                             message=run_query_message,
                                             logger=logger,
                                             job_status='failed',
                                             e_message=run_query_message,
                                             debug_message='')

                        raise DDAException(message='scwlist empty', debug_message='')



                except dc.WorkerException as e:
                    run_query_message = 'WorkerException'
                    debug_message = self.get_exceptions_message(e)
                    # FAILED
                    query_out.set_failed('test has input prods',
                                         message='has input_products=%s' % run_query_message,
                                         logger=logger,
                                         excep=e,
                                         job_status='failed',
                                         e_message=run_query_message,
                                         debug_message=debug_message)

                    raise DDAException('WorkerException', debug_message)

                except dc.AnalysisException as e:

                    run_query_message = 'AnalysisException'
                    debug_message = self.get_exceptions_message(e)

                    query_out.set_failed('test has input prods',
                                         message='run query message=%s' % run_query_message,
                                         logger=logger,
                                         excep=e,
                                         job_status='failed',
                                         e_message=run_query_message,
                                         debug_message=debug_message)

                    raise DDAException(message=run_query_message, debug_message=debug_message)

                except Exception as e:
                    run_query_message = 'DDAUnknownException in test has input prods: ' + repr(e)
                    query_out.set_failed('test has input prods ',
                                         message='run query message=%s' % run_query_message,
                                         logger=logger,
                                         excep=e,
                                         job_status='failed',
                                         e_message=run_query_message,
                                         debug_message='')

                    raise DDAUnknownException()

        return query_out,prod_list



    def get_comments(self,res):
        comment=''
        warning=''
        if hasattr(res,'comment'):
            comment=res.comment
        if hasattr(res,'warning'):
            warning=res.warning

        return comment,warning





    def run_query(self,call_back_url,run_asynch=True,logger=None,target=None,modules=None,assume=None):
        print(self,"run_query",call_back_url,target,modules,assume)

        if target is None:
            target=self.target

        if modules is None:
            modules=self.modules

        if assume is None:
            assume=self.assume



        res = None
        #status = 0
        message = ''
        debug_message = ''
        backend_comment = ''
        backend_warning = ''
        query_out = QueryOutput()
        try:

            logger.setLevel(logging.ERROR)


            print('--osa disp--')
            print('call_back_url',call_back_url)
            print('*** run_asynch', run_asynch)



            res= dc.RemoteDDA(self.data_server_url, self.data_server_cache).query(target=target,
                                                    modules=modules,
                                                    assume=assume,
                                                    inject=self.inject,
                                                    prompt_delegate = run_asynch,
                                                    callback = call_back_url)

            backend_comment,backend_warning=self.get_comments(res)



            print ('--> url for call_back',call_back_url)
            print("--> cached object in", res,res.ddcache_root_local)
            #DONE
            query_out.set_done(message=message, debug_message=str(debug_message),job_status='done',comment=backend_comment,warning=backend_warning)

            #job.set_done()

        except dc.AnalysisException as e:

            run_query_message = 'AnalysisException'
            debug_message=self.get_exceptions_message(e)
            # we have to add the exception to the message
            try:
                my_dict = literal_eval(debug_message)
            except:
                my_dict= debug_message
            if 'exception' in debug_message:
                print('debug_message', type(debug_message), debug_message)
                #print(my_dict)
                if hasattr(my_dict,'keys'):
                    if 'exception' in my_dict.keys():
                        run_query_message=run_query_message+':%s'%my_dict['exception']
                elif 'exception' in str(my_dict):
                    run_query_message = run_query_message + ':%s' %str(my_dict)

            query_out.set_failed('run query ',
                                 message='run query message=%s' % run_query_message,
                                 logger=logger,
                                 excep=e,
                                 job_status='failed',
                                 e_message=run_query_message,
                                 debug_message=debug_message)

            raise DDAException(message=run_query_message,debug_message=debug_message)

        except dc.WorkerException as e:

            run_query_message = 'WorkerException'
            debug_message = self.get_exceptions_message(e)
            #FAILED
            query_out.set_failed('run query ',
                                 message='run query message=%s' % run_query_message,
                                 logger=logger,
                                 excep=e,
                                 job_status='failed',
                                 e_message=run_query_message,
                                 debug_message=debug_message)

            raise DDAException(message=run_query_message, debug_message=debug_message)


        except dc.AnalysisDelegatedException as e:
            # DONE DELEGATION
            backend_comment, backend_warning = self.get_comments(res)
            query_out.set_done(message=message, debug_message=str(debug_message), job_status='submitted',comment=backend_comment,warning=backend_warning)

        except Exception as e:
            run_query_message = 'DDAUnknownException in run_query: ' + repr(e)
            query_out.set_failed('run query ',
                                 message='run query message=%s' %run_query_message,
                                 logger=logger,
                                 excep=e,
                                 job_status='failed',
                                 e_message=run_query_message,
                                 debug_message='')

            raise DDAUnknownException(message=run_query_message)




        return res,query_out


    @classmethod
    def get_scwlist_assumption(cls, scw_list, T1, T2, RA, DEC, radius, use_max_pointings):

        #print('DEBUG --> scw_list', scw_list,len(scw_list))
        if scw_list is not None and scw_list != []:

            scw_list=[item.strip() for item in scw_list]
            template = re.compile(r'^(\d{12}).(\d{3})$')
            acceptList = [item.strip() for item in scw_list if template.match(item)]
            if len(acceptList) != len(scw_list):
                wrong_list = [item for item in scw_list if item not in acceptList]
                raise DDAException(message='the following scws have a wrong format %s' % wrong_list)


            scwlist_assumption = ['ddosa.IDScWList','ddosa.IDScWList(use_scwid_list=[%s])' % (", ".join(["\""+str(scw)+"\"" for scw in scw_list])) ]
            if len(scw_list) > use_max_pointings:
                raise RequestNotUnderstood(message='scws are limited to %d' % use_max_pointings)
        else:
            scwlist_assumption = ['rangequery.TimeDirectionScWList',
                                  f'''rangequery.TimeDirectionScWList(
                                                  use_coordinates=dict(RA={RA},DEC={DEC},radius={radius}),
                                                  use_timespan=dict(T1="{T1}",T2="{T2}"),
                                                  use_max_pointings={use_max_pointings},
                                                  use_scwversion="any",
                                                  )\
                                              ''']



        return scwlist_assumption


    @classmethod
    def get_osa_query_base(cls, instrument):

        # time_range_type = instrument.get_par_by_name('time_group_selector').value
        RA = instrument.get_par_by_name('RA').value
        DEC = instrument.get_par_by_name('DEC').value
        radius = instrument.get_par_by_name('radius').value
        scw_list = instrument.get_par_by_name('scw_list').value
        user_catalog = instrument.get_par_by_name('user_catalog').value
        use_max_pointings = instrument.get_par_by_name('max_pointings').value

        extramodules = []
        if scw_list is None or scw_list == []:
            T1_iso = instrument.get_par_by_name('T1')._astropy_time.isot
            T2_iso = instrument.get_par_by_name('T2')._astropy_time.isot
            extramodules.append('git://rangequery/staging-1-3')
        else:
            T1_iso = None
            T2_iso = None

        scwlist_assumption = cls.get_scwlist_assumption(scw_list, T1_iso, T2_iso, RA, DEC, radius, use_max_pointings)
        cat = cls.get_instr_catalog(instrument,user_catalog=user_catalog)

        inject = []

        if cat is not None:
            extramodules.append("git://gencat")
            inject.append(cat)

        return scwlist_assumption,cat,extramodules,inject

    @classmethod
    def get_instr_catalog(cls, instrument, user_catalog=None):
        cat = None

        def get_col_data(t, n, default):
            if n in user_catalog.table.colnames:
                return t[n].data
            else:
                return default

        if user_catalog is not None:
            cat = ['SourceCatalog',
                   {
                       "catalog": [
                           {
                               "RA": float(ra.deg),
                               "DEC": float(dec.deg),
                               "NAME": str(name),
                               "FLAG": int(flag),
                               "ISGRI_FLAG": int(isgri_flag),
                           }
                           for ra, dec, name, flag, isgri_flag in zip(
                               user_catalog.ra,
                               user_catalog.dec,
                               user_catalog.name,
                               get_col_data(user_catalog.table, 'FLAG', np.zeros(len(user_catalog.ra))),
                               get_col_data(user_catalog.table, 'ISGRI_FLAG', np.zeros(len(user_catalog.ra))),
                           )
                       ],
                       "version": "v2",  # catalog id here; good if user-understandable, but can be computed internally
                       "autoversion": True,
                   }
                   ]

        if instrument.name == 'jemx':
            print('jemx cat', cat)
        if instrument.name == 'isgri':
            print('isgri cat', cat)

        # else:
        #    cat = None

        return cat

