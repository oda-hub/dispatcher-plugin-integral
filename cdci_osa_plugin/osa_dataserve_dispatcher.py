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

__author__ = "Andrea Tramacere"

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
import ddosaclient as dc
import  logging
import  simple_logger
from cdci_osa_plugin import conf_file as plugin_conf_file

from cdci_data_analysis.configurer import DataServerConf
from cdci_data_analysis.analysis.queries import  *
from cdci_data_analysis.analysis.job_manager import  Job
from cdci_data_analysis.analysis.io_helper import FilePath
from cdci_data_analysis.analysis.products import  QueryOutput
import json
import traceback
import time
from ast import literal_eval
import os
from contextlib import contextmanager

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






class OsaDispatcherException(Exception):

    def __init__(self, message='', debug_message=''):
        super(OsaDispatcherException, self).__init__(message)
        self.message=message
        self.debug_message=debug_message



class DDOSAException(Exception):

    def __init__(self, message='', debug_message=''):
        super(DDOSAException, self).__init__(message)
        self.message=message
        self.debug_message=debug_message


class DDOSAUnknownException(DDOSAException):

    def __init__(self,message='ddosa unknown exception',debug_message=''):
        super(DDOSAUnknownException, self).__init__(message,debug_message)




class OsaDispatcher(object):

    def __init__(self,config=None,use_dicosverer=False,target=None,modules=[],assume=[],inject=[],instrument=None):
        #print('--> building class OsaQyery')
        simple_logger.log()
        simple_logger.logger.setLevel(logging.ERROR)

        self.target = target
        self.modules = modules
        self.assume = assume
        self.inject = inject

        #instrument = None
        config=None
        #print('--> config passed to init', config,instrument)

        #print ('TEST')
        #for k in instrument.data_server_conf_dict.keys():
        #    print ('dict:',k,instrument.data_server_conf_dict[k ])

        #config = DataServerConf(data_server_url=instrument.data_server_conf_dict['data_server_url'],
        #                       data_server_port=instrument.data_server_conf_dict['data_server_port'],
        #                       data_server_remote_cache=instrument.data_server_conf_dict['data_server_cache'],
        #                       dispatcher_mnt_point=instrument.data_server_conf_dict['dispatcher_mnt_point'],
        #                       dummy_cache=instrument.data_server_conf_dict['dummy_cache'])
        #for v in vars(config):
        #    print('attr:', v, getattr(config, v))


        if use_dicosverer == True:
            try:
                c = discover_docker.DDOSAWorkerContainer()

                self.data_server_url = c.data_server_url
                self.data_server_cache = c.dataserver_cache
                #print("===>managed to read from docker:")



            except Exception as e:
                raise RuntimeError("failed to read from docker", e)






        elif config is not None:
            pass



        elif instrument is not None and hasattr(instrument, 'data_server_conf_dict'):

            #print('--> from data_server_conf_dict')
            try:
                config = DataServerConf(data_server_url=instrument.data_server_conf_dict['data_server_url'],
                                       data_server_port=instrument.data_server_conf_dict['data_server_port'],
                                       data_server_remote_cache=instrument.data_server_conf_dict['data_server_cache'],
                                       dispatcher_mnt_point=instrument.data_server_conf_dict['dispatcher_mnt_point'],
                                       dummy_cache=instrument.data_server_conf_dict['dummy_cache'])

                #print ('config',config)
                #for v in vars(config):
                #    print('attr:', v, getattr(config, v))

            except Exception as e:
                #    #print(e)

                print("ERROR->")
                raise RuntimeError("failed to use config ", e)

        elif instrument is not None:
            try:
                #print('--> plugin_conf_file', plugin_conf_file)
                config = instrument.from_conf_file(plugin_conf_file)

            except Exception as e:
                #    #print(e)

                print("ERROR->")
                raise RuntimeError("failed to use config ", e)

        else:

            raise OsaDispatcherException(message='instrument cannot be None',
                                 debug_message='instrument se to None in OsaDispatcher __init__')

        try:
            _data_server_url = config.data_server_url
            _data_server_port = config.data_server_port
            _data_server_cache = config.data_server_cache

        except Exception as e:
            #    #print(e)

            print("ERROR->")
            raise RuntimeError("failed to use config ", e)

        self.config(_data_server_url, _data_server_port,_data_server_cache)

        #print("data_server_url:", self.data_server_url)
        #print("dataserver_port:", self.data_server_port)
        #print('--> done')



    def config(self,_data_server_url, _data_server_port,_data_server_cache):
        #print('config done in config method')
        self.data_server_port = _data_server_port
        self.data_server_url=_data_server_url
        self.data_server_cache=_data_server_cache

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
        remote = dc.RemoteDDOSA(self.data_server_url, self.data_server_cache)

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

                raise DDOSAException('Connection Error',debug_message)

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

            raise DDOSAException('Connection Error', debug_message)



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

            target = "ReportScWList"
            modules = ['git://rangequery']


            scwlist_assumption = OsaDispatcher.get_scwlist_assumption(None, T1_iso, T2_iso, RA, DEC, radius, use_max_pointings)
            assume = ["rangequery.ReportScWList(input_scwlist=%s)"%scwlist_assumption[0],
                      scwlist_assumption[1]]


            remote = dc.RemoteDDOSA(self.data_server_url, self.data_server_cache)

            try:
                product = remote.query(target=target,modules=modules,assume=assume)
                #DONE
                query_out.set_done(message=message, debug_message=str(debug_message))
                prod_list= product.scwidlist
                #print ('ciccio scwlist for T1,T2',T1_iso,T2_iso,scw_list)
                if len(prod_list)<1:
                    run_query_message = 'scwlist empty'
                    debug_message = ''
                    query_out.set_failed('test has input prods',
                                         message=run_query_message,
                                         logger=logger,
                                         job_status='failed',
                                         e_message=run_query_message,
                                         debug_message='')

                    raise DDOSAException(message='scwlist empty', debug_message='')



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

                raise DDOSAException('WorkerException', debug_message)


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

                raise DDOSAException(message=run_query_message, debug_message=debug_message)

            except Exception as e:
                run_query_message = 'DDOSAUnknownException in test has input prods'
                query_out.set_failed('test has input prods ',
                                     message='run query message=%s' % run_query_message,
                                     logger=logger,
                                     excep=e,
                                     job_status='failed',
                                     e_message=run_query_message,
                                     debug_message='')

                raise DDOSAUnknownException()

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

            simple_logger.logger.setLevel(logging.ERROR)


            print('--osa disp--')
            print('call_back_url',call_back_url)
            print('*** run_asynch', run_asynch)



            res= dc.RemoteDDOSA(self.data_server_url, self.data_server_cache).query(target=target,
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

            raise DDOSAException(message=run_query_message,debug_message=debug_message)

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

            raise DDOSAException(message=run_query_message, debug_message=debug_message)


        except dc.AnalysisDelegatedException as e:
            # DONE DELEGATION
            backend_comment, backend_warning = self.get_comments(res)
            query_out.set_done(message=message, debug_message=str(debug_message), job_status='submitted',comment=backend_comment,warning=backend_warning)

        except Exception as e:
                run_query_message = 'DDOSAUnknownException in run_query'
                query_out.set_failed('run query ',
                                     message='run query message=%s' %run_query_message,
                                     logger=logger,
                                     excep=e,
                                     job_status='failed',
                                     e_message=run_query_message,
                                     debug_message='')

                raise DDOSAUnknownException(message=run_query_message)




        return res,query_out


    @classmethod
    def get_scwlist_assumption(cls, scw_list, T1, T2, RA, DEC, radius, use_max_pointings):

        if scw_list is not None and scw_list != []:
            scwlist_assumption = ['ddosa.IDScWList','ddosa.IDScWList(use_scwid_list=[%s])' % (", ".join(["\""+str(scw)+"\"" for scw in scw_list])) ]
        else:
            scwlist_assumption = ['rangequery.TimeDirectionScWList',
                                  'rangequery.TimeDirectionScWList(\
                                                  use_coordinates=dict(RA=%(RA)s,DEC=%(DEC)s,radius=%(radius)s),\
                                                  use_timespan=dict(T1="%(T1)s",T2="%(T2)s"),\
                                                  use_max_pointings=%(use_max_pointings)d,\
                                                  use_scwversion="any",\
                                                  )\
                                              ' % (dict(RA=RA, DEC=DEC, radius=radius, T1=T1, T2=T2, use_max_pointings=use_max_pointings))]

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
            extramodules.append('git://rangequery')
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
    def get_instr_catalog(cls, instrument,user_catalog=None):
        cat=None
        if user_catalog is not None:
            cat = ['SourceCatalog',
                   {
                       "catalog": [
                           {
                               "RA": float(ra.deg),
                               "DEC": float(dec.deg),
                               "NAME": str(name),
                           }
                           for ra, dec, name in zip(user_catalog.ra, user_catalog.dec, user_catalog.name)
                       ],
                       "version": "v2",  # catalog id here; good if user-understandable, but can be computed internally
                       "autoversion": True,
                   }
                   ]

        if instrument.name=='jemx':
            print('jemx cat',cat)
        if instrument.name=='isgri':
            print('isgri cat',cat)

        #else:
        #    cat = None

        return cat
