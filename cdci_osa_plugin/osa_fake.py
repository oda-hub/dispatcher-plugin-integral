from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy
# absolute import eg: import numpy as np

import  os

import logging
import  time as system_time
# Project
# relative import eg: from .mod import f


from .osa_common_pars import  osa_common_instr_query
from cdci_data_analysis.analysis.instrument import Instrument

from cdci_data_analysis.analysis.queries import  *
from cdci_osa_plugin import conf_file,conf_dir


logger = logging.getLogger(__name__)



def osa_fake_factory():

    src_query=SourceQuery('src_query')

    instr_query_pars=osa_common_instr_query()

    E1_keV = SpectralBoundary(value=10., E_units='keV', name='E1_keV')
    E2_keV = SpectralBoundary(value=40., E_units='keV', name='E2_keV')
    spec_window = ParameterRange(E1_keV, E2_keV, 'spec_window')

    instr_query_pars.append(spec_window)


    instr_query=InstrumentQuery(
        name='fake_parameters',
        extra_parameters_list=instr_query_pars,
        input_prod_list_name='scw_list',
        input_prod_value=None,
        catalog=None,
        catalog_name='user_catalog')





    waiting_time = Integer(value=5, name='waiting_time')
    fake_long_request = FakeQuery('fake_long_request')
    fake_long_request.parameters.append(waiting_time)
    #update_image=ImageProcessQuery('update_image')

    query_dictionary={}

    query_dictionary['fake_long_request'] = 'fake_long_request'


    #print('--> conf_file',conf_file)
    #print('--> conf_dir', conf_dir)

    return  Instrument('osa_fake',
                       data_serve_conf_file=conf_file,
                       src_query=src_query,
                       instrumet_query=instr_query,
                       product_queries_list=[fake_long_request],
                       data_server_query_class=FakeDispatcher,
                       query_dictionary=query_dictionary)


class FakeDispatcher(object):

    def __init__(self,config=None,use_dicosverer=False,target=None,modules=[],assume=[],inject=[],instrument=None):
      pass

    def config(self,_data_server_url,_data_server_cache):
        pass

    def get_exception_status_message(self,e):
        pass


    def get_exceptions_message(self,e):
        pass


    def test_communication(self, max_trial=120, sleep_s=1,logger=None):
        query_out = QueryOutput()

        message = ''
        debug_message = ''
        connection_status_message = ''
        busy_exception = False
        query_out.set_done(message=message, debug_message=str(debug_message))
        return query_out

    def test_has_input_products(self,instrument,logger=None):
        query_out = QueryOutput()
        query_out.set_done(message='message', debug_message=str('debug_message'))
        return  query_out,[]

    def get_comments(self,res):
        pass





    def run_query(self,call_back_url,run_asynch=True,logger=None,target=None,modules=None,assume=None):

        query_out = QueryOutput()
        res=None
        query_out.set_done(message='', debug_message=str(''), job_status='done',
                           comment='', warning='')

        return res,query_out



class FakeQuery(ProductQuery):

    def __init__(self,name):

        super(FakeQuery, self).__init__(name)


    def get_data_server_query(self,instrument,
                              config=None):

        t = instrument.get_par_by_name('waiting_time').value
        print('wating time',t)
        system_time.sleep(t)
        d=FakeDispatcher()
        return d


    def build_product_list(self, instrument, res, out_dir, prod_prefix=None, api=False):


        prod_list = [None]

        return prod_list



    def process_product_method  (self, instrument, prod_list,api=False, **kw):

        query_out = QueryOutput()

        query_out.prod_dictionary['image'] = 'TEST'



        return query_out

