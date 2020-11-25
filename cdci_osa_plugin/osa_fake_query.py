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
import  time
# Project
# relative import eg: from .mod import f
from astropy.io import  fits as pf

from cdci_data_analysis.analysis.queries import ProductQuery
from cdci_data_analysis.analysis.products import QueryOutput


logger = logging.getLogger(__name__)



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
        pass

    def test_has_input_products(self,instrument,logger=None):
       pass


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
        time.sleep(t)

        return FakeDispatcher()


    def build_product_list(self, instrument, res, out_dir, prod_prefix=None, api=False):


        prod_list = [None]

        return prod_list



    def process_product_method  (self, instrument, prod_list,api=False, **kw):

        query_out = QueryOutput()

        query_out.prod_dictionary['image'] = 'TEST'



        return query_out

