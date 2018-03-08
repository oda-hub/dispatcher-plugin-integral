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


__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


from cdci_osa_plugin import conf_file
from cdci_data_analysis.analysis.queries import  *
from cdci_data_analysis.analysis.instrument import Instrument

from .osa_image_query import JemxMosaicQuery
from .osa_dataserve_dispatcher import OsaDispatcher
from .osa_common_pars import  osa_common_instr_query




def osa_jemx_factory():

    src_query=SourceQuery('src_query')

    instr_query_pars = osa_common_instr_query()
    instr_num = Name(value='jemx1', name='jemx_num')
    instr_query_pars.append(instr_num)

    instr_query = InstrumentQuery(
        name='isgri_parameters',
        extra_parameters_list=instr_query_pars,
        input_prod_list_name='scw_list',
        input_prod_value=None,
        catalog=None,
        catalog_name='user_catalog')




    #
    # light_curve =LightCurveQuery('isgri_lc_query',
    #                              None,
    #                              get_products_method=get_osa_lightcurve,
    #                              get_dummy_products_method=get_osa_lightcurve_dummy_products,
    #                              process_product_method=process_osa_lc_products)

    image=JemxMosaicQuery('isgri_image_query')

    #
    # spectrum=SpectrumQuery('isgri_spectrum_query', None,
    #                        get_products_method=get_osa_spectrum,
    #                        get_dummy_products_method=get_osa_spectrum_dummy_products,
    #                        process_product_method=process_osa_spectrum_products)




    # xspec_fit = SpectralFitQuery('spectral_fit_query', None)

    query_dictionary={}
    query_dictionary['jemx_image'] = 'jemx_image_query'
    #query_dictionary['isgri_spectrum'] = 'isgri_spectrum_query'
    #query_dictionary['isgri_lc'] = 'isgri_lc_query'
    #query_dictionary['spectral_fit'] = 'spectral_fit_query'

    return  Instrument('jemx',
                       data_serve_conf_file=conf_file,
                       src_query=src_query,
                       instrumet_query=instr_query,
                       #input_product_query=input_data,
                       product_queries_list=[image],
                       data_server_query_class=OsaDispatcher,
                       query_dictionary=query_dictionary)
