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


from cdci_osa_plugin import conf_file,conf_dir
from cdci_data_analysis.analysis.queries import  *
from cdci_data_analysis.analysis.instrument import Instrument

from .osa_image_query import JemxMosaicQuery
from .osa_dataserve_dispatcher import OsaDispatcher
from .osa_common_pars import  osa_common_instr_query
from .osa_spectrum_query import JemxSpectrumQuery
from .osa_lightcurve_query import JemxLightCurveQuery



def osa_jemx_factory():

    src_query=SourceQuery('src_query')

    instr_query_pars = osa_common_instr_query()
    instr_num = Integer(value=2,name='jemx_num')
    instr_query_pars.append(instr_num)

    E1_keV = SpectralBoundary(value=3., E_units='keV', name='E1_keV')
    E2_keV = SpectralBoundary(value=35., E_units='keV', name='E2_keV')

    spec_window = ParameterRange(E1_keV, E2_keV, 'spec_window')
    instr_query_pars.append(spec_window)

    instr_query = InstrumentQuery(
        name='jemx_parameters',
        extra_parameters_list=instr_query_pars,
        input_prod_list_name='scw_list',
        input_prod_value=None,
        catalog=None,
        catalog_name='user_catalog')





    image=JemxMosaicQuery('jemx_image_query')

    spectrum = JemxSpectrumQuery('jemx_spectrum_query')

    light_curve = JemxLightCurveQuery('jemx_lc_query')


    xspec_fit = SpectralFitQuery('spectral_fit_query', None)

    query_dictionary={}
    query_dictionary['jemx_image'] = 'jemx_image_query'
    query_dictionary['jemx_spectrum'] = 'jemx_spectrum_query'
    query_dictionary['jemx_lc'] = 'jemx_lc_query'
    query_dictionary['spectral_fit'] = 'spectral_fit_query'

    #print('--> conf_file', conf_file)
    #print('--> conf_dir', conf_dir)

    return  Instrument('jemx',
                       data_serve_conf_file=conf_file,
                       src_query=src_query,
                       instrumet_query=instr_query,
                       #input_product_query=input_data,
                       product_queries_list=[image,spectrum,xspec_fit,light_curve],
                       data_server_query_class=OsaDispatcher,
                       query_dictionary=query_dictionary)
