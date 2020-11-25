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

from .osa_image_query import IsgriMosaicQuery
from .osa_spectrum_query import IsgriSpectrumQuery
from .osa_lightcurve_query import IsgriLightCurveQuery
from .osa_dataserve_dispatcher import OsaDispatcher
from .osa_common_pars import  osa_common_instr_query
from .osa_fake import FakeQuery





def osa_isgri_factory():

    src_query=SourceQuery('src_query')

    instr_query_pars=osa_common_instr_query()

    E1_keV = SpectralBoundary(value=10., E_units='keV', name='E1_keV')
    E2_keV = SpectralBoundary(value=40., E_units='keV', name='E2_keV')
    spec_window = ParameterRange(E1_keV, E2_keV, 'spec_window')

    instr_query_pars.append(spec_window)


    instr_query=InstrumentQuery(
        name='isgri_parameters',
        extra_parameters_list=instr_query_pars,
        input_prod_list_name='scw_list',
        input_prod_value=None,
        catalog=None,
        catalog_name='user_catalog')





    light_curve =IsgriLightCurveQuery('isgri_lc_query')

    image=IsgriMosaicQuery('isgri_image_query')

    spectrum=IsgriSpectrumQuery('isgri_spectrum_query')

    xspec_fit = SpectralFitQuery('spectral_fit_query', None)

    #update_image=ImageProcessQuery('update_image')

    query_dictionary={}
    query_dictionary['isgri_image'] = 'isgri_image_query'
    query_dictionary['isgri_spectrum'] = 'isgri_spectrum_query'
    query_dictionary['isgri_lc'] = 'isgri_lc_query'
    query_dictionary['spectral_fit'] = 'spectral_fit_query'


    #print('--> conf_file',conf_file)
    #print('--> conf_dir', conf_dir)

    return  Instrument('isgri',
                       data_serve_conf_file=conf_file,
                       src_query=src_query,
                       instrumet_query=instr_query,
                       product_queries_list=[image,spectrum,light_curve,xspec_fit],
                       data_server_query_class=OsaDispatcher,
                       query_dictionary=query_dictionary)

