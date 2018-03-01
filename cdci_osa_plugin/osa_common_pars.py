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

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f

from cdci_data_analysis.analysis.parameters import *


def osa_common_instr_query():
    #not exposed to frontend
    #TODO make a special class
    max_pointings=Integer(value=50,name='max_pointings')

    radius = Angle(value=5.0, units='deg', name='radius')
    E1_keV = SpectralBoundary(value=10., E_units='keV', name='E1_keV')
    E2_keV = SpectralBoundary(value=40., E_units='keV', name='E2_keV')
    spec_window = ParameterRange(E1_keV, E2_keV, 'spec_window')

    instr_query_pars=[radius,max_pointings,spec_window]


    return instr_query_pars
