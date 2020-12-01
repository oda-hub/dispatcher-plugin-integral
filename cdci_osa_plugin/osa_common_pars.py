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


class DummyOsaRes(object):

    def __init__(self):
        pass

from cdci_data_analysis.analysis.parameters import *

import odakb
import socket

def osa_common_instr_query():
    #not exposed to frontend
    #TODO make a special class
    max_pointings=Integer(value=500, name='max_pointings')

    radius = Angle(value=5.0, units='deg', name='radius')
    osa_version = Name(name_format='str', name='osa_version')
    if 'cdciweb01' in socket.gethostname():
        osa_version._allowed_values=['OSA10.2', 'OSA11.0', 'OSA11.0-dev'] #TODO-VS: add kb request
    else:
        osa_version._allowed_values = [ a['vs'] for a in odakb.sparql.select('oda:osa_version oda:osa_option ?vs') ]

    instr_query_pars=[radius,max_pointings,osa_version]


    return instr_query_pars



