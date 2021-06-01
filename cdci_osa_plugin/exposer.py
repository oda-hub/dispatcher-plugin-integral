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

import os

__author__ = "Andrea Tramacere"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy 
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


from .osa_isgri import  osa_isgri_factory
from .osa_jemx import osa_jemx_factory
instr_factory_list = [osa_isgri_factory, osa_jemx_factory]

if os.environ.get('DISPATCHER_DEBUG_MODE', 'no') == 'yes':
    from .osa_fake import osa_fake_factory
    instr_factory_list.append(osa_fake_factory)

