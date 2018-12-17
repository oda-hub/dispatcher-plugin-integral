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


# Project
# relative import eg: from .mod import f

from oda_api.api import DispatcherAPI
from oda_api.data_products import NumpyDataProduct



def test_get_spectrum(instrument,product,product_type='Reale'):
    disp = DispatcherAPI.build_from_envs()

    data = disp.get_product(instrument=instrument,
                            product=product,
                            T1='2003-03-15T23:27:40.0',
                            T2='2003-03-16T00:03:12.0',
                            osa_version='OSA10.2',
                            RA=255.986542,
                            DEC=-37.844167,
                            detection_threshold=5.0,
                            radius=15.,
                            E1_keV=3.,
                            E2_keV=35.,
                            product_type=product_type)
    assert (data != None)
    return data

def test_spectra(data):
    assert (type(data) == list)
    assert (type(data[0]) == NumpyDataProduct)

def test_get_image(instrument,product,product_type='Reale'):


    disp = DispatcherAPI.build_from_envs()
    data = disp.get_product(instrument=instrument,
                            product=product,
                            T1='2003-03-15T23:27:40.0',
                            T2='2003-03-16T00:03:12.0',
                            osa_version='OSA10.2',
                            RA=255.986542,
                            DEC=-37.844167,
                            detection_threshold=5.0,
                            radius=15.,
                            E1_keV=10.,
                            E2_keV=35.,
                            product_type=product_type)

    assert(data!=None)
    return data


def test_image(data):
    assert (type(data) == list)
    assert (type(data[0]) == NumpyDataProduct)


def test_catalog(cat):
    assert (type(cat)==dict)

def test_jemx_spectrum():
    data = test_get_spectrum('jemx','jemx_sepctrum',product_type='Real')
    test_spectra(data)

def test_isgri_spectrum():
    data = test_get_spectrum('isgri','isgri_sepctrum',product_type='Real')
    test_spectra(data)



def test_jemx_image():
    data=test_get_image('jemx','jemx_image',product_type='Real')
    test_image(data)
    test_catalog(data[1])

def test_isgri_image():
    data=test_get_image('isgri','isgri_image',product_type='Real')
    test_image(data)
    test_catalog(data[1])