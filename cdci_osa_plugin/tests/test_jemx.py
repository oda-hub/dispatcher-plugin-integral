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



def test_jemx_spectrum():
    from oda_api.api import DispatcherAPI
    from oda_api.plot_tools import OdaImage, OdaLightCurve
    from oda_api.data_products import NumpyDataProduct
    import  os

    cookies = dict(_oauth2_proxy=open(os.environ.get('HOME') + '/.oda-api-token').read().strip())

    disp = DispatcherAPI(host='10.194.169.161', port=32784, instrument='mock')

    data = disp.get_product(instrument='jemx',
                            product='jemx_spectrum',
                            T1='2003-03-15T23:27:40.0',
                            T2='2003-03-16T00:03:12.0',
                            query_type='Real',
                            osa_version='OSA10.2',
                            RA=255.986542,
                            DEC=-37.844167,
                            detection_threshold=5.0,
                            radius=15.,
                            E1_keV=3.,
                            E2_keV=35.,
                            product_type='Real')


    assert (type(data)==list)
    assert (type(data[0])==NumpyDataProduct)

def test_jemx_image():
    from oda_api.api import DispatcherAPI
    from oda_api.plot_tools import OdaImage, OdaLightCurve
    from oda_api.data_products import NumpyDataProduct
    import  os

    cookies = dict(_oauth2_proxy=open(os.environ.get('HOME') + '/.oda-api-token').read().strip())

    disp = DispatcherAPI(host='10.194.169.161', port=32784, instrument='mock')

    data = disp.get_product(instrument='jemx',
                            product='jemx_image',
                            T1='2003-03-15T23:27:40.0',
                            T2='2003-03-16T00:03:12.0',
                            osa_version='OSA10.2',
                            RA=255.986542,
                            DEC=-37.844167,
                            detection_threshold=5.0,
                            radius=15.,
                            E1_keV=3.,
                            E2_keV=35.,
                            product_type='Real')


    assert (type(data)==list)
    assert (type(data[0])==NumpyDataProduct)