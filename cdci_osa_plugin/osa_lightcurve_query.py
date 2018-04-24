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
import os

# Dependencies
# eg numpy
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


import ddosaclient as dc

# Project
# relative import eg: from .mod import f
import  numpy as np

from astropy.io import fits as pf
from cdci_data_analysis.analysis.io_helper import FitsFile
from cdci_data_analysis.analysis.queries import LightCurveQuery
from cdci_data_analysis.analysis.products import LightCurveProduct,QueryProductList,QueryOutput

from .osa_dataserve_dispatcher import OsaDispatcher



class IsgriLigthtCurve(LightCurveProduct):
    def __init__(self,name,file_name,data,header,prod_prefix=None,out_dir=None,src_name=None):


        super(IsgriLigthtCurve, self).__init__(name,
                                               data,
                                               header,
                                               file_name=file_name,
                                               name_prefix=prod_prefix,
                                               file_dir=out_dir,
                                               src_name=src_name)



    @classmethod
    def build_from_ddosa_res(cls,
                             name,
                             file_name,
                             res,
                             src_name='ciccio',
                             prod_prefix = None,
                             out_dir = None):

        #hdu_list = pf.open(res.lightcurve)
        hdu_list = FitsFile(res.lightcurve).open()
        data = None
        header=None

        for hdu in hdu_list:
            if hdu.name == 'ISGR-SRC.-LCR':
                print('name', hdu.header['NAME'])
                if hdu.header['NAME'] == src_name:
                    data = hdu.data
                    header = hdu.header

            lc = cls(name=name, data=data, header=header,file_name=file_name,out_dir=out_dir,prod_prefix=prod_prefix,src_name=src_name)

        return lc


class OsaLightCurveQuery(LightCurveQuery):

    def __init__(self, name):

        super(OsaLightCurveQuery, self).__init__(name)

    def get_data_server_query(self, instrument,
                              config=None):

        scwlist_assumption, cat, extramodules, inject=OsaDispatcher.get_osa_query_base(instrument)
        E1=instrument.get_par_by_name('E1_keV').value
        E2=instrument.get_par_by_name('E2_keV').value
        src_name = instrument.get_par_by_name('src_name').value
        delta_t = instrument.get_par_by_name('time_bin')._astropy_time_delta.sec
        target, modules, assume=self.set_instr_dictionaries(extramodules,scwlist_assumption,E1,E2,src_name,delta_t)


        q = OsaDispatcher(config=config, target=target, modules=modules, assume=assume, inject=inject)

        return q


    def set_instr_dictionaries(self, extramodules,scwlist_assumption,E1,E2,src_name,delta_t):
        raise RuntimeError('Must be specified for each instrument')

    def process_product_method(self, instrument, prod_list):
        query_lc = prod_list.get_prod_by_name('isgri_lc')

        #prod_dictionary = {}

        if query_lc is not None and query_lc.data is not None:

            query_lc.write(overwrite=True)

        query_out = QueryOutput()

        #print('query_lc.data',query_lc.data)
        query_out.prod_dictionary['image'] = None
        query_out.prod_dictionary['file_name'] = ''
        query_out.prod_dictionary['download_file_name'] = ''
        query_out.prod_dictionary['prod_process_message'] = 'no light curve produced for name %s' % query_lc.src_name

        if query_lc is not None and query_lc.data is not None:
                html_fig = query_lc.get_html_draw()
                query_out.prod_dictionary['image'] = html_fig
                query_out.prod_dictionary['file_name'] = str(query_lc.file_path.name)
                query_out.prod_dictionary['download_file_name'] = 'light_curve.fits.gz'
                query_out.prod_dictionary['prod_process_message'] = ''


        print('--> send prog')
        return query_out

class IsgriLightCurveQuery(OsaLightCurveQuery):
    def __init__(self,name ):
        super(IsgriLightCurveQuery, self).__init__(name)





    def build_product_list(self,instrument,res,out_dir,prod_prefix=None):

        src_name = instrument.get_par_by_name('src_name').value

        lc = IsgriLigthtCurve.build_from_ddosa_res('isgri_lc', 'query_lc.fits',
                                                   res,
                                                   src_name=src_name,
                                                   prod_prefix=prod_prefix,
                                                   out_dir=out_dir)

        # print('spectrum_list',spectrum_list)
        prod_list=[lc]


        return prod_list

    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2,src_name,delta_t):
        print('-->lc standard mode from scw_list', scwlist_assumption)
        print('-->src_name', src_name)
        target = "lc_pick"

        if extramodules is None:
            extramodules=[]

        modules = ["git://ddosa"]+extramodules+['git://ddosa_delegate']

        assume = ['ddosa.LCGroups(input_scwlist=%s)' % scwlist_assumption[0],
                  scwlist_assumption[1],
                  'ddosa.lc_pick(use_source_names=["%s"])' % src_name,
                  'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,
                                                                                                           E2=E2),
                  'ddosa.LCEnergyBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,
                                                                                                           E2=E2),
                  'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0_p2",use_DoPart2=1)',
                  'ddosa.CatForLC(use_minsig=3)',
                  'ddosa.LCTimeBin(use_time_bin_seconds=%f)' % delta_t]

        return target, modules, assume

    def get_dummy_products(self, instrument, config, out_dir='./'):
        src_name = instrument.get_par_by_name('src_name').value

        dummy_cache = config.dummy_cache
        delta_t = instrument.get_par_by_name('time_bin')._astropy_time_delta.sec
        print('delta_t is sec', delta_t)
        query_lc = LightCurveProduct.from_fits_file(inf_file='%s/query_lc.fits' % dummy_cache,
                                                    out_file_name='query_lc.fits',
                                                    prod_name='isgri_lc',
                                                    ext=1,
                                                    file_dir=out_dir)
        print('name', query_lc.header['NAME'])

        if src_name is not None:
            if query_lc.header['NAME'] != src_name:
                query_lc.data = None

        prod_list = QueryProductList(prod_list=[query_lc])

        return prod_list




