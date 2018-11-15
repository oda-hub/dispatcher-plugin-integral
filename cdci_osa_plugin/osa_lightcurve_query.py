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
from pathlib import Path

from astropy.io import fits as pf
from cdci_data_analysis.analysis.io_helper import FitsFile
from cdci_data_analysis.analysis.queries import LightCurveQuery
from cdci_data_analysis.analysis.products import LightCurveProduct,QueryProductList,QueryOutput
from cdci_data_analysis.analysis.io_helper import FilePath
from cdci_data_analysis.analysis.plot_tools import ScatterPlot
from oda_api.data_products import NumpyDataProduct
from .osa_dataserve_dispatcher import OsaDispatcher
from .osa_common_pars import DummyOsaRes


class IsgriLigthtCurve(LightCurveProduct):
    def __init__(self,
                 name='isgri_lc',
                 file_name=None,
                 data=None,
                 file_dir=None,
                 prod_prefix=None,
                 src_name='',
                 meta_data={}):

        if meta_data == {} or meta_data is None:
            self.meta_data = {'product': 'osa_lc', 'instrument': 'integral', 'src_name': src_name}
        else:
            self.meta_data = meta_data

        self.meta_data['time']='TIME'
        self.meta_data['rate'] ='RATE'
        self.meta_data['rate_err'] = 'ERROR'

        data.name=name


        super(IsgriLigthtCurve, self).__init__(name=name,
                                              data=data,
                                              name_prefix=prod_prefix,
                                              file_dir=file_dir,
                                              file_name=file_name,
                                              meta_data=meta_data)



    @classmethod
    def build_from_ddosa_res(cls,
                             res,
                             src_name='',
                             prod_prefix='',
                             file_dir=None,
                             api=False):



        lc_list = []

        if file_dir is None:
            file_dir = './'

        if prod_prefix is None:
            prod_prefix=''

        for source_name, lightcurve_attr in res.extracted_sources:
            meta_data = {}
            input_lc_paht = getattr(res, lightcurve_attr)
            print('lc file input-->', input_lc_paht, lightcurve_attr)


            #hdu_list = FitsFile(lc_paht).open()
            #data = NumpyDataProduct.from_fits_file(lc_paht, hdu_name='ISGR-SRC.-LCR', name='isgri_lc', instr='isgri',
            #                                       descr='src_name:%s' % name)

            npd = NumpyDataProduct.from_fits_file(input_lc_paht, meta_data=meta_data)

            du = npd.get_data_unit_by_name('ISGR-SRC.-LCR')


            if du is not None:
                src_name = du.header['NAME']

                meta_data['src_name'] = src_name
                meta_data['time_bin'] = du.header['TIMEDEL']

                out_file_name =  Path(input_lc_paht).resolve().stem
                #if prod_prefix !='' and prod_prefix!=None:
                #    out_file_name = prod_prefix + '_' + out_file_name

                print('lc file output-->', out_file_name, lightcurve_attr)


                lc = cls( data=npd, file_name=out_file_name, file_dir=file_dir, prod_prefix=prod_prefix,
                         src_name=src_name,meta_data=meta_data)

                lc_list.append(lc)

        return lc_list








    def get_html_draw(self, plot=False):
        # from astropy.io import fits as pf
        # print ('loading -->',self.file_path.path)

        # hdul = pf.open(self.file_path.path)
        hdul = FitsFile(self.file_path.path).open()

        data = hdul[1].data
        header = hdul[1].header

        import matplotlib
        # matplotlib.use('TkAgg')
        #import pylab as plt
        #fig, ax = plt.subplots()

        #filtering zero flux values
        msk_non_zero = np.count_nonzero([data['RATE'], data['ERROR']], axis=0) > 0
        data=data[msk_non_zero]

        x = data['TIME']
        y = data['RATE']
        dy = data['ERROR']
        mjdref = header['mjdref'] + np.int(x.min())



        x = x - np.int(x.min())

        sp=ScatterPlot(w=600,h=600,x_label='MJD-%d  (days)' % mjdref,y_label='Rate  (cts/s)')
        sp.add_errorbar(x,y,yerr=dy)
        footer_str=''
        try:
            slope = None
            normalized_slope = None
            chisq_red = None
            poly_deg = 0
            p, chisq, chisq_red, dof,xf,yf = self.do_linear_fit(x, y, dy, poly_deg, 'constant fit')
            sp.add_line(xf,yf,'constant fit',color='green')

            exposure = header['TIMEDEL'] * data['FRACEXP'].sum()
            exposure *= 86400.
            footer_str = 'Exposure %5.5f (s) \n' % exposure
            if p is not None:
                footer_str += '\n'
                footer_str += 'Constant fit\n'
                footer_str += 'flux level %5.5f (cts/s)\n' % p[0]
                footer_str += 'dof ' + '%d' % dof + '\n'
                footer_str += 'Chi-squared red. %5.5f\n' % chisq_red

        except:
            pass

        try:
            poly_deg = 1
            p, chisq, chisq_red, dof,xf,yf = self.do_linear_fit(x, y, dy, poly_deg, 'linear fit')
            if p is not None:
                footer_str += '\n'
                footer_str += 'Linear fit\n'
                footer_str += 'slope %5.5f\n' % p[0]
                footer_str += 'dof ' + '%d' % dof + '\n'
                footer_str += 'Chi-squared red. %5.5f\n' % chisq_red

            sp.add_line(xf, yf, 'linear fit',color='orange')
        except:
            pass



        html_dict= sp.get_html_draw()


        res_dict = {}
        res_dict['image'] =html_dict
        res_dict['header_text'] = ''
        res_dict['table_text'] = ''
        res_dict['footer_text'] = footer_str


        return res_dict

    def do_linear_fit(self, x, y, dy, poly_deg, label):

        p = None
        chisq = None
        chisq_red = None
        dof = None
        x_grid = None
        y_grid=None

        if y.size > poly_deg + 1:
            p = np.polyfit(x, y, poly_deg)

            x_grid = np.linspace(x.min(), x.max(), 100)
            lin_fit = np.poly1d(p)

            chisq = (lin_fit(x) - y) ** 2 / dy ** 2
            dof = y.size - (poly_deg + 1)
            chisq_red = chisq.sum() / float(dof)
            #plt.plot(x_grid, lin_fit(x_grid), '--', label=label)
            y_grid=lin_fit(x_grid)

        return p, chisq, chisq_red, dof,x_grid, y_grid




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
        osa_version = instrument.get_par_by_name('osa_version').value
        target, modules, assume=self.set_instr_dictionaries(extramodules,scwlist_assumption,E1,E2,src_name,delta_t,osa_version)



        q = OsaDispatcher(config=config,instrument=instrument, target=target, modules=modules, assume=assume, inject=inject)

        return q


    def set_instr_dictionaries(self, extramodules,scwlist_assumption,E1,E2,src_name,delta_t):
        raise RuntimeError('Must be specified for each instrument')


    def process_product_method(self, instrument, prod_list,api=False):

        _names = []
        _lc_path = []
        _html_fig = []
        _data_list=[]

        for query_lc in prod_list.prod_list:
            #print('--> lc  name',query_lc.name)
            #print('-->file name', query_lc.file_path.path)
            query_lc.write()

            if api==False:
                _names.append(query_lc.meta_data['src_name'])
                _lc_path.append(str(query_lc.file_path.name))
                _html_fig.append(query_lc.get_html_draw())


            if api==True:
                _data_list.append(query_lc.data)






        query_out = QueryOutput()

        if api == True:
            query_out.prod_dictionary['numpy_data_product_list'] = _data_list

        else:
            query_out.prod_dictionary['name'] = _names
            query_out.prod_dictionary['file_name'] = _lc_path
            query_out.prod_dictionary['image'] =_html_fig
            query_out.prod_dictionary['download_file_name'] = 'light_curve.fits.gz'

        query_out.prod_dictionary['prod_process_message'] = ''

        return query_out

class IsgriLightCurveQuery(OsaLightCurveQuery):
    def __init__(self,name ):
        super(IsgriLightCurveQuery, self).__init__(name)





    def build_product_list(self,instrument,res,out_dir,prod_prefix=None,api=False):
        meta_data = {'product': 'light_curve', 'instrument': 'isgri', 'src_name': ''}
        meta_data['query_parameters'] = self.get_parameters_list_as_json()

        prod_list = IsgriLigthtCurve.build_from_ddosa_res(res,
                                                          prod_prefix=prod_prefix,
                                                          file_dir=out_dir,
                                                          api=api)


        return prod_list



    def get_dummy_products(self, instrument, config, out_dir='./',prod_prefix=None,api=False):


        meta_data = {'product': 'light_curve', 'instrument': 'isgri', 'src_name': ''}
        meta_data['query_parameters'] = self.get_parameters_list_as_json()


        dummy_cache = config.dummy_cache


        res =DummyOsaRes()
        res.__setattr__('dummy_src','dummy_src')
        res.__setattr__('dummy_lc','%s/light_curve.fits.gz' % dummy_cache)
        res.__setattr__('extracted_sources',[('dummy_src','dummy_lc')])



        prod_list = IsgriLigthtCurve.build_from_ddosa_res(res,
                                                          prod_prefix=prod_prefix,
                                                          file_dir=out_dir,
                                                          api=api)


        prod_list = QueryProductList(prod_list=prod_list)

        return prod_list

    def set_instr_dictionaries(self, extramodules, scwlist_assumption, E1, E2, src_name, delta_t, osa_version="OSA10.2"):
        print('-->lc standard mode from scw_list', scwlist_assumption)
        print('-->src_name', src_name)
        target = "ISGRILCSum"

        if extramodules is None:
            extramodules = []

        if osa_version == "OSA10.2":
            modules = ["git://ddosa"] + extramodules + ['git://process_isgri_lc', 'git://ddosa_delegate']
        elif osa_version == "OSA11.0":
            modules = ["git://ddosa", "git://findic/icversion","git://ddosa11/icversion"] + extramodules + ['git://process_isgri_lc', 'git://ddosa_delegate']
        else:
            raise Exception("unknown osa version: "+osa_version)
                 

        assume = ['process_isgri_lc.ScWLCList(input_scwlist=%s)' % scwlist_assumption[0],
                  scwlist_assumption[1],
                  'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,
                                                                                                           E2=E2),
                  'ddosa.LCEnergyBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,
                                                                                                              E2=E2),
                  'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0_p2",use_DoPart2=1)',
                  'ddosa.CatForLC(use_minsig=3)',
                  'ddosa.LCTimeBin(use_time_bin_seconds=%f)' % delta_t]

        return target, modules, assume






