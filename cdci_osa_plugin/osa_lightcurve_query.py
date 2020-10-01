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


import ddaclient as dc

# Project
# relative import eg: from .mod import f
import  numpy as np
from numpy.lib.recfunctions import append_fields
from pathlib import Path

from astropy.io import fits as pf
from cdci_data_analysis.analysis.io_helper import FitsFile
from cdci_data_analysis.analysis.queries import LightCurveQuery,ProductQuery
from cdci_data_analysis.analysis.products import LightCurveProduct,QueryProductList,QueryOutput
from cdci_data_analysis.analysis.io_helper import FilePath
from cdci_data_analysis.analysis.parameters import TimeDelta
from cdci_data_analysis.analysis.plot_tools import ScatterPlot
from oda_api.data_products import NumpyDataProduct
from .osa_dataserve_dispatcher import OsaDispatcher
from .osa_common_pars import DummyOsaRes


class OsaLigthtCurve(LightCurveProduct):
    def __init__(self,
                 name='osa_lc',
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

        super(OsaLigthtCurve, self).__init__(name=name,
                                             data=data,
                                             name_prefix=prod_prefix,
                                             file_dir=file_dir,
                                             file_name=file_name,
                                             meta_data=meta_data)

    #@staticmethod
    #def make_ogip_compliant(du):
    #    timedel = du.header['TIMEDEL']
    #    timepix = du.header['TIMEPIXR']
    #    t_lc = du.data['TIME'] + (0.5 - timepix) * timedel
    #    dt_lc = (timedel / 2) * np.ones(t_lc.shape)
    #
    #    for i in range(len(t_lc) - 1):
    #        dt_lc[i + 1] = min(timedel / 2, t_lc[i + 1] - t_lc[i] - dt_lc[i])
    #
    #    _d = np.array(du.data)
    #    _o = append_fields(_d, 'TIMEDEL', dt_lc*2)
    #    du.data = _o.data


    @classmethod
    def build_isgri_lc_from_ddosa_res(cls,
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

            npd = NumpyDataProduct.from_fits_file(input_lc_paht, meta_data=meta_data)

            du = npd.get_data_unit_by_name('ISGR-SRC.-LCR')

            if du is not None:
                src_name = du.header['NAME']

                meta_data['src_name'] = src_name
                meta_data['time_bin'] = du.header['TIMEDEL']

                out_file_name =  Path(input_lc_paht).resolve().stem

                #OsaLigthtCurve.make_ogip_compliant(du)

                lc = cls(name='isgri_lc', data=npd, file_name=out_file_name, file_dir=file_dir, prod_prefix=prod_prefix,
                         src_name=src_name,meta_data=meta_data)


                lc_list.append(lc)

        return lc_list

    @classmethod
    def build_jemx_lc_from_ddosa_res(cls,
                                      res,
                                      src_name='',
                                      prod_prefix='',
                                      file_dir=None,
                                      api=False):

        lc_list=[]

        lc_path_list = [getattr(res,attr) for attr in dir(res) if attr.startswith("lc_")]

        src_name_list = [attr for attr in dir(res) if attr.startswith("lc_")]

        src_name_list = [n.replace('lc_','') for n in src_name_list]
        src_name_list = [n.replace('_', ' ') for n in src_name_list]
        print ('->',lc_path_list,src_name_list)

        if file_dir is None:
            file_dir = './'

        if prod_prefix is None:
            prod_prefix = ''

        for source_name, input_lc_paht in zip(src_name_list,lc_path_list):
            meta_data = {}

            npd = NumpyDataProduct.from_fits_file(input_lc_paht, meta_data=meta_data)



            du = npd.get_data_unit_by_name('JMX2-SRC.-LCR')

            if du is None:
                du = npd.get_data_unit_by_name('JMX1-SRC.-LCR')

            if du is None:
                # warning, this one is empty (add to warning list)
                continue
 #               raise RuntimeError('Missing data unit with light curve in the fits file')

            if du is not None:

                meta_data['src_name'] = source_name
                meta_data['time_bin'] = du.header['TIMEDEL']

                out_file_name = Path(input_lc_paht).resolve().stem

                #OsaLigthtCurve.make_ogip_compliant(du)

                lc = cls(name='jemx_lc', data=npd, file_name=out_file_name, file_dir=file_dir, prod_prefix=prod_prefix,
                         src_name=src_name, meta_data=meta_data)

                lc_list.append(lc)

        return lc_list



    def get_html_draw(self, plot=False):
        #
        npd = NumpyDataProduct.from_fits_file(self.file_path.path)

        du = npd.get_data_unit_by_name('ISGR-SRC.-LCR')

        if du is None:
            du = npd.get_data_unit_by_name('JMX2-SRC.-LCR')

        if du is None:
            du = npd.get_data_unit_by_name('JMX1-SRC.-LCR')

        if du is None:
            raise RuntimeError('du with lc not found in fits file')

        data = du.data
        header = du.header

        #filtering zero flux values
        msk_non_zero = np.count_nonzero([data['RATE'], data['ERROR']], axis=0) > 0
        data=data[msk_non_zero]

        x = data['TIME']
        dx= data['TIMEDEL']*0.5
        y = data['RATE']
        dy = data['ERROR']
        try:
            mjdref = header['mjdref'] + np.int(x.min())
        except:
            mjdref = header['MJDREF'] + np.int(x.min())


        x = x - np.int(x.min())

        sp=ScatterPlot(w=600,h=600,x_label='MJD-%d  (days)' % mjdref,y_label='Rate  (cts/s)')
        sp.add_errorbar(x,y,yerr=dy,xerr=dx)

        footer_str=None
        if self.name == 'jemx_lc':
            exposure = np.sum(data['FRACEXP']) * du.header['TIMEDEL']
            exposure *= 86400.
        elif self.name == 'isgri_lc':
            exposure = np.sum(data['FRACEXP'] * du.header['XAX_E']) * 2
            exposure *= 86400.
        else:
            # TODO update this option
            footer_str ='Exposure non evaluated for product  %s'%self.name

        if footer_str!=None:
            footer_str = 'Exposure %5.5f (s) \n' % exposure

        try:
            slope = None
            normalized_slope = None
            chisq_red = None
            poly_deg = 0
            p, chisq, chisq_red, dof,xf,yf = self.do_linear_fit(x, y, dy, poly_deg, 'constant fit')
            sp.add_line(xf,yf,'constant fit',color='green')

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
            y_grid=lin_fit(x_grid)

        return p, chisq, chisq_red, dof,x_grid, y_grid




class OSATimebin(TimeDelta):
    def __init__(self,
                 value=None,
                 delta_T_format_name=None,
                 name=None):
        self.t_bin_max_seconds = 4000.
        super(OSATimebin, self).__init__(value=value,
                                        delta_T_format_name=delta_T_format_name,
                                        name=name)



    @property
    def value(self):
        return self._astropy_time_delta.value

    @value.setter
    def value(self, v):
        units = self.units
        self._set_time(v, format=units)
        print('setting time bine to',v)
        if self._astropy_time_delta.sec>self.t_bin_max_seconds:
            raise RuntimeError('Time bin max value exceeded =%f'%self.t_bin_max_seconds)

class OsaLightCurveQuery(ProductQuery):
    def __init__(self, name):

        #super(OsaLightCurveQuery, self).__init__(name)

        # TODO define TimeDelta parameter with max value = 3ks
        # TODO done, verify

        osa_time_bin = OSATimebin(value=1000., name='time_bin', delta_T_format_name='time_bin_format')

        parameters_list = [osa_time_bin]
        super(OsaLightCurveQuery, self).__init__(name, parameters_list)


    def get_data_server_query(self, instrument,
                              config=None):

        scwlist_assumption, cat, extramodules, inject=OsaDispatcher.get_osa_query_base(instrument)
        E1=instrument.get_par_by_name('E1_keV').value
        E2=instrument.get_par_by_name('E2_keV').value
        src_name = instrument.get_par_by_name('src_name').value
        delta_t = instrument.get_par_by_name('time_bin')._astropy_time_delta.sec
        osa_version = instrument.get_par_by_name('osa_version').value
        if (isinstance(self,JemxLightCurveQuery)):
            jemx_num = instrument.get_par_by_name('jemx_num').value
            target, modules, assume=self.set_instr_dictionaries(extramodules, scwlist_assumption, E1, E2, src_name,
                                                                  delta_t, osa_version=osa_version,jemx_num=jemx_num)
        else:
            target, modules, assume = self.set_instr_dictionaries(extramodules, scwlist_assumption, E1, E2, src_name,
                                                                  delta_t, osa_version=osa_version)


        q = OsaDispatcher(config=config,instrument=instrument, target=target, modules=modules, assume=assume, inject=inject)
        return q

    def set_instr_dictionaries(self, extramodules,scwlist_assumption,E1,E2,src_name,delta_t):
        raise RuntimeError('Must be specified for each instrument')

    def process_product_method(self, instrument, prod_list,api=False, **kw):

        _names = []
        _lc_path = []
        _html_fig = []
        _data_list=[]

        for query_lc in prod_list.prod_list:
            #print('--> lc  name',query_lc.name)
            #print('-->file name', query_lc.file_path.path)

            query_lc.add_url_to_fits_file(instrument._current_par_dic, url=instrument.disp_conf.products_url)
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

        prod_list = OsaLigthtCurve.build_isgri_lc_from_ddosa_res(res,
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
        res.__setattr__('dummy_lc','%s/isgri_query_lc.fits' % dummy_cache)
        res.__setattr__('extracted_sources',[('dummy_src','dummy_lc')])



        prod_list = OsaLigthtCurve.build_isgri_lc_from_ddosa_res(res,
                                                                 prod_prefix=prod_prefix,
                                                                 file_dir=out_dir,
                                                                 api=api)


        prod_list = QueryProductList(prod_list=prod_list)

        return prod_list

    def set_instr_dictionaries(self, extramodules, scwlist_assumption, E1, E2, src_name, delta_t, osa_version="OSA10.2"):
        #print('-->lc standard mode from scw_list', scwlist_assumption)
        #print('-->src_name', src_name)
        target = "ISGRILCSum"

        if extramodules is None:
            extramodules = []

        if osa_version == "OSA10.2":
            modules = ["git://ddosa/staging-1-3",'git://process_isgri_lc'] + extramodules + ['git://ddosa_delegate/staging-1-3']
        elif osa_version == "OSA11.0":
            modules = ["git://ddosa/staging-1-3", "git://findic/icversionpy37","git://ddosa11/icversion",'git://process_isgri_lc'] + extramodules + ['git://ddosa_delegate/staging-1-3']
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







class JemxLightCurveQuery(OsaLightCurveQuery):

    def __init__(self, name):
        super(JemxLightCurveQuery, self).__init__(name)

    def build_product_list(self, instrument, res, out_dir, prod_prefix=None, api=False):
        meta_data = {'product': 'light_curve', 'instrument': 'jemx', 'src_name': ''}
        meta_data['query_parameters'] = self.get_parameters_list_as_json()

        prod_list = OsaLigthtCurve.build_jemx_lc_from_ddosa_res(res,
                                                                 prod_prefix=prod_prefix,
                                                                 file_dir=out_dir,
                                                                 api=api)

        return prod_list

    def get_dummy_products(self, instrument, config, out_dir='./', prod_prefix=None, api=False):

        meta_data = {'product': 'light_curve', 'instrument': 'jemx', 'src_name': ''}
        meta_data['query_parameters'] = self.get_parameters_list_as_json()

        dummy_cache = config.dummy_cache

        res = DummyOsaRes()

        res.__setattr__('lc_crab', '%s/jemx_query_lc.fits.gz' % dummy_cache)
        #res.__setattr__('extracted_sources', [('dummy_src', 'dummy_lc')])

        prod_list = OsaLigthtCurve.build_jemx_lc_from_ddosa_res(res,
                                                                 prod_prefix=prod_prefix,
                                                                 file_dir=out_dir,
                                                                 api=api)

        prod_list = QueryProductList(prod_list=prod_list)

        return prod_list

    def set_instr_dictionaries(self, extramodules, scwlist_assumption, E1, E2, src_name, delta_t,jemx_num,
                               osa_version="OSA10.2"):

        target = "lc_pick"

        if extramodules is None:
            extramodules = []

        if osa_version == "OSA10.2":
            modules = ["git://ddosa/staging-1-3","git://ddjemx"] + extramodules +['git://ddosa_delegate/staging-1-3']
        elif osa_version == "OSA11.0":

            modules = ["git://ddosa/staging-1-3","git://ddjemx", "git://findic/icversionpy37", "git://ddosa11/icversion"] \
                      + extramodules + ['git://ddosa_delegate/staging-1-3']
        else:
            raise Exception("unknown osa version: " + osa_version)


        assume = ['ddjemx.JMXLCGroups(input_scwlist=%s)' % scwlist_assumption[0],
                  scwlist_assumption[1],
                  'ddjemx.JEnergyBinsLC (use_bins=[(%(E1)s,%(E2)s)])' % dict(E1=E1, E2=E2),
                  'ddjemx.LCTimeBin(use_time_bin_seconds=%f)' % delta_t,
                  'ddjemx.JEMX(use_num=%d)' % jemx_num]

        return target, modules, assume



