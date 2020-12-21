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

from astropy.io import  fits as pf
import glob

from pathlib import Path

from cdci_data_analysis.analysis.io_helper import FitsFile
from cdci_data_analysis.analysis.queries import SpectrumQuery
from cdci_data_analysis.analysis.products import SpectrumProduct,QueryProductList,QueryOutput
from cdci_data_analysis.analysis.io_helper import FilePath

from .osa_dataserve_dispatcher import    OsaDispatcher
from oda_api.data_products import NumpyDataProduct

from .osa_common_pars import  DummyOsaRes


class JemxSpectrumProduct(SpectrumProduct):
    def __init__(self,name,file_name,data,prod_prefix=None,file_dir=None,meta_data={},rmf_file=None,arf_file=None):

        super(JemxSpectrumProduct, self).__init__(name=name,
                                                   data=data,
                                                   file_name=file_name,
                                                   name_prefix=prod_prefix,
                                                   file_dir=file_dir,
                                                   rmf_file=rmf_file,
                                                   arf_file=arf_file,
                                                   meta_data=meta_data)

    @classmethod
    def build_list_from_ddosa_res(cls, res, prod_prefix=None, out_dir=None,jemx_num=2):
        #print(dir(res),res)
        spec_list_attr = [attr for attr in dir(res) if  attr.startswith("spectrum_")]
        arf_list_attr = [attr for attr in dir(res) if attr.startswith("arf_")]
        rmf_list_attr = [attr for attr in dir(res) if attr.startswith("rmf_")]
        source_name_list=[n.split('_')[1] for n in spec_list_attr ]
        #print('jemx',spec_list_attr,arf_list_attr,source_name_list)
        #import pickle
        #for s in spec_list:
        #    print('jemx specrtrum',s)
        #with open('res.pkl','rb') as f:
        #    pickle.dump(res,f)

        spec_list = []

        if out_dir is None:
            out_dir = './'

        for source_name,spec_attr, arf_attr, rmf_attr in zip(source_name_list,spec_list_attr,arf_list_attr,rmf_list_attr):
            #for source_name, spec_attr, rmf_attr, arf_attr in res.extracted_sources:

            #source_name=1
            #spec_attr=1
            #arf_attr=2





            spec_filename = getattr(res, spec_attr)
            arf_filename=  getattr(res, arf_attr)
            rmf_filename = getattr(res, rmf_attr)

            print('jemx_num',jemx_num)
            print('spec in file-->', spec_filename)
            print('arf  in file-->', arf_filename)
            print('rmf  in file-->', rmf_filename)

            out_spec_file = Path(spec_filename).name.replace(' ','_')

            out_arf_file = Path(arf_attr).name.replace(' ','_')+'.fits.gz'

            out_rmf_file =  Path(rmf_attr).name.replace(' ','_')+'.fits.gz'



            name = 'jemx_arf'
            meta_data = {}
            meta_data['src_name'] = source_name
            meta_data['product'] = 'jemx_arf'
            np_arf = NumpyDataProduct.from_fits_file(arf_filename, meta_data=meta_data)

            np_arf.get_data_unit_by_name('JMX%d-AXIS-ARF'%jemx_num).name='SPECRESP'

            arf = cls(name=name, data=np_arf, file_name=out_arf_file, file_dir=out_dir, prod_prefix=prod_prefix,
                       meta_data=meta_data)


            name = 'jemx_rmf'
            meta_data = {}
            meta_data['src_name'] = source_name
            meta_data['product'] = 'jemx_rmf'
            meta_data['product'] = 'jemx_rmf'
            np_rmf = NumpyDataProduct.from_fits_file(rmf_filename, meta_data=meta_data)

            np_rmf.get_data_unit_by_name('JMX%d-RMF.-RSP'%jemx_num).name='SPECRESP MATRIX'

            np_rmf.get_data_unit_by_name('JMX%d-FBDS-MOD'%jemx_num).name='EBOUNDS'

            rmf = cls(name=name, data=np_rmf, file_name=out_rmf_file, file_dir=out_dir, prod_prefix=prod_prefix,
                      meta_data=meta_data)

            name = 'jemx_spectrum'
            meta_data = {}
            meta_data['src_name'] = source_name
            meta_data['product'] = 'jemx_spectrum'

            np_spec = NumpyDataProduct.from_fits_file(spec_filename, meta_data=meta_data)
            np_spec.data_unit[1].header['ANCRFILE'] = 'NONE'
            np_spec.data_unit[1].header['RESPFILE'] = 'NONE'

            spec = cls(name=name, data=np_spec, file_name=out_spec_file, file_dir=out_dir, prod_prefix=prod_prefix,
                       meta_data=meta_data,rmf_file=rmf.file_path.name,arf_file=arf.file_path.name)


            #print('spec out file-->', out_spec_file)
            #print('arf  out file-->', out_arf_file)
            #print('rmf  out file-->', out_rmf_file)


            spec_list.append(spec)
            spec_list.append(arf)
            spec_list.append(rmf)

        return spec_list

class IsgriSpectrumProduct(SpectrumProduct):

    def __init__(self,name,file_name,data,prod_prefix=None,file_dir=None,meta_data={},rmf_file=None,arf_file=None):

        super(IsgriSpectrumProduct, self).__init__(name=name,
                                                   data=data,
                                                   file_name=file_name,
                                                   name_prefix=prod_prefix,
                                                   file_dir=file_dir,
                                                   rmf_file=rmf_file,
                                                   arf_file=arf_file,
                                                   meta_data=meta_data)




    @classmethod
    def build_list_from_ddosa_res(cls,res,prod_prefix=None,out_dir=None):



        spec_list=[]

        if out_dir is None:
            out_dir='./'
        for source_name, spec_attr, rmf_attr, arf_attr in res.extracted_sources:

            #print('spec in file-->',getattr(res, spec_attr), spec_attr)
            #print('arf  in file-->', getattr(res, arf_attr), arf_attr)
            #print('rmf  in file-->', getattr(res, rmf_attr), rmf_attr)

            spec_filename = getattr(res, spec_attr)
            arf_filename= getattr(res, arf_attr)

            rmf_filename = getattr(res, rmf_attr)

            out_spec_file = Path(getattr(res, spec_attr)).name

            out_arf_file = Path(getattr(res, arf_attr)).name

            out_rmf_file = Path(out_dir, getattr(res, rmf_attr)).name



            name = 'isgri_arf'
            meta_data = {}
            meta_data['src_name'] = source_name
            meta_data['product'] = 'isgri_arf'
            np_arf = NumpyDataProduct.from_fits_file(arf_filename, meta_data=meta_data)

            np_arf.get_data_unit_by_name('ISGR-ARF.-RSP').name='SPECRESP'

            arf = cls(name=name, data=np_arf, file_name=out_arf_file, file_dir=out_dir, prod_prefix=prod_prefix,
                       meta_data=meta_data)



            name = 'isgri_rmf'
            meta_data = {}
            meta_data['src_name'] = source_name
            meta_data['product'] = 'isgri_rmf'
            meta_data['product'] = 'isgri_rmf'
            np_rmf = NumpyDataProduct.from_fits_file(rmf_filename, meta_data=meta_data)

            np_rmf.get_data_unit_by_name('ISGR-RMF.-RSP').name= 'SPECRESP MATRIX'

            np_rmf.get_data_unit_by_name('ISGR-EBDS-MOD').name='EBOUNDS'

            rmf = cls(name=name, data=np_rmf, file_name=out_rmf_file, file_dir=out_dir, prod_prefix=prod_prefix,
                      meta_data=meta_data)

            name = 'isgri_spectrum'
            meta_data = {}
            meta_data['src_name'] = source_name
            meta_data['product'] = 'isgri_spectrum'

            np_spec = NumpyDataProduct.from_fits_file(spec_filename, meta_data=meta_data)
            np_spec.data_unit[1].header['ANCRFILE'] = 'NONE'
            np_spec.data_unit[1].header['RESPFILE'] = 'NONE'

            spec = cls(name=name, data=np_spec, file_name=out_spec_file, file_dir=out_dir, prod_prefix=prod_prefix,
                       meta_data=meta_data,rmf_file=rmf.file_path.name,arf_file=arf.file_path.name)


            #print('spec out file-->', out_spec_file)
            #print('arf  out file-->', out_arf_file)
            #print('rmf  out file-->', out_rmf_file)


            spec_list.append(spec)
            spec_list.append(arf)
            spec_list.append(rmf)

        return spec_list



class OsaSpectrumQuery(SpectrumQuery):

    def __init__(self, name):

        super(OsaSpectrumQuery, self).__init__(name)

    def get_data_server_query(self, instrument,
                              config=None):


        scwlist_assumption, cat, extramodules, inject=OsaDispatcher.get_osa_query_base(instrument)
        E1=instrument.get_par_by_name('E1_keV').value
        E2=instrument.get_par_by_name('E2_keV').value
        osa_version=instrument.get_par_by_name('osa_version').value

        if (isinstance(self,JemxSpectrumQuery)):
            jemx_num = instrument.get_par_by_name('jemx_num').value
            target, modules, assume=self.set_instr_dictionaries(extramodules,scwlist_assumption,E1,E2,osa_version,jemx_num=jemx_num)
        else:
            target, modules, assume = self.set_instr_dictionaries(extramodules, scwlist_assumption, E1, E2, osa_version)
        q=OsaDispatcher(config=config, target=target, modules=modules, assume=assume, inject=inject,instrument=instrument)

        return q


    def set_instr_dictionaries(self,catalog,):
        raise RuntimeError('Must be specified for each instrument')

    def process_product_method(self, instrument, prod_list,api=False,**kw):

        #print('process_product_method,api',api)
        _names = []

        _sepc_path = []
        _arf_path = []
        _rmf_path = []

        query_out = QueryOutput()
        for query_spec in prod_list.prod_list:
            #print('jemx',query_spec)
            if query_spec is not None:
                #print('jemx', query_spec.name)

                query_spec.add_url_to_fits_file(instrument._current_par_dic, url=instrument.disp_conf.products_url)
                query_spec.write()

                if query_spec.name=='isgri_spectrum' or  query_spec.name=='jemx_spectrum':
                    _names.append(query_spec.meta_data['src_name'])
                    _sepc_path.append(str(query_spec.file_path.name))
                    _arf_path.append(str(query_spec.arf_file))
                    _rmf_path.append(str(query_spec.rmf_file))

        #print (_names,_sepc_path,_arf_path,_rmf_path)

        if api==False:


            query_out.prod_dictionary['spectrum_name'] = _names
            query_out.prod_dictionary['ph_file_name'] = _sepc_path
            query_out.prod_dictionary['arf_file_name'] = _arf_path
            query_out.prod_dictionary['rmf_file_name'] = _rmf_path

            query_out.prod_dictionary['download_file_name'] = 'spectra.tar.gz'
        else:
            spec_list=[]
            for query_spec in prod_list.prod_list:

                spec_list.append(query_spec.data)

            query_out.prod_dictionary['numpy_data_product_list'] = spec_list

        query_out.prod_dictionary['prod_process_message'] = ''
        return query_out



class IsgriSpectrumQuery(OsaSpectrumQuery):
    def __init__(self,name ):
        super(IsgriSpectrumQuery, self).__init__(name)







    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2,osa_version="OSA10.2"):
        target = "ISGRISpectraSum"

        versions = osa_version.split("-", 1)
        if len(versions) == 1:
            osa_version_base, osa_subversion = versions[0], 'default-isdc'
        elif len(versions) == 2:
            osa_version_base, osa_subversion = versions
        else:
            raise RuntimeError(f"this should not be possible, OSA version split did not split as it should")

        #TODO: this really should be re-used
        if osa_version_base == "OSA10.2":
            modules = ["git://ddosa/staging-1-3","git://useresponse/staging-1-3", "git://process_isgri_spectra/osa10-staging-1-3"]
        elif osa_version_base == "OSA11.0":
            modules = ["git://ddosa/staging-1-3","git://findic/staging-1-3-icversion","git://ddosa11/staging-1-3"] 
            modules += ["git://useresponse/staging-1-3-osa11", "git://process_isgri_spectra/staging-1-3-osa11"]
        else:
            raise RuntimeError(f"unknown OSA version {osa_version_base}, complete version {osa_version}")

        modules += [ "git://rangequery/staging-1-3"]+extramodules+['git://ddosa_delegate/staging-1-3']



        assume = ['process_isgri_spectra.ScWSpectraList(input_scwlist=%s)'% scwlist_assumption[0],
                   scwlist_assumption[1],
                  'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")' % dict(E1=E1,E2=E2),
                  'process_isgri_spectra.ISGRISpectraSum(use_extract_all=True)',
                  'ddosa.ImagingConfig(use_SouFit=0,use_DoPart2=1,use_version="soufit0_p2")',
                  'ddosa.CatForSpectraFromImaging(use_minsig=3)',
                  ]
 
        if osa_subversion != "default-isdc":
            assume.append(f'ddosa.ICRoot(use_ic_root_version="{osa_subversion}")')

        #print ('ciccio',target,modules,assume)
        return target,modules,assume




    def build_product_list(self,instrument,res,out_dir,prod_prefix='query_spectrum',api=False):

        spectrum_list = IsgriSpectrumProduct.build_list_from_ddosa_res(res,
                                                                       out_dir=out_dir,
                                                                       prod_prefix=prod_prefix)

        prod_list = spectrum_list

        return prod_list
    def get_dummy_products(self,instrument,config,out_dir='./',prod_prefix='query_spectrum',api=False):

        if out_dir is None:
            out_dir = './'

        #print ('config.dummy_cache',config.dummy_cache)
        #print ('out_dir',out_dir)
        spec_files=glob.glob(config.dummy_cache+'/query_spectrum_isgri_sum*.fits*')
        arf_files=glob.glob(config.dummy_cache+'/query_spectrum_rmf_sum_*.fits*')
        rmf_files=glob.glob(config.dummy_cache+'/query_spectrum_arf_sum_*.fits*')

        spec_files.sort()
        arf_files.sort()
        rmf_files.sort()
        print('==>,spec_files',spec_files )
        print('==>,arf_files',arf_files )
        print('==>,rmf_files', rmf_files)
        res = DummyOsaRes()

        extracted_sources = []
        for ID,spec_file in enumerate(spec_files):

            name=spec_file.split('sum_')[-1].replace('.fits','')
            name=name.replace('.gz', '')
            name=name.replace('query_spectrum','')

            res.__setattr__(name, name)
            res.__setattr__(name+'_spec', spec_file)
            res.__setattr__(name+'_rmf', arf_files[ID])
            res.__setattr__(name+'_arf', rmf_files[ID])

            extracted_sources.append((name,name+'_spec',name + '_rmf',name+'_arf'))


        res.__setattr__('extracted_sources',extracted_sources)


        spectrum_list = IsgriSpectrumProduct.build_list_from_ddosa_res(res,
                                                                       out_dir=out_dir,
                                                                       prod_prefix=None)

        prod_list = QueryProductList(prod_list=spectrum_list)

        return prod_list




class JemxSpectrumQuery(OsaSpectrumQuery):
    def __init__(self,name ):
        super(JemxSpectrumQuery, self).__init__(name)







    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2,osa_version="OSA10.2",jemx_num=2):
        target = "spe_pick"


        if osa_version=="OSA10.2":
            modules = ["git://ddosa/staging-1-3","git://ddjemx","git://rangequery/staging-1-3"]+extramodules+['git://ddosa_delegate/staging-1-3']
        elif osa_version=="OSA11.0":
            modules = ["git://ddosa/staging-1-3","git://ddjemx","git://findic/icversionpy37","git://ddosa11/icversion",
                               "git://rangequery/staging-1-3"]+extramodules+['git://ddosa_delegate/staging-1-3']
        else:
            raise Exception("unknown OSA version "+osa_version)

        assume = ['ddjemx.JMXSpectraGroups(input_scwlist=%s)'% scwlist_assumption[0],
                   scwlist_assumption[1],
                  'ddjemx.JEnergyBins(use_bins=[(%(E1)s,%(E2)s)])' % dict(E1=E1, E2=E2),
                  'ddjemx.JEMX(use_num=%d)'%jemx_num,
                  'ddjemx.JEnergyBins(use_nchanpow=-4)']





        #print ('jemx',target,modules,assume)
        return target,modules,assume




    def build_product_list(self,instrument,res,out_dir,prod_prefix='query_spectrum',api=False):

        jemx_num = instrument.get_par_by_name('jemx_num').value
        spectrum_list = JemxSpectrumProduct.build_list_from_ddosa_res(res,
                                                                       out_dir=out_dir,
                                                                       prod_prefix=prod_prefix,
                                                                       jemx_num=jemx_num)

        prod_list = spectrum_list

        return prod_list

    def get_dummy_products(self,instrument,config,out_dir='./',prod_prefix='query_spectrum',api=False):

        if out_dir is None:
            out_dir = './'

        #print ('config.dummy_cache',config.dummy_cache)
        #print ('out_dir',out_dir)
        #spec_files=glob.glob(config.dummy_cache+'/jemx_query_spectrum_spec_Crab_pha.fits.gz')
        #arf_files=glob.glob(config.dummy_cache+'/jemx_query_spectrum_arf_Crab.fits.gz')
        #rmf_files=glob.glob(config.dummy_cache+'/jemx_query_spectrum_rmf_Crab.fits.gz')


        res = DummyOsaRes()

        name='query_spectrum_Carb'
        res.__setattr__(name, name)
        res.__setattr__('spectrum_Crab', config.dummy_cache+'/jemx_query_spectrum_spec_Crab_pha.fits.gz')
        res.__setattr__('rmf_Crab', config.dummy_cache+'/jemx_query_spectrum_rmf_Crab.fits.gz')
        res.__setattr__('arf_Crab', config.dummy_cache+'/jemx_query_spectrum_arf_Crab.fits.gz')




        spectrum_list = JemxSpectrumProduct.build_list_from_ddosa_res(res,
                                                                       out_dir=out_dir,
                                                                       prod_prefix=None,
                                                                       jemx_num=2)

        prod_list = QueryProductList(prod_list=spectrum_list)

        return prod_list





