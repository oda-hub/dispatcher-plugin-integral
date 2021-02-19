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

import  os

import logging

# Project
# relative import eg: from .mod import f
from astropy.io import  fits as pf

from cdci_data_analysis.analysis.queries import ImageQuery
from cdci_data_analysis.analysis.products import QueryProductList,CatalogProduct,ImageProduct,QueryOutput
from cdci_data_analysis.analysis.catalog import BasicCatalog
from cdci_data_analysis.analysis.io_helper import  FitsFile
from cdci_data_analysis.analysis.exceptions import RequestNotUnderstood, MissingParameter
from oda_api.data_products import NumpyDataProduct,NumpyDataUnit
import  numpy as np
from .osa_catalog import  OsaIsgriCatalog,OsaJemxCatalog
from .osa_dataserve_dispatcher import    OsaDispatcher
from .osa_common_pars import DummyOsaRes


logger = logging.getLogger(__name__)

class DataAccessIssue(Exception):
    pass

class OsaImageProduct(ImageProduct):

    def __init__(self,
                 name='mosaic_image',
                 file_name=None,
                 data=None,
                 file_dir=None,
                 prod_prefix=None,
                 meta_data={}):

        if meta_data=={} or meta_data is None:
            self.meta_data = {'product':'osa_mosaic','instrument': 'integral', 'src_name': ''}
        else:
            self.meta_data=meta_data
        data.name=name
        super(OsaImageProduct, self).__init__(name=name,
                                              data=data,
                                              name_prefix=prod_prefix,
                                              file_dir=file_dir,
                                              file_name=file_name,
                                              meta_data=meta_data)





    @classmethod
    def build_from_ddosa_skyima(cls,file_name=None,skyima=None,ext=None,file_dir=None,prod_prefix=None,meta_data={}):
        try:
            data=NumpyDataProduct.from_fits_file(skyima,ext=None,meta_data=meta_data)
        except Exception as e:
            logger.error(f"issue while reading skyima {skyima}: {repr(e)}")
            raise DataAccessIssue(f"issue while reading skyima {skyima}: {repr(e)}")

        return  cls(data=data,file_dir=file_dir,prod_prefix=prod_prefix,file_name=file_name,meta_data=meta_data)

    @classmethod
    def build_empty(cls,file_name=None,file_dir=None,prod_prefix=None,meta_data={}):
        ima = NumpyDataUnit(np.zeros((100, 100)), hdu_type='image')
        data = NumpyDataProduct(data_unit=ima)
        return  cls(data=data,file_dir=file_dir,prod_prefix=prod_prefix,file_name=file_name,meta_data=meta_data)



class OsaMosaicQuery(ImageQuery):

    def __init__(self,name):

        super(OsaMosaicQuery, self).__init__(name)


    def get_data_server_query(self,instrument,
                              config=None):


        scwlist_assumption, cat, extramodules, inject=OsaDispatcher.get_osa_query_base(instrument)
        E1=instrument.get_par_by_name('E1_keV').value
        E2=instrument.get_par_by_name('E2_keV').value
        osa_version = instrument.get_par_by_name('osa_version').value
        if (isinstance(self, JemxMosaicQuery)):
            jemx_num = instrument.get_par_by_name('jemx_num').value
            target, modules, assume = self.set_instr_dictionaries(extramodules, scwlist_assumption, E1, E2, osa_version,
                                                                  jemx_num=jemx_num)
        else:
            target, modules, assume = self.set_instr_dictionaries(extramodules, scwlist_assumption, E1, E2, osa_version)


        q=OsaDispatcher(config=config, target=target, modules=modules, assume=assume, inject=inject,instrument=instrument)

        return q



    def process_product_method  (self, instrument, prod_list,api=False, **kw):

        query_image = prod_list.get_prod_by_name('mosaic_image')
        query_catalog = prod_list.get_prod_by_name('mosaic_catalog')
        detection_significance = instrument.get_par_by_name('detection_threshold').value

        if detection_significance is not None:
            query_catalog.catalog.selected = query_catalog.catalog._table['significance'] > float(
                detection_significance)

        query_image.add_url_to_fits_file(instrument._current_par_dic,url=instrument.disp_conf.products_url)
        query_image.write(overwrite=True)
        query_catalog.write(overwrite=True,format='fits')
        query_catalog.write(overwrite=True, format='ds9')

        if api==False:
            #TODO  MAKE THIS BETTER
            try:
                html_fig = query_image.get_html_draw(catalog=query_catalog.catalog,data_ID=4)
            except:
                html_fig = query_image.get_html_draw(catalog=query_catalog.catalog, data_ID=4)

        #print('--> query was ok 2')
        query_out = QueryOutput()

        #print ('CICCIO api',api)

        if api==False:
            query_out.prod_dictionary['image'] = html_fig
            query_out.prod_dictionary['file_name'] = [str(query_image.file_path.name),
                                                      str(query_catalog.file_path.name + '.fits'),
                                                      str(query_catalog.file_path.name + '.reg')]
            query_out.prod_dictionary['download_file_name'] = 'image.tar.gz'
            query_out.prod_dictionary['catalog'] = query_catalog.catalog.get_dictionary()
        else:
            query_out.prod_dictionary['numpy_data_product_list'] = [query_image.data]
            query_out.prod_dictionary['catalog'] = query_catalog.catalog.get_dictionary()
            #TODO add the encode method to catalog
            #query_out.prod_dictionary['catalog'] = query_catalog.catalog.get_dictionary()

        query_out.prod_dictionary['prod_process_message'] = ''

        return query_out


    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2,osa_version="OSA10.2"):
        raise RuntimeError('Must be specified for each instrument')


class IsgriMosaicQuery(OsaMosaicQuery):

    def __init__(self, name):
        super(IsgriMosaicQuery, self).__init__(name)

    def build_product_list(self, instrument, res, out_dir, prod_prefix=None, api=False):
        meta_data = {'product': 'mosaic', 'instrument': 'isgri', 'src_name': ''}
        meta_data['query_parameters'] = self.get_parameters_list_as_json()

        image = OsaImageProduct.build_from_ddosa_skyima(file_name='isgri_query_mosaic.fits',
                                                        skyima=res.skyima,
                                                        ext=4,
                                                        file_dir=out_dir,
                                                        prod_prefix=prod_prefix,
                                                        meta_data=meta_data)

        osa_catalog = CatalogProduct('mosaic_catalog',
                                     catalog=OsaIsgriCatalog.build_from_ddosa_srclres(res.srclres),
                                     file_name='query_catalog',
                                     name_prefix=prod_prefix,
                                     file_dir=out_dir)


        prod_list = [image, osa_catalog]

        return prod_list


    def get_dummy_products(self, instrument, config, out_dir='./', api=False):



        meta_data = {'product': 'mosaic', 'instrument': 'isgri', 'src_name': ''}
        meta_data['query_parameters'] = self.get_parameters_list_as_json()
        dummy_cache = config.dummy_cache

        skyima='%s/isgri_query_mosaic.fits' % dummy_cache
        res = DummyOsaRes()
        res.__setattr__('skyima',skyima )

        user_catalog = instrument.get_par_by_name('user_catalog').value

        image = OsaImageProduct.build_from_ddosa_skyima(file_name='isgri_query_mosaic.fits',
                                                        skyima=skyima,
                                                        ext=0,
                                                        file_dir=out_dir,
                                                        meta_data=meta_data)



        catalog = CatalogProduct(name='mosaic_catalog',
                                 catalog=BasicCatalog.from_fits_file('%s/query_catalog.fits' % dummy_cache),
                                 file_name='query_catalog',
                                 file_dir=out_dir)

        if user_catalog is not None:
            #print('setting from user catalog', user_catalog, catalog)
            catalog.catalog = user_catalog

        prod_list = QueryProductList(prod_list=[image, catalog])
        return prod_list


 









    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2,osa_version="OSA10.2"):
        #print ('E1,E2',E1,E2)
        target = "mosaic_ii_skyimage"

        #TODO: this should be re-used. where? common pars?
        if osa_version is None:
            raise MissingParameter("osa_version is needed")

        versions = osa_version.split("-", 1)
        if len(versions) == 1:
            osa_version_base, osa_subversion = versions[0], 'default-isdc'
        elif len(versions) == 2:
            osa_version_base, osa_subversion = versions
        else:
            raise RuntimeError(f"this should not happen")

        if osa_version_base == "OSA10.2":
            modules = ["git://ddosa/staging-1-3"] + extramodules + ['git://ddosa_delegate/staging-1-3']
        elif osa_version_base == "OSA11.0":
            modules = ["git://ddosa/staging-1-3","git://findic/staging-1-3-icversion","git://ddosa11/staging-1-3"] + extramodules+['git://ddosa_delegate/staging-1-3']
        else:
            raise RuntimeError(f"unknown OSA version {osa_version_base}, complete version {osa_version}")

        assume = ['ddosa.ImageGroups(input_scwlist=%s)' % scwlist_assumption[0],
                   scwlist_assumption[1],
                  'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")'%dict(E1=E1,E2=E2),
                  'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")',
                   ]

        if osa_subversion != "default-isdc":
            assume.append(f'ddosa.ICRoot(use_ic_root_version="{osa_subversion}")')

        return target, modules, assume
        








class JemxMosaicQuery(OsaMosaicQuery):

    def __init__(self,name ):
        super(JemxMosaicQuery, self).__init__(name)



    def get_dummy_products(self, instrument, config, out_dir='./', api=False):



        meta_data = {'product': 'mosaic', 'instrument': 'jemx', 'src_name': ''}
        meta_data['query_parameters'] = self.get_parameters_list_as_json()
        dummy_cache = config.dummy_cache

        skyima='%s/jemx_query_mosaic.fits' % dummy_cache
        res = DummyOsaRes()
        res.__setattr__('skyima',skyima )

        user_catalog = instrument.get_par_by_name('user_catalog').value

        image = OsaImageProduct.build_from_ddosa_skyima(file_name='jemx_query_mosaic.fits',
                                                        skyima=skyima,
                                                        ext=0,
                                                        file_dir=out_dir,
                                                        meta_data=meta_data)



        catalog = CatalogProduct(name='mosaic_catalog',
                                 catalog=BasicCatalog.from_fits_file('%s/jemx_query_catalog.fits' % dummy_cache),
                                 file_name='query_catalog',
                                 file_dir=out_dir)

        if user_catalog is not None:
            #print('setting from user catalog', user_catalog, catalog)
            catalog.catalog = user_catalog

        prod_list = QueryProductList(prod_list=[image, catalog])
        return prod_list

    def build_product_list(self, instrument, res, out_dir, prod_prefix=None, api=False):
        meta_data = {'product': 'mosaic', 'instrument': 'jemx', 'src_name': ''}
        meta_data['query_parameters'] = self.get_parameters_list_as_json()

        if hasattr(res, 'skyima'):
            image = OsaImageProduct.build_from_ddosa_skyima(file_name='jemx_query_mosaic.fits',
                                                            skyima=res.skyima,
                                                            ext=4,
                                                            file_dir=out_dir,
                                                            prod_prefix=prod_prefix,
                                                            meta_data=meta_data)

            osa_catalog = CatalogProduct('mosaic_catalog',
                                         catalog=OsaJemxCatalog.build_from_ddosa_srclres(res.srclres),
                                         file_name='query_catalog',
                                         name_prefix=prod_prefix,
                                         file_dir=out_dir)

        else:
            image = OsaImageProduct.build_empty(file_name='jemx_query_mosaic.fits',
                                                file_dir=out_dir,
                                                prod_prefix=prod_prefix,
                                                meta_data=meta_data)

            osa_catalog = CatalogProduct('mosaic_catalog',
                                         catalog=OsaJemxCatalog.build_from_dict_list({}),
                                         file_name='query_catalog',
                                         name_prefix=prod_prefix,
                                         file_dir=out_dir)



        prod_list = [image, osa_catalog]

        return prod_list


    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2,osa_version="OSA10.2",jemx_num=2):

        target = "mosaic_jemx_osa"

        if osa_version == "OSA10.2":
            modules = ["git://ddosa/staging-1-3", "git://ddjemx"]  + extramodules + ['git://ddosa_delegate/staging-1-3']
        elif osa_version == "OSA11.0":
            modules = ["git://ddosa/staging-1-3", "git://ddjemx", "git://findic/icversionpy37", "git://ddosa11/icversion"] + extramodules + [
                'git://ddosa_delegate/staging-1-3']
        else:
            raise Exception("unknown OSA version:", osa_version)

        assume = ['ddjemx.JMXImageGroups(input_scwlist=%s)' % scwlist_assumption[0],
                   scwlist_assumption[1],
                  'ddjemx.JEnergyBins(use_bins=[(%(E1)s,%(E2)s)])' % dict(E1=E1, E2=E2),
                  'ddjemx.JEMX(use_num=%s)'%jemx_num]

        return target, modules, assume







