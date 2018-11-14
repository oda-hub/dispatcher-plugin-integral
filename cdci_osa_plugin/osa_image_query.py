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

# Project
# relative import eg: from .mod import f
from astropy.io import  fits as pf

from cdci_data_analysis.analysis.queries import ImageQuery
from cdci_data_analysis.analysis.products import QueryProductList,CatalogProduct,ImageProduct,QueryOutput
from cdci_data_analysis.analysis.catalog import BasicCatalog
from cdci_data_analysis.analysis.io_helper import  FitsFile
from oda_api.data_products import NumpyDataProduct

from .osa_catalog import  OsaIsgriCatalog,OsaJemxCatalog
from .osa_dataserve_dispatcher import    OsaDispatcher
from .osa_common_pars import DummyOsaRes


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
        data=NumpyDataProduct.from_fits_file(skyima,ext=ext,meta_data=meta_data)
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
        target, modules, assume=self.set_instr_dictionaries(extramodules,scwlist_assumption,E1,E2,osa_version)

        q=OsaDispatcher(config=config, target=target, modules=modules, assume=assume, inject=inject,instrument=instrument)

        return q



    def process_product(self, instrument, prod_list,api=False):

        query_image = prod_list.get_prod_by_name('mosaic_image')
        query_catalog = prod_list.get_prod_by_name('mosaic_catalog')
        detection_significance = instrument.get_par_by_name('detection_threshold').value

        if detection_significance is not None:
            query_catalog.catalog.selected = query_catalog.catalog._table['significance'] > float(
                detection_significance)


        query_image.write(overwrite=True)
        query_catalog.write(overwrite=True,format='fits')
        query_catalog.write(overwrite=True, format='ds9')

        if api==False:
            html_fig = query_image.get_html_draw(catalog=query_catalog.catalog)

        #print('--> query was ok 2')
        query_out = QueryOutput()

        #print ('CICCIO api',api)

        if api==False:
            query_out.prod_dictionary['image'] = html_fig
            query_out.prod_dictionary['file_name'] = [str(query_image.file_path.name),
                                                      str(query_catalog.file_path.name + '.fits'),
                                                      str(query_catalog.file_path.name + '.reg')]
            query_out.prod_dictionary['download_file_name'] = 'image.fits.gz'
            query_out.prod_dictionary['catalog'] = query_catalog.catalog.get_dictionary()
        else:
            query_out.prod_dictionary['numpy_data_product_list'] = [query_image.data]
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
            print('setting from user catalog', user_catalog, catalog)
            catalog.catalog = user_catalog

        prod_list = QueryProductList(prod_list=[image, catalog])
        return prod_list


 









    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2,osa_version="OSA10.2"):
        print ('E1,E2',E1,E2)
        target = "mosaic_ii_skyimage"

        if osa_version=="OSA10.2":
            modules = ["git://ddosa", 'git://ddosa_delegate'] + extramodules
        elif osa_version=="OSA11.0":
            modules = ["git://ddosa","git://findic/icversion","git://ddosa11/icversion"] + extramodules+['git://ddosa_delegate']
        else:
            raise Exception("unknown OSA version:",osa_version)

        assume = ['ddosa.ImageGroups(input_scwlist=%s)' % scwlist_assumption[0],
                   scwlist_assumption[1],
                  'ddosa.ImageBins(use_ebins=[(%(E1)s,%(E2)s)],use_version="onebin_%(E1)s_%(E2)s")'%dict(E1=E1,E2=E2),
                  'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")',
                   ]

        return target, modules, assume
        








class JemxMosaicQuery(OsaMosaicQuery):
    def __init__(self,name ):
        super(JemxMosaicQuery, self).__init__(name)


#     def get_dummy_products(self, instrument, config=None, **kwargs):
#         pass
#
#
#     def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2):
#         target = "mosaic_jemx"
#         modules = ["git://ddjemx","git://ddosa_delegate_ddjemx"] + extramodules
#
#         assume = ['ddjemx.JMXScWImageList(input_scwlist=%s)' % scwlist_assumption[0],
#                    scwlist_assumption[1],
#                   'ddjemx.JEnergyBins(use_bins=[(%(E1)s,%(E2)s)])' % dict(E1=E1, E2=E2),
#                   'ddjemx.JEMX(use_num=2)']
#
#         return target, modules, assume
#
#     def build_product_list(self, instrument,res, out_dir, prod_prefix=None,api=False):
#
#         image = OsaImageProduct.build_from_ddosa_skyima('mosaic_image', 'jemx_query_mosaic.fits', res.skyima,
#                                                         out_dir=out_dir, prod_prefix=prod_prefix)
#         osa_catalog = CatalogProduct('mosaic_catalog', catalog=OsaJemxCatalog.build_from_ddosa_srclres(res.srclres),
#                                      file_name='query_catalog', name_prefix=prod_prefix, file_dir=out_dir)
#
#         prod_list = [image, osa_catalog]
#
#         return prod_list
#
#
#     def get_dummy_products(self, instrument, config, out_dir='./'):
#         meta_data = {'product': instrument.name, 'instrument': 'jemx', 'src_name': ''}
#         meta_data['query_parameters']=self.get_parameters_list_as_json()
#         dummy_cache = config.dummy_cache
#
#
#         user_catalog = instrument.get_par_by_name('user_catalog').value
#
#         image = OsaImageProduct.from_fits_file(in_file='%s/jemx_query_mosaic.fits' % dummy_cache,
#                                             out_file_name='jemx_query_mosaic.fits',
#                                             ext=0,
#                                             file_dir=out_dir,
#                                             meta_data=meta_data)
#
#         catalog = CatalogProduct(name='mosaic_catalog',
#                                  catalog=BasicCatalog.from_fits_file('%s/query_catalog.fits' % dummy_cache),
#                                  file_name='query_catalog.fits',
#                                  file_dir=out_dir)
#
#         if user_catalog is not None:
#             print('setting from user catalog', user_catalog, catalog)
#             catalog.catalog = user_catalog
#
#         prod_list = QueryProductList(prod_list=[image, catalog])
#         return prod_list
#
#
#
