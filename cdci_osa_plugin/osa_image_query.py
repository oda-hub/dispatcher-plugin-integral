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


from .osa_catalog import  OsaIsgriCatalog,OsaJemxCatalog
from .osa_dataserve_dispatcher import    OsaDispatcher

class OsaImageProduct(ImageProduct):

    def __init__(self,name,file_name,skyima,out_dir=None,prod_prefix=None):
        header = skyima.header
        data = skyima.data
        super(OsaImageProduct, self).__init__(name,data=data,header=header,name_prefix=prod_prefix,file_dir=out_dir,file_name=file_name)
        #check if you need to copy!





    @classmethod
    def build_from_ddosa_skyima(cls,name,file_name,skyima,out_dir=None,prod_prefix=None):
        skyima=FitsFile(skyima).open()
        return  cls(name,skyima=skyima[4],out_dir=out_dir,prod_prefix=prod_prefix,file_name=file_name)





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



    def process_product(self, instrument, prod_list):

        query_image = prod_list.get_prod_by_name('mosaic_image')
        query_catalog = prod_list.get_prod_by_name('mosaic_catalog')
        detection_significance = instrument.get_par_by_name('detection_threshold').value

        if detection_significance is not None:
            query_catalog.catalog.selected = query_catalog.catalog._table['significance'] > float(
                detection_significance)

        #print('--> query was ok')
        # file_path = Path(scratch_dir, 'query_mosaic.fits')
        query_image.write(overwrite=True)
        #print('--> query was ok 1')
        # file_path = Path(scratch_dir, 'query_catalog.fits')
        query_catalog.write(overwrite=True,format='fits')
        #print('--> query was ok 2')
        query_catalog.write(overwrite=True, format='ds9')
        #print('--> query was ok 3')
        html_fig = query_image.get_html_draw(catalog=query_catalog.catalog,
                                             vmin=instrument.get_par_by_name('image_scale_min').value,
                                             vmax=instrument.get_par_by_name('image_scale_max').value)
        print('--> query was ok 2')
        query_out = QueryOutput()

        query_out.prod_dictionary['image'] = html_fig
        query_out.prod_dictionary['data'] = query_image.data.tolist()
        query_out.prod_dictionary['catalog'] = query_catalog.catalog.get_dictionary()
        query_out.prod_dictionary['file_name'] = [str(query_image.file_path.name), str(query_catalog.file_path.name+'.fits'),str(query_catalog.file_path.name+'.reg')]
        query_out.prod_dictionary['download_file_name'] = 'image.tgz'
        query_out.prod_dictionary['prod_process_message'] = ''

        return query_out


    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2,osa_version="OSA10.2"):
        raise RuntimeError('Must be specified for each instrument')













class JemxMosaicQuery(OsaMosaicQuery):
    def __init__(self,name ):
        super(JemxMosaicQuery, self).__init__(name)


    def get_dummy_products(self, instrument, config=None, **kwargs):
        pass


    def set_instr_dictionaries(self,extramodules,scwlist_assumption,E1,E2):
        target = "mosaic_jemx"
        modules = ["git://ddjemx","git://ddosa_delegate_ddjemx"] + extramodules

        assume = ['ddjemx.JMXScWImageList(input_scwlist=%s)' % scwlist_assumption[0],
                   scwlist_assumption[1],
                  'ddjemx.JEnergyBins(use_bins=[(%(E1)s,%(E2)s)])' % dict(E1=E1, E2=E2),
                  'ddjemx.JEMX(use_num=2)']

        return target, modules, assume

    def build_product_list(self, instrument,res, out_dir, prod_prefix=None):

        image = OsaImageProduct.build_from_ddosa_skyima('mosaic_image', 'jemx_query_mosaic.fits', res.skyima,
                                                        out_dir=out_dir, prod_prefix=prod_prefix)
        osa_catalog = CatalogProduct('mosaic_catalog', catalog=OsaJemxCatalog.build_from_ddosa_srclres(res.srclres),
                                     file_name='query_catalog', name_prefix=prod_prefix, file_dir=out_dir)

        prod_list = [image, osa_catalog]

        return prod_list


    def get_dummy_products(self, instrument, config, out_dir='./'):

        dummy_cache = config.dummy_cache

        failed = False
        image = None
        catalog = None

        user_catalog = instrument.get_par_by_name('user_catalog').value

        image = ImageProduct.from_fits_file(in_file='%s/jemx_query_mosaic.fits' % dummy_cache,
                                            out_file_name='jemx_query_mosaic.fits',
                                            prod_name='mosaic_image',
                                            ext=0,
                                            file_dir=out_dir)

        catalog = CatalogProduct(name='mosaic_catalog',
                                 catalog=BasicCatalog.from_fits_file('%s/query_catalog.fits' % dummy_cache),
                                 file_name='query_catalog.fits',
                                 file_dir=out_dir)

        if user_catalog is not None:
            print('setting from user catalog', user_catalog, catalog)
            catalog.catalog = user_catalog

        prod_list = QueryProductList(prod_list=[image, catalog])
        return prod_list


class IsgriMosaicQuery(OsaMosaicQuery):
    def __init__(self,name ):
        super(IsgriMosaicQuery, self).__init__(name)





    def build_product_list(self,instrument,res,out_dir,prod_prefix=None):
        #print('ciccio', prod_prefix)

        image = OsaImageProduct.build_from_ddosa_skyima('mosaic_image', 'isgri_query_mosaic.fits', res.skyima,
                                                            out_dir=out_dir, prod_prefix=prod_prefix)
        osa_catalog = CatalogProduct('mosaic_catalog',
                                         catalog=OsaIsgriCatalog.build_from_ddosa_srclres(res.srclres),
                                         file_name='query_catalog', name_prefix=prod_prefix, file_dir=out_dir)

        prod_list =  [image, osa_catalog]


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
            
        

    
        return target,modules,assume

    def get_dummy_products(self, instrument, config, out_dir='./'):

        dummy_cache = config.dummy_cache

        failed = False
        image = None
        catalog = None

        user_catalog = instrument.get_par_by_name('user_catalog').value

        image = ImageProduct.from_fits_file(in_file='%s/isgri_query_mosaic.fits' % dummy_cache,
                                            out_file_name='isgri_query_mosaic.fits',
                                            prod_name='mosaic_image',
                                            ext=0,
                                            file_dir=out_dir)

        catalog = CatalogProduct(name='mosaic_catalog',
                                 catalog=BasicCatalog.from_fits_file('%s/query_catalog.fits' % dummy_cache),
                                 file_name='query_catalog',
                                 file_dir=out_dir)

        if user_catalog is not None:
            print('setting from user catalog', user_catalog, catalog)
            catalog.catalog = user_catalog

        prod_list = QueryProductList(prod_list=[image, catalog])
        return prod_list

