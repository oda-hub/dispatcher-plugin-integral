
from __future__ import absolute_import, division, print_function

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)
import re
__author__ = 'andrea tramacere'




#!/usr/bin/env python

from setuptools import setup, find_packages
import glob

install_req = [
    'cdci_data_analysis',
    'astropy',
    'simple_logger',
    'numpy',
    'sentry-sdk'
]

test_req = [
    'pytest',
    'pytest-depends',
]

packs=find_packages()

print ('packs', packs)

include_package_data = True

scripts_list=glob.glob('./bin/*')
setup(name='cdci_osa_plugin',
      version=1.0,
      description='A OSA plugin for CDCI online data analysis',
      author='Andrea Tramacere',
      author_email='andrea.tramacere@unige.ch',
      scripts=scripts_list,
      packages=packs,
      package_data={'cdci_osa_plugin':['config_dir/*']},
      include_package_data=True,
      install_requires=install_req,
      extras_require={
          'test': test_req
      }
      )

