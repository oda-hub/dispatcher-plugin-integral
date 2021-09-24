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
from cdci_data_analysis.analysis.parameters import *
from datetime import timedelta
import logging
import json
import redis
import socket
import odakb
import os

from builtins import (bytes, str, open, super, range,
                      zip, round, input, int, pow, object, map, zip)

__author__ = "Andrea Tramacere, Volodymyr Savchenko"

# Standard library
# eg copy
# absolute import rg:from copy import deepcopy

# Dependencies
# eg numpy
# absolute import eg: import numpy as np

# Project
# relative import eg: from .mod import f


class DummyOsaRes(object):

    def __init__(self):
        pass


logger = logging.getLogger(__name__)


def get_redis():
    return redis.Redis(host='localhost', port=6379, db=0)


def learn_osa_versions():
    r = [a['vs'] for a in odakb.sparql.select(
        'oda:osa_version oda:osa_option ?vs')]
    return r


def get_osa_versions():
    r = None

    try:
        r = get_redis().get('osa-versions')
        r_j = json.loads(r.decode())
    except Exception as e:
        logger.warning('issue accessing redis: %s', e)

    if r is None:
        r_j = learn_osa_versions()

        try:
            get_redis().set('osa-versions', json.dumps(r).encode(), ex=600)
        except Exception as e:
            logger.warning('issue accessing redis: %s', e)

    return r_j


class OSAVersion(Name):
    @property
    def value(self):
        return self._value

    @value.setter
    def value(self,v):
        if v is not None:
            osa_version_base, osa_subversion, version_modifiers = split_osa_version(v)
            # if self.check_value is not None:
            #     self.check_value(v, units=self.units,name=self.name)
            # if self._allowed_values is not None:
            #     if v not in self._allowed_values:
            #         raise RuntimeError('value',v,'not allowed, allowed=',self._allowed_values)

            if osa_version_base not in ["OSA10.2", "OSA11.0", "OSA11.1"]:
                raise RuntimeError('value', v, 'not allowed, allowed=', self._allowed_values)

            if isinstance(v, (str, six.string_types)):
                self._value = v.strip()
            else:
                self._value = v
        else:
            self._value=None

def osa_common_instr_query():
    # not exposed to frontend
    # TODO make a special class (VS:??)

    # it is *CRITICAL* that default parameters are public, 
    # or at least coincide with the defaults in check_query_roles else they are not validated
    # TODO: ensure this is guaranteed
    max_pointings = Integer(value=50, name='max_pointings')

    radius = Angle(value=5.0, units='deg', name='radius')

    osa_version = OSAVersion(name_format='str', name='osa_version')
    if  os.environ.get('DISPATCHER_MOCK_KB', 'no') == 'yes' or 'cdciweb01' in socket.gethostname():
        osa_version._allowed_values = [
            'OSA10.2', 'OSA11.0', 'OSA11.1']  # this really only for test
    else:
        osa_version._allowed_values = get_osa_versions()
        # can not really naturally select here by token roles

    data_rights = Name(name_format='str', name='integral_data_rights', value="public")
    data_rights._allowed_values = ["public", "all-private"]

    instr_query_pars = [
        radius,
        max_pointings,
        osa_version,
        data_rights
    ]

    return instr_query_pars


def split_osa_version(osa_version):
    version_and_modifiers = osa_version.split("--")
   
    osa_version = version_and_modifiers[0]
    version_modifiers = version_and_modifiers[1:]

    versions = osa_version.split("-", 1)
    if len(versions) == 1:
        osa_version_base, osa_subversion = versions[0], 'default-isdc'
    elif len(versions) == 2:
        osa_version_base, osa_subversion = versions
    else:
        raise RuntimeError(f"this should not be possible, OSA version split did not split as it should")

    return osa_version_base, osa_subversion, version_modifiers