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
from typing import Optional
from cdci_data_analysis.analysis.parameters import String, Integer, Angle
from datetime import timedelta
import logging
import json
import redis
import odakb
import os

from cdci_data_analysis.analysis.exceptions import RequestNotUnderstood

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


class OSAVersion(String):
    def __init__(self,
                 value: Optional[str] = None,
                 name: Optional[str] = None,
                 allowed_base_osa_version_values: Optional[list] = None,
                 obsolete_base_osa_version_values: Optional[dict] = None):

        if obsolete_base_osa_version_values is None:
            self._obsolete_base_osa_version_values = {}
        else:
            self._obsolete_base_osa_version_values = obsolete_base_osa_version_values

        if allowed_base_osa_version_values is None:
            raise RuntimeError(f"can not initialize without allowed base OSA versions")
        else:
            self._allowed_base_osa_version_values = allowed_base_osa_version_values

        super().__init__(value=value, name=name)

        if os.environ.get('DISPATCHER_MOCK_KB', 'no') != 'yes':
            # this is in addition to base OSA versions
            self._allowed_values = get_osa_versions()
        else:
            self._allowed_values = ["OSA11.2-dev210827.0528-37487", "OSA11.2-devt20221103_osa11.2"]

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, v):
        if v is not None:
            osa_version_base, osa_subversion, version_modifiers = split_osa_version(v)

            if osa_version_base in self._obsolete_base_osa_version_values:
                if osa_subversion == 'default-isdc':
                    raise RequestNotUnderstood(f"Please note {osa_version_base} is being phased out. "
                                            f"We consider that for all or almost all likely user requests " 
                                            f"{self._obsolete_base_osa_version_values[osa_version_base]} shoud be used instead of {osa_version_base}.")                                    

            if osa_version_base not in (self._allowed_base_osa_version_values + list(self._obsolete_base_osa_version_values.keys())):
                # these should not be RuntimeError, but bad request errors. TODO to check if they propagate properly
                raise RequestNotUnderstood(f'value {v} is not allowed. '
                                   f'The OSA version should start with one of {self._allowed_base_osa_version_values}, '
                                    'and may contain additional components.')

            if osa_subversion != 'default-isdc':
                # suggestions should be only given to users with special roles. Let's just give none
                if f"{osa_version_base}-{osa_subversion}" not in self._allowed_values:
                    logger.warning("unknown dev OSA version %s, allowed %s", f"{osa_version_base}-{osa_subversion}", self._allowed_values)
                    raise RequestNotUnderstood("unknown dev OSA version!")

            if isinstance(v, (str, six.string_types)):
                self._value = v.strip()
            else:
                raise RequestNotUnderstood("OSA version should be a string")
        else:
            self._value = None


def osa_common_instr_query():
    # not exposed to frontend
    # TODO make a special class (VS:??)

    # it is *CRITICAL* that default parameters are public, 
    # or at least coincide with the defaults in check_query_roles else they are not validated
    # TODO: ensure this is guaranteed
    max_pointings = Integer(value=50, name='max_pointings')

    radius = Angle(value=5.0, units='deg', name='radius')

    osa_version = OSAVersion(name='osa_version', 
                             value='OSA11.2',
                             allowed_base_osa_version_values=["OSA10.2", "OSA11.2"],
                             obsolete_base_osa_version_values={"OSA11.0": "OSA11.2", "OSA11.1": "OSA11.2"})
    
    data_rights = String(name_format='str', name='integral_data_rights', value="public")
    data_rights._allowed_values = ["public", "all-private"]

    instr_query_pars = [
        radius,
        max_pointings,
        osa_version,
        data_rights
    ]

    return instr_query_pars


def get_known_osa_modifiers():
    return ['fullbkg', 'jemxnrt', 'rmfoffsetv1']


def split_osa_version(osa_version):
    version_and_modifiers = osa_version.split("--")

    logger.info("split_osa_version osa_version=%s", osa_version)
   
    osa_version = version_and_modifiers[0]
    version_modifiers = version_and_modifiers[1:]

    versions = osa_version.split("-", 1)
    if len(versions) == 1:
        osa_version_base, osa_subversion = versions[0], 'default-isdc'
    elif len(versions) == 2:
        osa_version_base, osa_subversion = versions
    else:
        raise RuntimeError()

    normalized_version_modifiers = list(sorted(set(version_modifiers)))
    if version_modifiers != normalized_version_modifiers:
        raise RuntimeError(f"non-normative OSA version modifier(s): '{'--'.join(version_modifiers)}', expected '{'--'.join(normalized_version_modifiers)}'. "
                            "Modifers should be sorted and non-duplicate.")

    known_osa_modifiers = get_known_osa_modifiers()
    unknown_version_modifiers = set(version_modifiers) - set(known_osa_modifiers)
    if len(unknown_version_modifiers) > 0:
        raise RuntimeError(f"provided unknown OSA version modifier(s): '{'--'.join(unknown_version_modifiers)}' in version '{osa_version}', known: '{'--'.join(known_osa_modifiers)}'")

    logger.info("split_osa_version to %s , %s , %s", osa_version, osa_version_base, osa_subversion)

    return osa_version_base, osa_subversion, version_modifiers
