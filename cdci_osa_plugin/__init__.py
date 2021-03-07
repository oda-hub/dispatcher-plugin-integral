

from __future__ import absolute_import, division, print_function


import pkgutil
import logging
import os

__author__ = "Andrea Tramacere"


pkg_dir = os.path.abspath(os.path.dirname(__file__))
pkg_name = os.path.basename(pkg_dir)
__all__ = []
for importer, modname, ispkg in pkgutil.walk_packages(path=[pkg_dir],
                                                      prefix=pkg_name+'.',
                                                      onerror=lambda x: None):

    if ispkg == True:
        __all__.append(modname)
    else:
        pass


conf_dir = os.path.dirname(__file__)+'/config_dir'
conf_file = os.path.join(conf_dir, 'data_server_conf.yml')
env_conf_file = os.environ.get('CDCI_OSA_PLUGIN_CONF_FILE')

if env_conf_file is not None:
    conf_file = env_conf_file


logging.info("loading %s config from %s", __name__, conf_file)
