

from __future__ import absolute_import, division, print_function


import pkgutil
import os

__author__ = "Andrea Tramacere"



pkg_dir = os.path.abspath(os.path.dirname(__file__))
pkg_name = os.path.basename(pkg_dir)
__all__=[]
for importer, modname, ispkg in pkgutil.walk_packages(path=[pkg_dir],
                                                      prefix=pkg_name+'.',
                                                      onerror=lambda x: None):

    if ispkg == True:
        __all__.append(modname)
    else:
        pass



#conf_dir=os.path.dirname(__file__)+'/config_dir'



# this line below means that config needs to be installed with the software, and may end up in the env dir.
# it's not really how configs work: only default config might be installed, and deployment needs to be able to customize the config
# arguably a variable is not the best solution as well. what seems to be common is default locations (/etc/cdci/... ~/.cdci/, ./.cdci-.. etc) and command-line parameters

conf_dir=os.path.dirname(__file__)+'/config_dir'

if conf_dir is not None:
    conf_dir=conf_dir

conf_file=os.environ.get('CDCI_OSA_PLUGIN_CONF_FILE',os.path.join(conf_dir,'data_server_conf.yml'))
