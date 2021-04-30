
cdci_osa_plugin
==========================================
*OSA INTEGRAL plugin for cdci_data_analysis*

What's the license?
-------------------

cdci_osa_plugin is distributed under the terms of The MIT License.

Who's responsible?
-------------------
Andrea Tramacere, Volodymyr Savchenko

ISDC Data Centre for Astrophysics, Astronomy Department of the University of Geneva, Chemin d'Ecogia 16, CH-1290 Versoix, Switzerland





test
------------------------------------
* set the evn variable with the dispatcher url, e.g.
  - `export DISP_URL='cdcicn01.isdc.unige.ch:32003/dispatch-data'       `

* if need set the token env var, e.g.
  - `export ODA_API_TOKEN=$HOME/.oda-api-token`

* `pytest ../cdci_osa_plugin/tests/test_osa.py::test_jemx_image -sv`
