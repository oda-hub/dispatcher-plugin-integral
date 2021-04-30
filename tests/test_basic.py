def test_discover_plugin():
    import cdci_data_analysis.plugins.importer as importer

    assert 'cdci_osa_plugin' in importer.cdci_plugins_dict.keys()
