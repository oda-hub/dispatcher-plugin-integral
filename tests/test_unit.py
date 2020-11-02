
def test_osa_versions():
    from cdci_osa_plugin.osa_image_query import IsgriMosaicQuery
    from cdci_osa_plugin.osa_spectrum_query import IsgriSpectrumQuery

    assert IsgriMosaicQuery("name").set_instr_dictionaries(
                    extramodules=[],
                    scwlist_assumption=["sa", "sb"],
                    E1=25.,
                    E2=80.,
                    osa_version="OSA10.2",
                ) == (
                        'mosaic_ii_skyimage', 
                        ['git://ddosa/staging-1-3', 
                         'git://ddosa_delegate/staging-1-3'], 
                        ['ddosa.ImageGroups(input_scwlist=sa)', 
                         'sb',
                         'ddosa.ImageBins(use_ebins=[(25.0,80.0)],use_version="onebin_25.0_80.0")', 
                         'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")']
                    )

    assert IsgriMosaicQuery("name").set_instr_dictionaries(
                    extramodules=[],
                    scwlist_assumption=["sa", "sb"],
                    E1=25.,
                    E2=80.,
                    osa_version="OSA11.0",
                ) == (
                        'mosaic_ii_skyimage', 
                        ['git://ddosa/staging-1-3', 
                         'git://findic/staging-1-3-icversion',
                         'git://ddosa11/staging-1-3',
                         'git://ddosa_delegate/staging-1-3'], 
                        ['ddosa.ImageGroups(input_scwlist=sa)', 
                         'sb',
                         'ddosa.ImageBins(use_ebins=[(25.0,80.0)],use_version="onebin_25.0_80.0")', 
                         'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")']
                    )

    print(IsgriSpectrumQuery("name").set_instr_dictionaries(
                    extramodules=[],
                    scwlist_assumption=["sa", "sb"],
                    E1=25.,
                    E2=80.,
                    osa_version="OSA11.0-dev",
                ))
    assert IsgriSpectrumQuery("name").set_instr_dictionaries(
                    extramodules=[],
                    scwlist_assumption=["sa", "sb"],
                    E1=25.,
                    E2=80.,
                    osa_version="OSA11.0-dev",
                ) ==  (
                        'ISGRISpectraSum', 
                        [
                            'git://ddosa/staging-1-3', 
                            'git://findic/staging-1-3-icversion', 
                            'git://ddosa11/staging-1-3', 
                            'git://useresponse/osa11', 
                            'git://process_isgri_spectra/osa11',
                            'git://rangequery/staging-1-3', 
                            'git://ddosa_delegate/staging-1-3'
                        ], 
                        [
                            'process_isgri_spectra.ScWSpectraList(input_scwlist=sa)', 
                            'sb', 
                            'ddosa.ImageBins(use_ebins=[(25.0,80.0)],use_version="onebin_25.0_80.0")', 
                            'process_isgri_spectra.ISGRISpectraSum(use_extract_all=True)', 
                            'ddosa.ImagingConfig(use_SouFit=0,use_DoPart2=1,use_version="soufit0_p2")', 
                            'ddosa.CatForSpectraFromImaging(use_minsig=3)'
                        ]
                     )

    assert IsgriMosaicQuery("name").set_instr_dictionaries(
                    extramodules=[],
                    scwlist_assumption=["sa", "sb"],
                    E1=25.,
                    E2=80.,
                    osa_version="OSA11.0-dev",
                ) == (
                        'mosaic_ii_skyimage', 
                        ['git://ddosa/staging-1-3', 
                         'git://findic/staging-1-3-icversion',
                         'git://ddosa11/staging-1-3',
                         'git://ddosa_delegate/staging-1-3'], 
                        ['ddosa.ImageGroups(input_scwlist=sa)', 
                         'sb',
                         'ddosa.ImageBins(use_ebins=[(25.0,80.0)],use_version="onebin_25.0_80.0")', 
                         'ddosa.ImagingConfig(use_SouFit=0,use_version="soufit0")',
                         'ddosa.ICRoot(use_ic_root_version="dev")'
                         ]
                    )
