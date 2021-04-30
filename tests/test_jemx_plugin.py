import pytest
import logging
import requests
import json

logger = logging.getLogger(__name__)

default_params = dict(
    query_status="new",
    query_type="Real",
    instrument="jemx",
    product_type="antares_spectrum",
    RA=280.229167,
    DEC=-5.55,
    radius=2.,
    index_min=1.5,
    index_max=3.,
    async_dispatcher=False
)

dummy_params = dict(
    query_status="new",
    query_type="Dummy",
    instrument="isgri",
    product_type="isgri_image",
    async_dispatcher=False
)
