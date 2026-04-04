"""Polygon API session."""

import os

from massive import RESTClient


def get_client() -> RESTClient:
    api_key = os.environ.get('POLYGON_API_KEY', '')
    if not api_key:
        raise RuntimeError(
            'POLYGON_API_KEY not set'
        )
    return RESTClient(api_key=api_key)
