#!python
# -*- coding: utf-8 -*-
"""File: foursq_api.py
Description:
    ForSquare APIs
History:
    0.1.0 The first version.
"""
__version__ = '0.1.0'
__author__ = 'SpaceLis'

import json
import logging
import time
from tcrawl.api import APIError, api_call, buildpath

def search(**kargs):
    """Foursquare API"""
    host = 'api.foursquare.com'
    api_path = '/v2/venues/search'
    kargs['oauth_token'] = '31JU4VJBLV4SOFWMP2W13XIZEZDYIK5E3LH3PJ3TXTQY1HMF'
    path = buildpath(api_path, kargs)
    while True:
        try:
            resp = api_call(host, path, True)
            ret = json.loads(resp.read())
        except APIError as twe:
            if twe.code == 403:
                sleeptime = 600
                logging.warning( \
                    'Rate limits exceeded, retry after {0} sec'.format( \
                    sleeptime))
                time.sleep(sleeptime)
                continue
        break
    return ret

def test():
    """A test
    """
    print search(ll='44.3,37.2')

if __name__ == '__main__':
    test()

