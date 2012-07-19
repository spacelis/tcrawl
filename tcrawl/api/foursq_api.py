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
from tcrawl.api.api import APIError, api_call, buildpath
from datetime import datetime

def search(**kargs):
    """Foursquare API"""
    host = 'api.foursquare.com'
    api_path = '/v2/venues/search'
    kargs['oauth_token'] = 'DTQX41P5JP1IA5QGNZC0UQ1FTBVNP31I1HE54Y0LDRFTAXGS'
    kargs['v'] = datetime.today().strftime('%Y%m%d')
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
    print json.dumps(search(ll='40.76305,-73.97375', query='Louis Vuitton', intent='match'))


if __name__ == '__main__':
    test()

