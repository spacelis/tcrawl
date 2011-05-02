#!python
# -*- coding: utf-8 -*-
"""File: bing_api.py
Description:
    A set of function calling Microsoft Bing's API
History:
    0.1.0 The first version.
"""
__version__ = '0.1.0'
__author__ = 'SpaceLis'

import sys, traceback, json, time
from tcrawl.api import api_call2, buildpath, APIError
def searchrequest(**kargs):
    """Bing's search API
    """
    host = 'api.bing.net'
    #host = 'api.search.live.net'
    api_path = 'json.aspx'
    kargs['Web.Count'] = 50
    kargs['Web.Offset'] = 0
    path = buildpath(api_path, kargs)
    sresults = list()
    while True:
        try:
            time.sleep(1)
            resp = api_call2(host, path, False)
            jresp = json.loads(resp.read())
            sresults.extend(jresp['SearchResponse']['Web']['Results'])
            break
        except APIError as err:
            traceback.print_exc(file=sys.stdout)
            print err.code
            print err.resp.read()
    return sresults
