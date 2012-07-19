#!python
# -*- coding: utf-8 -*-
"""File: google_api.py
Description:
    APIs from Google service
History:
    0.1.0 The first version.
"""
__version__ = '0.1.0'
__author__ = 'SpaceLis'

from tcrawl.api.api import APIError, api_call, buildpath
import time, random, traceback, sys
import json

random.seed(time.time())

def websearch(**kargs):
    """Google web search API
        http://code.google.com/apis/websearch/
    """
    host = 'ajax.googleapis.com'
    api_path = '/ajax/services/search/web'
    sresults = list()
    if 'rsz' not in kargs:
        kargs['rsz'] = 8
    if 'start' not in kargs:
        kargs['start'] = 0
    while True:
        time.sleep(random.expovariate(0.05))
        path = buildpath(api_path, kargs)
        try:
            resp = api_call(host, path, True)
            jresp = json.loads(resp.read())
            print resp[:50]
            kargs['start'] += kargs['rsz']
            sresults.extend(jresp['responseData']['results'])
            if len(jresp['responseData']['cursor'])==0:
                break
            elif jresp['responseData']['cursor']['pages'][-1]['start'] \
                    < kargs['start']:
                break
            if kargs['start'] > 50:
                break
        except APIError:
            print 'Deception Failed!'
            time.sleep(3600)
        except StandardError:
            traceback.print_exc(file=sys.stdout)
            time.sleep(3600)
    return sresults

