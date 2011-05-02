
#!python
# -*- coding: utf-8 -*-
"""File: api.py
Description:
    The basic connection to online APIs
History:
    0.1.0 The first version.
"""
__version__ = '0.1.0'
__author__ = 'SpaceLis'

import re
import urllib
import httplib
import urlparse
import time
import pycurl

CHROME_HEADERS = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'gzip,deflate,sdch',
        'Accept-Language': 'en-US,en;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.14 Safari/534.24',
        }


#pylint: disable-msg=E1103

class APIError(Exception):
    """Error Caused by twitter.com"""
    def __init__(self, code, msg, resp):
        super(APIError, self).__init__()
        self.code, self.msg, self.resp = code, msg, resp

SINCEID_PATTERN = re.compile('()&since_id=\\d+|(\?)since_id=\\d+&')
PAGE = re.compile(r'page')
URLPATHEX = re.compile(r'http://.*?/')
URLTRIM = re.compile('\\?\\s*$')

def buildpath(api_path, kargs):
    """Build the request arguments"""
    inline_para = dict()
    keylist = list()
    for key in kargs.iterkeys():
        if api_path.find('%({0})'.format(key)) > 0:
            inline_para[key] = kargs[key]
            keylist.append(key)
    for key in keylist:
        del kargs[key]

    path = (api_path % inline_para) + \
            ('?' + urllib.urlencode(kargs) if len(kargs)>0 else '')
    return path


def urlsplit(url):
    """Split URL in to host and path
    """
    urlpart = urlparse.urlsplit(url)
    host = urlpart.netloc
    path = urlpart.path + '?' + urlpart.query
    path = URLTRIM.sub('', path)
    return host, path


def api_call(host, path, secure):
    """Retrieve data from server"""
    retry_left = 3
    while retry_left > 0:
        if secure:
            conn = httplib.HTTPSConnection(host, timeout=10)
        else:
            conn = httplib.HTTPConnection(host, timeout=10)
            print host, path

        CHROME_HEADERS['Host'] = host
        try:
            conn.request('GET', path, '', CHROME_HEADERS)
            resp = conn.getresponse()
        except Exception:
            raise APIError(0, 'Cannot connect to server', None)
        if resp.status == 200:
            return resp
        if resp.status == 301:
            host, path = urlsplit(resp.getheader('Location'))
            continue
        if resp.status == 502:
            time.sleep(1)
            continue
        raise APIError(resp.status, 'Unexpected Error', resp)

def api_call2(host, path, secure):
    """Retrieve data from server"""
    retry_left = 3
    while retry_left > 0:
        if secure:
            url = 'https://'
        else:
            url = 'http://'

        url += host + '/' + path
        print url
        try:
            resp = urllib.urlopen(url)
        except Exception:
            raise APIError(0, 'URL is invalid', None)
        return resp

def stream_call(url, writer):
    """cal streaming api
    """
    stream = pycurl.Curl()
    stream.setopt(pycurl.URL, url)
    stream.setopt(pycurl.WRITEFUNCTION, writer.write)
    stream.perform()

