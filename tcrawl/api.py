
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

import re, urllib, urllib2, httplib, time
import pycurl

_CHROME_HEADERS = {
        'Accept': ('text/html,application/xhtml+xml,'
            'application/xml;q=0.9,*/*;q=0.8'),
        'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
        'Accept-Encoding': 'deflate,sdch',
        'Accept-Language': 'en-US,en;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'User-Agent': ('Mozilla/5.0 (Windows NT 6.1; WOW64) '
            'AppleWebKit/534.24 (KHTML, like Gecko) '
            'Chrome/11.0.696.14 Safari/534.24'),
        }


_URL_OPERNER = urllib2.build_opener()
_URL_OPERNER.addheaders = [header for header in _CHROME_HEADERS.iteritems()]
#pylint: disable-msg=E1103

class APIError(Exception):
    """Error Caused by twitter.com"""
    def __init__(self, code, msg, resp):
        super(APIError, self).__init__()
        self.code, self.msg, self.resp = code, msg, resp

    def __str__(self):
        return str(self.code) + '\r\n' + self.msg + '\r\n' + self.resp.read()

SINCEID_PATTERN = re.compile('()&since_id=\\d+|(\?)since_id=\\d+&')
PAGE = re.compile(r'page')
URLPATHEX = re.compile(r'http://.*?/')
URLTRIM = re.compile('\\?\\s*$')
URLPTN = re.compile('https?://(.*?)/(.*)')

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
    """split URL into host and path
    """
    if url.find('https') == 0:
        secure = True
    elif url.find('http') == 0:
        secure = False

    mth = URLPTN.match(url)
    return mth.group(1), mth.group(2), secure

def api_call(host, path, secure):
    """Retrieve data from server"""
    retry_left = 3
    while retry_left > 0:
        if secure:
            conn = httplib.HTTPSConnection(host, timeout=10)
        else:
            conn = httplib.HTTPConnection(host, timeout=10)
            print host, path

        _CHROME_HEADERS['Host'] = host
        try:
            conn.request('GET', path, '', _CHROME_HEADERS)
            resp = conn.getresponse()
        except Exception:
            raise APIError(0, 'Cannot connect to server', None)
        if resp.status == 200:
            return resp
        if resp.status == 301:
            host, path, secure = urlsplit(resp.getheader('Location'))
            continue
        if resp.status == 502:
            time.sleep(1)
            continue
        raise APIError(resp.status, 'Unexpected Error', resp)

def api_call2(host, path, secure):
    """Retrieve data from server"""
    retry_left = 10
    while retry_left > 0:
        if secure:
            url = 'https://'
        else:
            url = 'http://'

        url += host + '/' + path
        try:
            resp = _URL_OPERNER.open(url, "", 100)
            return resp
        except Exception:
            raise APIError(0, 'INVALID: %s' % url, None)
        return None

def stream_call(url, writer):
    """cal streaming api
    """
    #FIXME not completed
    stream = pycurl.Curl()
    stream.setopt(pycurl.URL, url)
    stream.setopt(pycurl.WRITEFUNCTION, writer.write)
    stream.perform()

def test():
    """Test this module
    """
    print urlsplit('http://en.wikipedia.com/Index/Main')
    print urlsplit('https://en.wikipedia.com/index/main/')
    print api_call2('en.wikipedia.com', '', False).read()


if __name__ == '__main__':
    test()
