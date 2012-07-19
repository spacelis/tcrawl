#!python
# -*- coding: utf-8 -*-
"""File: streamer.py
Description:
    Utilizing the stream APIs provided by Twitter or some other data provider
    to crawl data.
History:
    0.1.1 fix bug imcomplete read; move account information out of code
    0.1.0 The first version.
"""
__version__ = '0.1.0'
__author__ = 'SpaceLis'

import signal
import httplib
import base64
import logging
import tcrawl.writer

STREAMAPIS = {'filter': {'host': 'stream.twitter.com',
                        'path': '/1/statuses/filter.json',
                        'method': 'POST',
                        'safe': True,
                        'auth': True},
            'sample': {'host': 'stream.twitter.com',
                        'path': '/1/statuses/sample.json',
                        'method': 'GET',
                        'safe': True,
                        'auth': True}}

class Streamer(object):
    """ Streaming Twitter's API
    """
    def __init__(self, api_name, wr, **kargs):
        super(Streamer, self).__init__()
        self.api_name = api_name
        self.writer = wr
        self.api_kargs = kargs
        self.running = True

    def user_account(self, user, pwd):
        """ Set the user account to be used for streaming
        """
        self.user = user
        self.pwd = pwd

    def _get_req_paras(self):
        """ General configuration for stream APIs
        """
        method = STREAMAPIS[self.api_name]['method']
        path = STREAMAPIS[self.api_name]['path']
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = '&'.join(['%s=%s' % (key, val) for key, val in self.api_kargs.iteritems()])
        if STREAMAPIS[self.api_name]['auth']:
            token = base64.encodestring('%s:%s' % (self.user, self.pwd))[:-1]
            headers['Authorization'] = 'Basic ' + token
        logging.debug('[Request] Method: ' + method)
        logging.debug('[Request] Path: ' + path)
        logging.debug('[Request] Headers: ' + str(headers))
        logging.debug('[Request] Body: ' + body)
        return method, path, body, headers

    def _get_conn(self):
        """ Perpare for HTTP connection
        """
        if STREAMAPIS[self.api_name]['safe']:
            conn = httplib.HTTPSConnection(STREAMAPIS[self.api_name]['host'])
        else:
            conn = httplib.HTTPConnection(STREAMAPIS[self.api_name]['host'])
        return conn



    def stream(self):
        """ streaming twitter's api
        """

        try:
            httpconn = self._get_conn()
            httpconn.request(*self._get_req_paras())
            httpresp = httpconn.getresponse()
            buf = httpresp.read(1000)
            while self.running and len(buf) > 0:
                self.writer.write(buf)
                buf = httpresp.read(1000)
                if len(buf) == 0:
                    logging.warn('Connection closed unexpectedly.')
        except Exception as e:
            logging.warn(e)


        # If the reader reach the end of the streaming, or there is a request of
        # stopping, the streamming process will stop.
        logging.info('Streaming stopped')

    def stop(self, signum, frame):
        """ stop streaming
        """
        if signum == signal.SIGINT or signum == signal.SIGTERM:
            logging.info('Stopping...')
            self.running = False

if __name__ == '__main__':
    import sys
    logging.basicConfig(format='%(asctime)s::%(levelname)s::%(message)s', level=logging.INFO)
    logging.info('Version: ' + __version__)
    w = tcrawl.writer.LineBufferedWriter(sys.argv[1], is_compressed=True)
    #s = Streamer('filter', w, locations='-127.33,24.68,-76.83,49.22')
    s = Streamer('filter', w, locations='-74.25909,40.477399,-73.700272,40.917577,-87.940101,41.644582,-87.523661,42.023019,-118.668157,33.704554,-117.753334,34.337306,-122.51368188,37.70813196,-122.35845384,37.83245301')
    s.user_account(sys.argv[2], sys.argv[3])
    #logging.info('parameters: ' + 'locations=-127.33,24.68,-76.83,49.22')
    logging.info('Sampling from 4 cities')
    # register ctrl-c singal for interrupting
    signal.signal(signal.SIGINT, s.stop)
    signal.signal(signal.SIGTERM, s.stop)
    # start streamming
    logging.info('streaming started.')
    while s.running:
        s.stream()
    w.close()



