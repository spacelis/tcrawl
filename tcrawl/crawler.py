#!python
# -*- coding: utf-8 -*-
"""File: threadcrawl.py
Description:
    Using threadpool to speed up crawling.
    Including crawling by user, crawling by POI.
History:
    0.2.6 ! rearrange the layout of the code
    0.2.5 ! change web page retrieving method to api_call2
    0.2.4 + web page retrieving method
    0.2.3 + google search API and Bing search API
    0.2.2 ! change search API to wrapped_search() to make sure
            the format
    0.2.1 add auto rename function to PicFileWriter
    0.2.0 rewrite to get rid of Tweepy to keep the lines simple
    0.1.7 rewrite with new modified Tweepy
    0.1.6 fix the bug of leaving last 99 queries undone
            and add Foursqure place genre retrieving
    0.1.5 minor changes for names
    0.1.4 add function to crawl by place id (POI)
    0.1.3 add function of retrieving place information
    0.1.2 + re-implemented by classes
    0.1.1 + support max_id to avoid duplicated retrieval
    0.1.0 The first version.
"""
__version__ = '0.2.6'
__author__ = 'SpaceLis'


import sys
import logging
import signal


import threadpool
import binding
import writer
from tcrawl.api import api

#pylint: disable-msg=E1103

#---------------------------------------------------------- Main Classes
class Crawler(object):
    """The base class for crawlers
    """
    #pylint: disable-msg=R0903
    def __init__(self, poolsize = 20):
        """Default contructor
        """
        self.name = None
        self.writer = None
        self.method = None
        self.poolsize = poolsize
        self.stopped = False

    def set_writer(self, _writer):
        """Set a writer for the crawler"""
        self.writer = _writer

    def get_writer(self):
        """Set a writer for the crawler"""
        return self.writer

    def set_method(self, method):
        """Set a crawling method for the crawler"""
        self.method = method

    def crawl(self, para_file):
        """ using thread pool to speed up crawling.
        """
        if not self.writer and not self.method:
            return
        fpara = open(para_file, 'r')

        pool = threadpool.ThreadPool(self.poolsize)
        parlst = list()
        for line in fpara:
            if self.stopped:
                break # Stop current crawling
            parlst.append(line.strip())
            if len(parlst) > 10:
                requests = threadpool.makeRequests(self.retrieve, parlst)
                map(pool.putRequest, requests)
                pool.wait()
                self.writer.flush()
                del parlst[:]

        #Flush the last part of lines in parlst
        if not self.stopped:
            requests = threadpool.makeRequests(self.retrieve, parlst)
            map(pool.putRequest, requests)
            pool.wait()
            self.writer.flush()

        fpara.close()
        self.writer.close()
        if not self.stopped:
            logging.info('Retrieving finished.')
        else:
            logging.info('Retrieving interrupted.')
        return

    def retrieve(self, para):
        """Retrieve one piece of data from the entrypoint
        """
        if self.stopped:
            logging.info('Interrupted before retrieving by ' + str(para))
            return
        paras = para.split('$')
        try:
            rtn = self.method(paras)
            self.writer.write(rtn)
        except api.APIError as err:
            logging.warning('Unexpected Error {0}: {1}\nParameter: {2}'.\
                    format(err.code, err.msg, paras))
        if self.stopped:
            logging.info('Interrupted after retrieving by ' + str(para))
            return
        return

    def stop(self, signum, dummy):
        """Handle the signal that stop crawling
        """
        if signum == signal.SIGINT:
            self.writer.lock.acquire()
            self.stopped = True
            logging.info('Ctrl-C issued')
            self.writer.lock.release()

#---------------------------------------------------------- Main Function
def crawl(crawl_type, para_file):
    """Main Crawling function
    """

    crl = Crawler()

    # Set a Writer for the crawler
    if crawl_type == 'picture':
        crl.set_writer(writer.PicFileWriter('data/pic'))
    elif crawl_type == 'url':
        crl.set_writer(writer.LineWriter(\
                writer.new_filename_time('data', crawl_type, 'ljson.gz'), True))
    else:
        crl.set_writer(writer.JsonList2FileWriter(\
                writer.new_filename_time('data', crawl_type, 'ljson.gz'), True))

    # Set a method for the crawler
    method = {'tweet_u': binding.tweet_by_user,
            'tweet_p': binding.tweet_by_pid,
            'tweet': binding.tweet_by_id,
            'place': binding.twitter_place_by_id,
            'tweet_g': binding.tweet_by_geocode,
            'place_4sq': binding.fsq_place_by_name,
            'picture': binding.pic_by_url,
            'followers': binding.twitter_fol_fri_by_uid,
            'websearch_g': binding.google_search,
            'websearch_b': binding.bing_search,
            'web': binding.webpage_by_url,
            'url': binding.retrieve_url,
        }
    if crawl_type in method:
        crl.set_method(method[crawl_type])
    else:
        print 'Wrong parameters!'
        return

    # register Ctrl-C singal for interrupting
    signal.signal(signal.SIGINT, crl.stop)

    # prepare for logging
    flog = writer.gen_filename_time('log', crawl_type,'log')
    logging.basicConfig( \
        filename= flog, \
        level=logging.INFO, \
        format='%(asctime)s::%(levelname)s::%(message)s')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)
    logging.info(__version__)
    logging.info('input:{0}'.format(sys.argv[2]))
    logging.info('output:{0}'.format(crl.get_writer().dest()))


    # start crawling
    crl.crawl(para_file)


if __name__ == '__main__':
    crawl(sys.argv[1], sys.argv[2])
