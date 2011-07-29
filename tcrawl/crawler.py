#!python
# -*- coding: utf-8 -*-
"""File: threadcrawl.py
Description:
    Using threadpool to speed up crawling.
    Including crawling by user, crawling by POI.
History:
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
__version__ = '0.2.5'
__author__ = 'SpaceLis'

import os, sys, time, json, threading, logging, signal
import gzip
import threadpool
from tcrawl import api, twitter_api, foursq_api
from tcrawl import pic_service_api, google_api, bing_api

#pylint: disable-msg=E1103

#---------------------------------------------------------- Utility Functions
def gen_filename(path, name, ext):
    """
        format filename with this certain pattern.
    """
    return '{0}/{1}-{2}.{3}'.format( \
            path, \
            name, \
            time.strftime('%d_%m_%Y-%H_%M_%S'), \
            ext)


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

    def set_writer(self, writer):
        """Set a writer for the crawler"""
        self.writer = writer

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

class Writer(object):
    """Storing retrieving results"""
    def __init__(self):
        super(Writer, self).__init__()
        self.lock = threading.RLock()

    def write(self, kargs):
        """Write the content in to a file
        """
        raise NotImplementedError

    def flush(self):
        """Flush the writer
        """
        raise NotImplementedError

    def close(self):
        """Close the writer
        """
        raise NotImplementedError

    def dest(self):
        """Return the destination
        """
        raise NotImplementedError

class LineWriter(Writer):
    """Write the result list into a file"""
    def __init__(self, dst, is_compressed):
        super(LineWriter, self).__init__()
        self.dst = dst
        if is_compressed:
            self.fout = gzip.open(dst, 'w')
        else:
            self.fout = open(dst, 'w')
    def write(self, line):
        self.lock.acquire()
        print >> self.fout, line.encode('utf-8',
                errors='ignore')
        self.lock.release()

    def flush(self):
        """Flush the writer
        """
        self.lock.acquire()
        self.fout.flush()
        self.lock.release()

    def close(self):
        """Close the writer
        """
        self.lock.acquire()
        self.fout.close()
        self.lock.release()

    def dest(self):
        """return the destination"""
        return self.dst

class JsonList2FileWriter(Writer):
    """Write the result list into a file"""
    def __init__(self, dst, is_compressed):
        super(JsonList2FileWriter, self).__init__()
        self.dst = dst
        if is_compressed:
            self.fout = gzip.open(dst, 'w')
        else:
            self.fout = open(dst, 'w')
    def write(self, kargs):
        self.lock.acquire()
        for item in kargs['list']:
            print >> self.fout, json.dumps(item).encode('utf-8',
                    errors='ignore')
        self.lock.release()

    def flush(self):
        """Flush the writer
        """
        self.lock.acquire()
        self.fout.flush()
        self.lock.release()

    def close(self):
        """Close the writer
        """
        self.lock.acquire()
        self.fout.close()
        self.lock.release()

    def dest(self):
        """return the destination"""
        return self.dst

class PicFileWriter(Writer):
    """Write the result list into a file"""
    def __init__(self, dst):
        self.dst = dst
        super(PicFileWriter, self).__init__()

    def write(self, kargs):
        """Write the pic to a file
        """
        if os.path.exists(kargs['name']):
            i = 1
            while os.path.exists(kargs['name'] + '_{0}'.format(i)):
                i += 1
            kargs['name'] += '_{0}'.format(i)

        self.lock.acquire()
        fout = open(self.dst + ('/' \
                if self.dst[-1]!='/' or self.dst[-1]!='\\' else '')
                + kargs['name'], 'wb')
        print >> fout, kargs['pic']
        fout.close()
        self.lock.release()

    def flush(self):
        """Flush the writer
        """
        pass

    def close(self):
        """Close the writer
        """
        pass

    def dest(self):
        """Return the destination"""
        return self.dst

class StreamWriter(Writer):
    """Write the stream into a file
    """
    def __init__(self, dst, is_compressed):
        super(StreamWriter, self).__init__()
        self.dst = dst
        if is_compressed:
            self.fout = gzip.open(dst, 'w')
        else:
            self.fout = open(dst, 'w')

    def write(self, line):
        self.lock.acquire()
        print >> self.fout, line
        self.lock.release()

    def flush(self):
        """Flush the writer
        """
        self.lock.acquire()
        self.fout.flush()
        self.lock.release()

    def close(self):
        """Close the writer
        """
        self.lock.acquire()
        self.fout.close()
        self.lock.release()

    def dest(self):
        """return the destination"""
        return self.dst


#--------------------------------------------------------- Crawling Methods
def by_user(paras):
    """Crawling tweets by API status/user_timeline
    """
    statuses = twitter_api.iterpage(twitter_api.user_timeline, \
            user_id=paras[0], \
            since_id=paras[1], \
            count=200)
    logging.info('{0} tweets collected from user {1}'.\
            format(len(statuses), paras[0]))
    return {'list': statuses}

def by_geocode(paras):
    """Crawling tweets by API search?geocode=
    """
    statuses = twitter_api.wrapped_search(geocode=paras[0], \
                        since_id=paras[1], \
                        rpp=100, \
                        q='')
    logging.info('{0} tweets collected from geocode={1}'.\
            format(len(statuses), paras[0]))
    return {'list': statuses}

def by_pid(paras):
    """Crawling tweets by API search?place:
    """
    statuses = twitter_api.wrapped_search(q='place:' + paras[0], \
                        since_id=paras[1], \
                        rpp=100)
    logging.info('{0} tweets has been retrieved from place {1}'.\
            format(len(statuses), paras[0]))
    return {'list': statuses}

def retrieve_tweet(paras):
    """Retrieve one tweet from API statuses/{id}
    """
    logging.info('TID={0}'.format(paras[0]))
    status = twitter_api.get_status(id=int(paras[0]))
    if status:
        return {'list': (status,)}
    else:
        return {'list': list()}

def retrieve_place(paras):
    """Retrieve one place from API place/{id}
    """
    logging.info('PID={0}'.format(paras[0]))
    place = twitter_api.geo_id(place_id=paras[0])
    if place:
        return {'list': (place,)}
    else:
        return {'list': list()}

def retrieve_place4sq(paras):
    """Retrieve one place from 4sq API venue/{id}
    """
    logging.info('Place {0}, {1}'.format(paras[1], paras[2]))
    place = foursq_api.search (ll=paras[2], query=paras[1], limit=1,
        intend='match')
    if place:
        place['t_place_id'] = paras[0]
        return {'list': (place, )}
    else:
        return {'list': list()}

def retrieve_followers_friends(paras):
    """Retrieve users' followers from follower_ids
    """
    logging.info('UserID={0}'.format(paras[0]))
    followers = twitter_api.followers_ids(user_id = paras[0])
    friends = twitter_api.friends_ids(user_id = paras[0])
    if len(followers)>0:
        return {'list': ({'user_id':paras[0], \
                'foids': followers, \
                'frids': friends},)}
    else: return {'list': list()}

def retrieve_google_search(paras):
    """Retrieve search results from Google
    """
    logging.info('keyword={0}'.format(paras[1]))
    sres = google_api.websearch(q = paras[1], rsz = 8, v = '1.0')
            #key='AIzaSyCZyU1PER77rHKYfZXC2sE-N2PzLieRz88')
    if len(sres) > 0:
        return {'list': ({'q': paras[0], \
                'gresults': sres},)}
    else: return {'list': list()}

def retrieve_bing_search(paras):
    """Retrieve search results from Google
    """
    logging.info('keyword={0}'.format(paras[1]))
    sres = bing_api.searchrequest(Query = paras[1], Sources='Web',
            Version='2.0', Market='en-US',
            AppId='ED4FF446CC9C1BA6BD0F1B013DE8B3A040F6D89E')
    if len(sres) > 0:
        return {'list': ({'q': paras[0], \
                'gresults': sres},)}
    else: return {'list': list()}

def retrieve_web_page(paras):
    """Retrieve web pages from url
    """
    logging.info('URL: {0}'.format(paras[1]))
    opener = api.api_call2(*api.urlsplit(paras[1]))
    web = opener.read().decode('utf-8', errors='ignore')
    oriurl = opener.geturl()
    if len(web) == 0:
        return {'list': list(),}
    return {'list': ({'tid': paras[0],
            'url': paras[1],
            'ourl': oriurl,
            'web': web},)}

def retrieve_url(paras):
    """Retrieve web pages from url
    """
    logging.info('URL: {0}'.format(paras[0]))
    web = api.api_call2(*api.urlsplit(paras[0])).read(). \
            decode('utf-8', errors='ignore')
    web = web.replace('\n', ' ')
    web = web.replace('\r', ' ')
    return ' '.join([paras[0], web])

# a list of usable picture service support by this crawling module
_SERVICEPROVIDERS = {'twitpic.com':pic_service_api.get_twit_pic,
                    'yfrog.com':pic_service_api.get_yfrog_pic,
                    'tweetphoto.com': pic_service_api.get_tweetphoto_pic,
                    'plixi.com': pic_service_api.get_tweetphoto_pic}

def retrieve_pic(paras):
    """Retrieve picture from online picture service
    """
    logging.info('Picture from {0}'.format(paras[0]))
    urlpart = paras[0].split('/')
    api_method = _SERVICEPROVIDERS[urlpart[2]]
    pic = api_method(url=paras[0])
    return {'pic': pic, 'name': paras[1]}

#---------------------------------------------------------- Main Function
def crawl(crawl_type, para_file):
    """Main Crawling function
    """

    crl = Crawler()

    # Set a Writer for the crawler
    if crawl_type == 'picture':
        crl.set_writer(PicFileWriter('data/pic'))
    elif crawl_type == 'url':
        crl.set_writer(LineWriter(\
                gen_filename('data', crawl_type, 'ljson.gz'), True))
    else:
        crl.set_writer(JsonList2FileWriter(\
                gen_filename('data', crawl_type, 'ljson.gz'), True))

    # Set a method for the crawler
    method = {'tweet_u': by_user,
            'tweet_p': by_pid,
            'tweet': retrieve_tweet,
            'place': retrieve_place,
            'tweet_g': by_geocode,
            'place_4sq': retrieve_place4sq,
            'picture': retrieve_pic,
            'followers': retrieve_followers_friends,
            'websearch_g': retrieve_google_search,
            'websearch_b': retrieve_bing_search,
            'web': retrieve_web_page,
            'url': retrieve_url,
        }
    if crawl_type in method:
        crl.set_method(method[crawl_type])
    else:
        print 'Wrong parameters!'
        return

    # register Ctrl-C singal for interrupting
    signal.signal(signal.SIGINT, crl.stop)

    # prepare for logging
    flog = gen_filename('log', crawl_type,'log')
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
