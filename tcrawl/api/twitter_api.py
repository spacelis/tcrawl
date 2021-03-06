#!python
# -*- coding: utf-8 -*-
"""File: twitter_api.py
Description:
    Contains http link to twitter's API
History:
    0.1.6 minor changes to get entities in REST API and to retrieve
        only recent tweets in search API
    0.1.5 Rewrite to make it simpler to maintain
    0.1.0 The first version.
"""
__version__ = '0.1.6'
__author__ = 'SpaceLis'

import re, logging, time, json

from tcrawl.api import APIError, buildpath, api_call, stream_call, api_call2, sleep
from text_util import html_filter

SINCEID_PATTERN = re.compile('()&since_id=\\d+|(\?)since_id=\\d+&')
PAGE = re.compile(r'page')
URLPATHEX = re.compile(r'http://.*?/')


def search(**kargs):
    """Search API"""
    host = 'search.twitter.com'
    api_path = '/search.json'
    kargs['result_type'] = 'recent'
    path = buildpath(api_path, kargs)
    statuses = list()
    while True:
        try:
            resp = api_call(host, path, True)
            jresp = json.loads(resp.read())
            statuses.extend(jresp['results'])
            if 'next_page' in jresp:
                path = api_path + jresp['next_page']
                continue
        except APIError as twe:
            if twe.code == 420:
                if twe.resp.read().find('limited') > 0:
                    sleeptime = int(twe.resp.getheader('Retry-After')) + 1
                    logging.warning(
                        'Rate limits exceeded, retry after {0} sec'.format(\
                        sleeptime))
                    sleep(sleeptime)
                    continue
            elif twe.code == 403:
                #if PAGE.search(twe.resp.read())!=None:
                    #break
                if twe.resp.read().find('since_id') > 0:
                    path = SINCEID_PATTERN.sub(r'\1', path)
                    continue
        break
    return statuses

def rest(api_path, **kargs):
    """REST API"""
    host = 'api.twitter.com'
    path = buildpath(api_path, kargs)
    ret = None
    while True:
        try:
            resp = api_call(host, path, True)
            ret = json.loads(resp.read())
            return ret
        except APIError as twe:
            if twe.code == 400:
                rate = int(twe.resp.getheader('X-RateLimit-Remaining', 0))
                if rate < 1:
                    sleeptime = int(twe.resp.getheader('X-RateLimit-Reset')) \
                            - int(time.time())
                    logging.warning( \
                        'Rate limits exceeded, retry after {0} sec'.format( \
                        sleeptime))
                    sleep(sleeptime)
            elif twe.code == 503:
                logging.warning('Service Unavailable. Retry after 1 min')
                sleep(60)
            else: raise twe


def wrapped_search(**karg):
    """use REST API to reform the result tweets from search API
    """
    statuses_sr = search(**karg)
    ids = [status['id'] for status in statuses_sr]
    statuses = list()
    for sid in ids:
        statuses.append(get_status(id=sid))
    return statuses

def iterpage(twapi, **kargs):
    """Iterating pages"""
    page = 1
    statuses = list()
    while True:
        kargs['page'] = page
        status_page = twapi(**kargs)
        if len(status_page) > 0:
            statuses.extend(status_page)
            page += 1
        else: break
    return statuses

def followers_ids(**kargs):
    """Iterating pages in API of followers"""
    kargs['cursor'] = -1
    followers = list()
    while True:
        follower_page = rest('/1/followers/ids.json', **kargs)
        followers.extend(follower_page['ids'])
        if follower_page['next_cursor'] == 0:
            break
        else:
            kargs['cursor'] = follower_page['next_cursor']
    return followers

def friends_ids(**kargs):
    """Iterating pages in API of friends/ids"""
    kargs['cursor'] = -1
    friends = list()
    while True:
        friends_page = rest('/1/friends/ids.json', **kargs)
        friends.extend(friends_page['ids'])
        if friends_page['next_cursor'] == 0:
            break
        else:
            kargs['cursor'] = friends_page['next_cursor']
    return friends

def user_timeline(**kargs):
    """Retrieve a user's time line"""
    kargs['include_entities'] = 'true'
    return rest('/1/statuses/user_timeline.json', **kargs)

def geo_id(**kargs):
    """Retrieve a place's information"""
    return rest('/1/geo/id/%(place_id)s.json', **kargs)

def get_status(**kargs):
    """Retrieve the tweet specified by id"""
    kargs['include_entities'] = 'true'
    return rest('/1/statuses/show/%(id)s.json', **kargs)


def html_status(**kargs):
    """ Retrieve the tweets specified by id and username by web access"""
    tid = kargs['id']
    uname = kargs['screen_name']
    host = 'twitter.com'
    path = '%s/status/%d' % (uname, tid)
    html = api_call2(host, path, False).read().decode('utf-8', errors='ignore')
    status = htmlstatus2dict(html)
    status['id'] = kargs['id']
    status['screen_name'] = kargs['screen_name']
    return status

STATUS_DELIM_F = {
    'text': ['<span class="entry-content">', '</span>'],
    'created_at':['<span class="published timestamp" data="{time:\'', '\'}">'],
    'ref_id': ['id="status_', '"'],

    #'user.id': [],
    'user.screen_name': ['<div class="thumb"><a href="http://twitter.com/', '"'],
    'user.name': ['<div class="full-name">', '</div>'],
    'place.id':[' data=\'{"place_id":"', '"'],
    'retweet_num': ['<span class="shared-content">Retweeted by ', ' '],
    }

STATUS_DELIM_B = {
    'lang': ['<meta content="', '" http-equiv="Content-Language" />'],
    'in_reply_to': ['/status/', '">in reply to '],
    }



def htmlstatus2dict(html):
    """ Extract status elements from HTML and assemble it as a dict
    """
    status = dict()
    for key in STATUS_DELIM_F.iterkeys():
        stptn = STATUS_DELIM_F[key][0]
        endptn = STATUS_DELIM_F[key][1]
        start = html.find(stptn)
        if start == -1:
            continue
        start += len(stptn)
        end = html.find(endptn, start)
        if end == -1:
            continue
        content = html[start:end]
        keypath = key.split('.')
        pnode = status
        for nidx in range(0, len(keypath) - 1):
            if keypath[nidx] not in pnode:
                pnode[keypath[nidx]] = dict()
            pnode = pnode[keypath[nidx]]
        if keypath[-1] == 'text':
            content = html_filter(content)
        pnode[keypath[-1]] = content.encode('utf-8', errors='ignore')

    for key in STATUS_DELIM_B.iterkeys():
        stptn = STATUS_DELIM_B[key][0]
        endptn = STATUS_DELIM_B[key][1]
        end = html.find(endptn)
        if end == -1:
            continue
        start = html.rfind(stptn, 0, end)
        if start == -1:
            continue
        start += len(stptn)
        content = html[start:end]
        keypath = key.split('.')
        pnode = status
        for nidx in range(0, len(keypath) - 1):
            if keypath[nidx] not in pnode:
                pnode[keypath[nidx]] = dict()
            pnode = pnode[keypath[nidx]]
        if keypath[-1] == 'text':
            content = html_filter(content)
        pnode[keypath[-1]] = content.encode('utf-8', errors='ignore')
    status['html'] = html.encode('utf-8', errors='ignore')
    return status

def test():
    """test"""
    #print foursq_search(ll='44.3,37.2')
    #print search_api(q='haha')
    #print place_id(place_id='247f43d441defc03')
    #while True:
    #iterpage(user_timeline, user_id=1317)
    #print followers_ids(user_id=171624164)
    print html_status(id=27759797860306944, screen_name='spacelis')


if __name__ == '__main__':
    #print search_api(q='greenpois0n', rpp=100)
    test()

