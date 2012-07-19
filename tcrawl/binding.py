#!python
# -*- coding: utf-8 -*-
"""File: binding.py
Description:
    A group of functions that connect the methods to APIs
History:
    0.1.0 The first version.
"""
__version__ = '0.1.0'
__author__ = 'SpaceLis'

import logging
from tcrawl.api import api
from tcrawl.api import twitter_api
from tcrawl.api import foursq_api
from tcrawl.api import pic_service_api
from tcrawl.api import google_api
from tcrawl.api import bing_api

def tweet_by_user(paras):
    """Crawling tweets by API status/user_timeline
    """
    statuses = twitter_api.iterpage(twitter_api.user_timeline, \
            user_id=paras[0], \
            since_id=paras[1], \
            count=200)
    logging.info('{0} tweets collected from user {1}'.\
            format(len(statuses), paras[0]))
    return {'list': statuses}

def tweet_by_geocode(paras):
    """Crawling tweets by API search?geocode=
    """
    statuses = twitter_api.wrapped_search(geocode=paras[0], \
                        since_id=paras[1], \
                        rpp=100, \
                        q='')
    logging.info('{0} tweets collected from geocode={1}'.\
            format(len(statuses), paras[0]))
    return {'list': statuses}

def tweet_by_pid(paras):
    """Crawling tweets by API search?place:
    """
    statuses = twitter_api.wrapped_search(q='place:' + paras[0], \
                        since_id=paras[1], \
                        rpp=100)
    logging.info('{0} tweets has been retrieved from place {1}'.\
            format(len(statuses), paras[0]))
    return {'list': statuses}

def tweet_by_id(paras):
    """Retrieve one tweet from API statuses/{id}
    """
    logging.info('TID={0}'.format(paras[0]))
    status = twitter_api.get_status(id=int(paras[0]))
    if status:
        return {'list': (status,)}
    else:
        return {'list': list()}

def twitter_place_by_id(paras):
    """Retrieve one place from API place/{id}
    """
    logging.info('PID={0}'.format(paras[0]))
    place = twitter_api.geo_id(place_id=paras[0])
    if place:
        return {'list': (place,)}
    else:
        return {'list': list()}

def fsq_place_by_name(paras):
    """Retrieve one place from 4sq API venue/{id}
    """
    logging.info('Place {0}, {1}'.format(paras[1], paras[2]))
    place = foursq_api.search (ll=paras[1], query=paras[2],
        intend='match')
    if place:
        place['t_place_id'] = paras[0]
        return {'list': (place, )}
    else:
        return {'list': list()}

def twitter_fol_fri_by_uid(paras):
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

def google_search(paras):
    """Retrieve search results from Google
    """
    logging.info('keyword={0}'.format(paras[1]))
    sres = google_api.websearch(q = paras[1], rsz = 8, v = '1.0')
            #key='AIzaSyCZyU1PER77rHKYfZXC2sE-N2PzLieRz88')
    if len(sres) > 0:
        return {'list': ({'q': paras[0], \
                'gresults': sres},)}
    else: return {'list': list()}

def bing_search(paras):
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

def webpage_by_url(paras):
    """Retrieve web pages from url
    """
    logging.info('URL: {0}'.format(paras[1]))
    try:
        web = api.api_call2(*api.urlsplit(paras[1])).read(). \
                decode('utf-8', errors='ignore')
        if len(web) == 0:
            return {'list': list(),}
        return {'list': ({'place_id': paras[0], \
                'web': web},)}
    except:
        return {'list': list(),}

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

def pic_by_url(paras):
    """Retrieve picture from online picture service
    """
    logging.info('Picture from {0}'.format(paras[0]))
    urlpart = paras[0].split('/')
    api_method = _SERVICEPROVIDERS[urlpart[2]]
    pic = api_method(url=paras[0])
    return {'pic': pic, 'name': paras[1]}
