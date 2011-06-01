#!python
# -*- coding: utf-8 -*-
"""File: pic_service_api.py
Description:
    Online picture services
History:
    0.1.2 support tweetpoto.com (plixi.com)
    0.1.0 The first version.
"""
__version__ = '0.1.0'
__author__ = 'SpaceLis'

from tcrawl.api import api_call, urlsplit

def get_twit_pic(**kargs):
    """Retrieve the picture from TwitPic"""
    twitpage = api_call(*urlsplit(kargs['url'])).read()
    anchor = '<img class="photo" id="photo-display" src="'
    start = twitpage.index(anchor) + len(anchor)
    end = twitpage.index('"', start)
    imgurl = twitpage[start:end]
    return api_call(*urlsplit(imgurl)).read()

def get_yfrog_pic(**kargs):
    """Retrieve the picture from YFrog
    """
    host, path, secure = urlsplit(kargs['url'])
    pic = api_call(host, path +':iphone', secure).read()
    return pic

def get_twitgoo_pic(**kargs):
    """Retrieve the picture from TwitGoo
    """
    host, path, secure = urlsplit(kargs['url'])
    pic = api_call(host, path +'/img', secure).read()
    return pic

def get_tweetphoto_pic(**kargs):
    """Retrieve the picture from TweetPhoto or Plixi.com
    """
    pic_page = api_call(*urlsplit(kargs['url'])).read()
    anchor = '" alt="" id="photo"'
    end  = pic_page.find(anchor)
    start = pic_page.rfind('"', 0, end) + 1
    imgurl = pic_page[start:end]
    return api_call(*urlsplit(imgurl)).read()

# a list of usable picture service support by this crawling module
_SERVICEPROVIDERS = {'twitpic.com':pic_service_api.get_twit_pic, \
                    'yfrog.com':pic_service_api.get_yfrog_pic, \
                    'tweetphoto.com': pic_service_api.get_tweetphoto_pic, \
                    'plixi.com': pic_service_api.get_tweetphoto_pic}
def get_pic(**kargs):
    """ Retrieving Pictures from the right site
    """
    urlpart = kargs['url'].split('/')
    pic_api = _SERVICEPROVIDERS[urlpart[2]]
    return pic_api(**kargs)

def test():
    """A test
    """
    fout = open('test.jpg', 'wb')
    #print >> fout, get_twitgoo_pic(url = 'http://twitgoo.com/216kxf')
    print >> fout, get_tweetphoto_pic(url = 'http://tweetphoto.com/36367177')


if __name__ == '__main__':
    test()
