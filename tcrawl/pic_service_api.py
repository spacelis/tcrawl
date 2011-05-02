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
    host, path = urlsplit(kargs['url'])
    twitpage = api_call(host, path, True).read()
    anchor = '<img class="photo" id="photo-display" src="'
    start = twitpage.index(anchor) + len(anchor)
    end = twitpage.index('"', start)
    imgurl = twitpage[start:end]
    host, path = urlsplit(imgurl)
    return api_call(host, path, True).read()


def get_yfrog_pic(**kargs):
    """Retrieve the picture from YFrog
    """
    host, path = urlsplit(kargs['url'])
    pic = api_call(host, path +':iphone', True).read()
    return pic


def get_twitgoo_pic(**kargs):
    """Retrieve the picture from TwitGoo
    """
    host, path = urlsplit(kargs['url'])
    pic = api_call(host, path +'/img', False).read()
    return pic

def get_tweetphoto_pic(**kargs):
    """Retrieve the picture from TweetPhoto or Plixi.com
    """
    host, path = urlsplit(kargs['url'])
    pic_page = api_call(host,  path , False).read()
    anchor = '" alt="" id="photo"'
    end  = pic_page.find(anchor)
    start = pic_page.rfind('"', 0, end) + 1
    imgurl = pic_page[start:end]
    host, path = urlsplit(imgurl)
    return api_call(host, path, False).read()


def test():
    """A test
    """
    fout = open('test.jpg', 'wb')
    #print >> fout, get_twitgoo_pic(url = 'http://twitgoo.com/216kxf')
    print >> fout, get_tweetphoto_pic(url = 'http://tweetphoto.com/36367177')


if __name__ == '__main__':
    test()
