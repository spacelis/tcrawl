#!python
# -*- coding: utf-8 -*-
"""File: writer.py
Description:
    This module contains a set of classes for writing to file system
History:
    0.1.0 The first version.
"""
__version__ = '0.1.0'
__author__ = 'SpaceLis'

import threading, re, os, gzip, json

_MULTISLASH = re.compile(r'/+')

def newfilename(filename, ext="", zeropads=2):
    """ generate a new filename if the filename if the filename already exists
        @arg filename the expected filename in str()
        @return the new filename that is not taken
    """
    newname = '%s.%s' % filename, ext
    if os.path.exists(newname):
        i = 1
        newname = filename + (('_%0' + str(zeropads)+'d.' + ext) % i)
        while os.path.exists(newname):
            i += 1
            newname = filename + (('_%0' + str(zeropads)+'d' + ext) % i)
    return newname

def concatpath(*paths):
    """ concatenate path without disturbing slashes
    """
    lpath = list("".join(paths))
    for i in range(len(lpath)):
        if lpath[i] == '\\':
            lpath[i] = '/'
    path = "".join(lpath)
    path, dummy = _MULTISLASH.subn('/', path)
    return path

class FileWriter(object):
    """Storing retrieving results"""
    def __init__(self):
        super(FileWriter, self).__init__()
        self.lock = threading.RLock()
        self.fout = None
        self.dst = None

    def write(self, kargs):
        """Write the content in to a file
        """
        raise NotImplementedError

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

class LineWriter(FileWriter):
    """Write the result list into a file"""
    def __init__(self, dst, **kargs):
        super(LineWriter, self).__init__()
        if kargs['is_compressed']:
            self.fout = gzip.open(dst, 'w')
        else:
            self.fout = open(dst, 'w')

    def write(self, line):
        self.lock.acquire()
        print >> self.fout, line.encode('utf-8',
                errors='ignore')
        self.lock.release()

class JsonList2FileWriter(LineWriter):
    """Write the result list into a file"""
    def __init__(self, dst, **kargs):
        super(JsonList2FileWriter, self).__init__(dst, **kargs)

    def write(self, kargs):
        self.lock.acquire()
        for item in kargs['list']:
            print >> self.fout, json.dumps(item).encode('utf-8',
                    errors='ignore')
        self.lock.release()

class DirectoryWriter(object):
    """Write the result list into a directory as files"""
    def __init__(self, dst, **karg):
        super(DirectoryWriter, self).__init__()
        self.lock = threading.RLock()
        self.dst = dst

    def write(self, kargs):
        """Write the data to a file
        """
        self.lock.acquire()
        fname = newfilename(concatpath(self.dst, kargs['name']), "")
        fout = open(fname, 'wb')
        print >> fout, kargs['data']
        fout.close()
        self.lock.release()

    def dest(self):
        """Return the destination"""
        return self.dst

def test():
    """test
    """
    print concatpath('/asdfio/asdi/', '\\iadfne/fidosa/')
    with open(newfilename('haha'), 'w') as fout:
        print >> fout, 'haha'
    with open(newfilename('haha'), 'w') as fout:
        print >> fout, 'haha'
    with open(newfilename('haha'), 'w') as fout:
        print >> fout, 'haha'




if __name__ == '__main__':
    test()
