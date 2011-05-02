#!python
# -*- coding: utf-8 -*-
"""File: streamer.py
Description:
    Utilizing the stream APIs provided to crawl data
History:
    0.1.0 The first version.
"""
__version__ = '0.1.0'
__author__ = 'SpaceLis'

#FIXME not really usable
class Streamer(object):
    """Use stream APIs to crawl data
    """
    def __init__(self, method):
        super(Streamer, self).__init__()
        self.method = method

    def stream(para_file):
        kargs = json.loads(open(para_file).read())
        kargs['writer'] = self.writer

def streaming(stream_type):
    """Start stream
    """
    signal.signal(signal.SIGINT, crl.stop)

    flog = gen_filename('log', stream_type,'log')
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


    crl.crawl(para_file)

if __name__ == '__main__':
    streaming(sys.argv[1], sys.argv[2])
