import socket
import random
import sys
import threading
import Queue 
import logging
import time
LOG_REQ_SIZE = 32

class prediction_t(object):
    def __init__(self, ctr_servers, num_of_conns = 1, timeout = 10):
        self._timeout = timeout
        self.ctr_servers = ctr_servers
        self.connect()
    
    def connect(self):
        ctr_servers = self.ctr_servers
        timeout = self._timeout
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout( timeout )
        server = ctr_servers[0]
        sock.connect( server )
        self.sock = sock


    def predict(self,  req, num_req =None ):
        if num_req == None:
            num_req = req.count('\n')
        try:
            logging.debug('_prediction begin send all')
            self.sock.sendall( req )
            logging.debug('_prediction end send all')
        except Exception as e:
            logging.error(e)
            raise
            return self.on_error( self.sock, req)
        else:
            datas = []
            num_resp = 0
            logging.debug('_prediction begin recv all')
            while 1:
                logging.debug('_prediction recv')
                try:
                    data = self.sock.recv( 5120 )
                    if data:
                        #print data
                        datas.append( data )
                        num_resp +=  data.count('\n')
                        if num_resp == num_req:
                            break
                    else:
                        self.on_error( self.sock, req)
                except socket.timeout:
                    logging.warn('prediction with network timeout for req(size=%d) %s...', len(req ), req[:-1][:LOG_REQ_SIZE])
                    return self.on_error( self.sock, req)
                except socket.error,e:
                    logging.warn('prediction with network error %s for req(size=%d) %s...', e, len(req), req[:-1][:LOG_REQ_SIZE])
                    return self.on_error( self.sock, req )
            logging.debug('_prediction end recv all')
            resp_str = ''.join(datas)
            resp_list = resp_str.split('\n')
            try:
                resps = [ float(resp.strip().split()[0]) for resp in resp_list if resp.strip()]
            except:
                logging.error('parse resps failed %s...', resp_str[:-1][:LOG_REQ_SIZE])
                return None
            return resps

    def on_error(self, sock, req ):
        self.sock.close()
        self.connect()
        return None

if __name__ == '__main__':
    pred = prediction_t([('172.17.16.126', 8890),], 1,)
    pred = prediction_t([('127.0.0.1', 9888),], 20,)
    pred_req1 = ' |u 1 |s 2 |r a bs\n'
    pred_req2 = ' |u 2 |s 3 |r a c\n'
    user = ' |u 1 2 3|k 4 7\n'
    user_len = len(user)
    item1 = '|a 1 2 3|b 7 8 9\n'
    item_len = len(item1)     
    item2 = '|a 11 21 31|b 17 18 19\n'
    item = [item2] * 600
    item = ''.join(item)
    item_len += len(item)
    import struct
    fmt = '!2sii%ds'%(user_len+item_len,)
    print >>sys.stderr, user_len, item_len
    s = struct.pack(fmt,'v1', user_len, item_len,user+item1+item )
    print >>sys.stderr, 'packet size:', len(s)
    i = 0
    t = 0
    while i<10000:
        print >>sys.stderr, 'predit %i begin'%i
        t1 = time.time()
        rst = pred.predict( s, 601)
        i += 1
        t2 = time.time()
        print >>sys.stderr, 'predit %i end'%i
        #print rst
        t += t2-t1
        print >>sys.stderr, t2-t1
        #time.sleep(0.001)
    print t/1000
