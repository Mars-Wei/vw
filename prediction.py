import socket
import random
import threading
import Queue 
import logging
import time
from pyutil.program.thread import ThreadPool
LOG_REQ_SIZE = 32
class conn_t(object):
    def __init__(self, sock, conn_pool):
        self.conn_pool = conn_pool
        self._sock = sock
        self.closed = False

    def recv(self, buf_size):
        return self._sock.recv( buf_size )

    def sendall(self, buf ):
        return self._sock.sendall( buf )
    
    def close(self):
        self._sock.close()
        self.closed = True

    def __del__(self):
        if  self.closed:
            sock = self.conn_pool.new_random_socket()
        else:
            sock = self._sock
        self.conn_pool.add( sock )

class connection_pool_t(object):
    def __init__(self, socket_factory,  servers, num_of_conns = 1):
        self._lifo = Queue.LifoQueue()
        self._socket_factory = socket_factory
        self._servers = servers
        self._num_of_conns = num_of_conns

        random.seed( hash(self) )



    def build_connection(self ):
        logging.debug('prediction build connection')
        num_of_conns = self._num_of_conns
        socket_factory = self._socket_factory
        servers = self._servers

        for i in range(num_of_conns):
            time.sleep( random.randint(1,5) )
            serv = random.choice( servers )
            connected = False
            logging.debug('try connect to %s  ', serv)
            max_try = 5
            for i in range( max_try ):
                sock = socket_factory( serv )
                if sock: 
                    connected = True
                    self.add(sock)
                    break
                time.sleep( random.randint(1,12) )

            if connected:    
                logging.debug('connect to %s ok  ', serv)
            else:    
                logging.info('connect to %s failed  ', serv)

        builded_conn_size = self._lifo.qsize()
        logging.debug('make total %d connections',  builded_conn_size )
        return  builded_conn_size

    def backend_connect_thread(self):        
        num_of_conns = self._num_of_conns

        def _connect_loop():
            sleeps = range(1,61)
            idx = 0
            while 1:
                if self.empty():
                    logging.info('backend thread try connect to servers with num_of_conns=%d', num_of_conns)
                    self.build_connection()
                time.sleep( random.random()*sleeps[ idx ] )
                idx += 1
                if idx >= 60: idx = 0

        _t = threading.Thread( target = _connect_loop )
        _t.setDaemon( True )
        _t.start()


    def empty(self):
        return self._lifo.empty()

    def add(self, sock):
        if sock:
            logging.debug("connection pool add valid sock")
            self._lifo.put(sock)
        else:
            logging.warn("connection pool add sock==None")

    def new_random_socket( self ):
        logging.info('prediction create new socket handler')
        serv_idx = random.randint(0, len(self._servers)-1)
        serv = self._servers[ serv_idx ] 
        sock = self._socket_factory( serv )
        return sock

    def pop_connection( self, timeout ):
        logging.debug('_prediction pop connection')
        if self._lifo.empty():
            return None
        else:    
            try:
                logging.debug('_prediction lifo get')
                sock = self._lifo.get(block=True, timeout = timeout)
                return conn_t(sock, self)
            except:
                logging.error('get sock handler from lifo failed')
                return None

class prediction_t(object):
    def __init__(self, ctr_servers, num_of_conns = 1, timeout = 0.1 , thread_num = 3, async_connect = False):
        self._timeout = timeout
        self._thrd_pool = ThreadPool( thread_num)
        self._async_connect = async_connect

        def sock_factory( server ):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout( timeout )
            try:
                sock.connect( server )
                return sock
            except socket.timeout:
                return None
            except socket.error:        
                return None

        self._conn_pool = connection_pool_t(sock_factory, ctr_servers, num_of_conns )    
        if self._async_connect :
            self._conn_pool.backend_connect_thread()


    def has_connection(self):
        return not self._conn_pool.empty()

    def build_connection(self):
        if not self._async_connect:
            return self._conn_pool.build_connection()

    class pred_req_t(object):
        pass

    def predict(self, req , timeout = 0.01   ):

        ts = time.time()
        rsp_que = Queue.Queue()
        pred_req = self.pred_req_t()
        pred_req.req = req
        pred_req.expired = False
        self._thrd_pool.queue_task( self._predict, (pred_req, rsp_que) )
        try:
            qs = rsp_que.get( True, timeout)
        except Queue.Empty:    
            qs = None
            pred_req.expired = True
            logging.error("get prediction timeout for req %s...", req[:-1][:LOG_REQ_SIZE])
        te = time.time()
        logging.info('predict req(size=%d) %s cost %f secs', len(req), req[:-1][:LOG_REQ_SIZE],  te-ts)
        return qs

    def _predict(self,  pred_req, rsp_que ):
        for i in xrange( 1000 ):
            if pred_req.expired:
                rsp_que.put( None )
                return
            conn = self._conn_pool.pop_connection( self._timeout )
            if conn:
                break
            #else:
            #    time.sleep( random.randint() )

        if not conn:
            logging.error('prediction no connection is available!')
            return self.on_error( conn, pred_req.req, rsp_que )

        req = pred_req.req
        num_req = req.count('\n')
        try:
            logging.debug('_prediction begin send all')
            conn.sendall( req )
            logging.debug('_prediction end send all')
        except:
            return self.on_error( conn, req , rsp_que)
        else:
            datas = []
            num_resp = 0
            logging.debug('_prediction begin recv all')
            while 1:
                logging.debug('_prediction recv')
                try:
                    data = conn.recv( 5120 )
                    if data:
                        #print data
                        datas.append( data )
                        num_resp +=  data.count('\n')
                        if num_resp == num_req:
                            break
                    else:
                        self.on_error( conn, req, rsp_que )
                except socket.timeout:
                    logging.warn('prediction with network timeout for req(size=%d) %s...', len(req ), req[:-1][:LOG_REQ_SIZE])
                    return self.on_error( conn, req, rsp_que )
                except socket.error,e:
                    logging.warn('prediction with network error %s for req(size=%d) %s...', e, len(req), req[:-1][:LOG_REQ_SIZE])
                    return self.on_error( conn, req, rsp_que )
            logging.debug('_prediction end recv all')
            resp_str = ''.join(datas)
            resp_list = resp_str.split('\n')
            try:
                resps = [ float(resp.strip().split()[0]) for resp in resp_list if resp.strip()]
            except:
                logging.error('parse resps failed %s...', resp_str[:-1][:LOG_REQ_SIZE])
                return resp_que.put(None)

            return rsp_que.put( resps )        

    def on_error(self, conn,  req , rsp_que ):
        if conn:
            conn.close()
        rsp_que.put( None )
        return None

if __name__ == '__main__':
    pred = prediction.prediction_t([('10.4.16.44', 12013),], 1, thread_num = 10, timeout = 5,  )
    if 0 == pred.build_connection():
        print >>sys.stderr, "no connection is made"
        sys.exit(1)
    pred_req1 = ' |u 1 |s 2 |r a bs\n'
    pred_req2 = ' |u 2 |s 3 |r a c\n'
    reqs = [pred_req1, pred_req2]
    rst = pred.predict( ''.join(reqs), 10 )
    print rst
