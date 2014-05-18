import socket
import sys
cs = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
cs.connect(('127.0.0.1',9999))
gdata = '1'*90280
import time
t = 0
i = 0
while i < 1000:
	cs.sendall(gdata)
	readed = 0
	t1 = time.time()
	while readed < 10000:
		data=cs.recv(1024)
		readed += len(data)	
	t2 = time.time()
	print >>sys.stderr, 'read cost',t2-t1
	t+= t2-t1
	i += 1

	
