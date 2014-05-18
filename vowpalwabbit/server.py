import socket
import sys
s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.bind(('127.0.0.1',9999))
#90280Bytes
s.listen(5)
while 1:
	cs,ca = s.accept()
	print >>sys.stderr, 'accept ok'
	while 1:
		readed = 0
		datas = []
		while readed <90280:
			data  = cs.recv(90280-readed)
			readed += len(data)	
			datas.append(data)

		data = ''.join(datas)
		chunk = 10
		idx = 0
		while idx < 10000:
			#print >>sys.stderr, 'send', idx
			sent = cs.send(data[idx:idx+chunk])
			idx += sent
			#print >>sys.stderr, 'sent', idx
		
