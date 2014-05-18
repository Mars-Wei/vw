import zlib
import time
s = open('vm_message.txt').read()
t1 = time.time()
count = 1000
for i in range(count):
    zlib.compress(s,3)

t2 = time.time()
print 'cost', (t2-t1)/count*1000,'ms'
