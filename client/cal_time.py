import sys
import os
import time
os.chdir('/tmp/cfs')

f = open('/tmp/cfs/abc','w')
f.write('a' * 5000000)
start = time.time()
f.close()
os.listdir('/tmp/cfs')
end = time.time()

print 'the time is ' , end - start , ' s'

os.system('rm /tmp/cfs/abc')




