# this is to test the system in the upper most level,
import os
import sys
import os.path

RED = '\033[91m' # red
END = '\033[0m' # normal
GRE = '\033[1;32;40m' # green

# Umount the file system
#os.system('fusermount -u /tmp/cfs')
# Mount the file system
#os.system('./main -s /tmp/cfs')

os.chdir('/tmp/cfs')
# delete all files
os.system('rm -rf *')
the_files = []
for line in os.listdir('/tmp/cfs'):
	the_files.append(line)	
assert len(the_files) == 0
print GRE,'Test 1: delete all files and folders passed!',RED


# create some files and test 
f = open('1.txt','w')
f.write('W' * 12)
f.close()
f = open('2.txt','w')
f.write('B' * 1024)
f.close()
f = open('3.txt','w')
f.write('C' * 512000)
f.close()
f = open('3.txt','a')
f.write('T' * 512000)
f.close()
f = open('4.txt','w')
f.write('Z' * 2048000)
f.close()

f1 = open('1.txt','r')
a = f1.read()
assert all([a[i] == 'W' for i in range(12)])
assert len(a) == 12
f1.close()
f2 = open('2.txt','r')
a = f2.read()
assert all([a[i] == 'B' for i in range(1024)])
assert len(a) == 1024
f2.close()
f3 = open('3.txt','r')
a = f3.read()
assert all([a[i] == 'C' for i in range(512000)])
assert len(a) == 512000 * 2
assert all([a[i] == 'T' for i in range(512000,512000*2)])
f3.close()
f4 = open('4.txt','r')
a = f4.read()
assert all([a[i] == 'Z' for i in range(2048000)])
assert len(a) == 2048000
f4.close()
print GRE,'Test 2: create file and read its contents passed',RED

os.system("rm 1.txt 2.txt 3.txt 4.txt")
the_files = []
for line in os.listdir('/tmp/cfs'):
	the_files.append(line)	
assert len(the_files) == 0
print GRE,'Test 3: delete the file just created success',RED

try:
	os.mkdir('folder1')
	os.mkdir('folder2')
	os.mkdir('folder3')
	os.mkdir('folder4')
except Exception as e:
	print RED,'Test4: Create Folder failed!',e,RED
	sys.exit(1)
for i in range(1,5):
	assert(os.path.isdir('/tmp/cfs/folder' + str(i)) == True)
print GRE,'Test 4: Create many single-layer folders passed!',RED

os.rmdir('folder1'),os.rmdir('folder2'),os.rmdir('folder3')
assert(os.path.isdir('/tmp/cfs/folder4') == True)
for i in range(1,4):
	assert os.path.isdir('/tmp/cfs/folder' + str(i)) == False
print GRE,'Test 5: Del single-layer folders passed!',RED

failed = False
try:
	os.mkdir('folder4')
except:
	failed = True
assert failed == True
print GRE,'Test 6: duplicate mkdir should fail passed!'
# only folder4 folder1/folder2/folder3/folder4/ sfolder1/sfolder2/.../sfolder4 exists
TEST_FOLDER = 5
s = 'folder1'
for i in range(2,TEST_FOLDER):
	os.mkdir(s)
	s = s + '/folder' + str(i)
os.mkdir(s)
s = '/tmp/cfs/folder1'
for i in range(2,TEST_FOLDER):
	assert os.path.isdir(s) == True
	s = s + '/folder' + str(i)
os.system('mkdir -p ' + '/'.join(['sfolder' + str(i) for i in range(1,TEST_FOLDER)]))
s = '/tmp/cfs/sfolder1'
for i in range(2,TEST_FOLDER):
	assert os.path.isdir(s) == True
	s = s + '/sfolder' + str(i)
assert os.path.isdir(s) == True
print GRE,'Test 7: multilevel create folder success',RED 

for last in range(TEST_FOLDER,2,-1):
	s = '/tmp/cfs/' + '/'.join(['folder' + str(i) for i in range(1,last)])
	os.rmdir(s)
	assert os.path.isdir(s) == False
os.rmdir('folder1')
for last in range(TEST_FOLDER,2,-1):
	s = '/tmp/cfs/' + '/'.join(['sfolder' + str(i) for i in range(1,last)])
	os.rmdir(s)
	assert os.path.isdir(s) == False
os.rmdir('sfolder1')
assert os.path.isdir('/tmp/cfs/folder4') == True
os.rmdir('/tmp/cfs/folder4')
assert os.path.isdir('/tmp/cfs/folder4') == False
print GRE,'Test 8: multilevel remove folder success',RED

os.system('mkdir -p ' + '/tmp/cfs/' + '/'.join(['folder' + str(i) for i in range(1,TEST_FOLDER)]))
SIZE_TEST_9 = 1024

for i in range(1,TEST_FOLDER):
	s = '/' + 'file' + str(i)
	f = open('/tmp/cfs/' + '/'.join(['folder' + str(i) for i in range(1,i+1)]) + s,'w')
	f.write(str(i) * SIZE_TEST_9)
	f.close()
for i in range(1,TEST_FOLDER):
	s = '/' + 'file' + str(i)
	f = open('/tmp/cfs/' + '/'.join(['folder' + str(i) for i in range(1,i+1)]) + s,'r')
	a = f.read()
	assert all([a[w] == str(i) for w in range(SIZE_TEST_9)])
	assert len(a) == SIZE_TEST_9
	f.close()
	
print GRE,'Test 9: read/write files under foders passed!',RED 

for i in range(TEST_FOLDER-1,0,-1):
	s = 'file' + str(i)
	folder_name = '/tmp/cfs/' + '/'.join(['folder' + str(i) for i in range(1,i+1)])
	#print 'rmdir::', folder_name
	file_name = folder_name + '/' + s
	os.unlink(file_name)
	assert os.path.exists(file_name) == False
	f = open(file_name,'w')
	f.write('Q' * 1024)
	f.close()
	assert os.path.exists(file_name) == True
	f1 = open(file_name,'r')
	ww = f1.read()
	assert all([ww[i] == 'Q' for i in range(1024)])
	assert len(ww) == 1024
	f1.close()
	os.unlink(file_name)
	assert os.path.exists(file_name) == False
	os.rmdir(folder_name)
	assert os.path.isdir(folder_name) == False

the_files = []
for line in os.listdir('/tmp/cfs'):
	the_files.append(line)	
assert len(the_files) == 0	
print GRE,'Test 10: Del in rested folder, re create, then delete it, passed!',RED

for i in [0,1,2,3,4,1024]:
	f = open(str(i) + '.test','w')
	f.write('C' * i)
	f.close()
	f = open(str(i) + '.test','r')
	r = f.read()
	assert all([r[mm] == 'C' for mm in range(i)])
	assert len(r) == i
	f.close()
	os.system('rm ' + str(i) + '.test')
the_files = []
for line in os.listdir('/tmp/cfs'):
	the_files.append(line)	
assert len(the_files) == 0	
print GRE,'Test 11: small file optimizationed size passed!',RED
 
for i in [589000, 1589000]:
	f = open(str(i) + '.test','w')
	f.write('C' * i)
	f.close()
	f = open(str(i) + '.test','r')
	r = f.read()
	assert all([r[mm] == 'C' for mm in range(i)])
	assert len(r) == i
	f.close()
	os.system('rm ' + str(i) + '.test')
the_files = []
for line in os.listdir('/tmp/cfs'):
	the_files.append(line)	
assert len(the_files) == 0	
print GRE,'Test 12: large file file size and contents passed!',RED

for i in range(1,10):
	f = open('freq.test','w')
	f.write('Z' * i)
	f.close()
	f = open('freq.test','r')
	r = f.read()
	assert all([r[mm] == 'Z' for mm in range(i)])
	assert len(r) == i
	f.close()
sum = 0 
open('freq2.test','w').close()
for i in range(1,20):
	f = open('freq2.test','a')
	f.write('Z' * i)
	sum += i
	f.close()
	f = open('freq2.test','r')
	r = f.read()
	assert all([r[mm] == 'Z' for mm in range(sum)])
	assert len(r) == sum
	f.close()
os.system('rm freq.test')
os.system('rm freq2.test')
the_files = []
for line in os.listdir('/tmp/cfs'):
	the_files.append(line)	
assert len(the_files) == 0	
print GRE,'Test 13: many times of write the same file passed!'

a = open('/tmp/to_be_copied','w')
a.write('C' * 1024000)
a.write('Z' * 2)
a.write('\n')
a.write('hehe')
a.close()
os.system('cp /tmp/to_be_copied /tmp/cfs/123321')
f1 = open('/tmp/cfs/123321','r')
f2 = open('/tmp/to_be_copied','r')
a1 = f1.read()
a2 = f2.read()
f1.close()
f2.close()
assert len(a1) == len(a2)
assert all([a1[i] == a2[i] for i in range(len(a1))])
os.system('rm 123321')
print GRE,'Test 14: copy text files test passed!'

source_file = '/home/ubuntu/cloud/client/test_binary.pptx'
os.system('cp ' + source_file + ' /tmp/cfs/test_binary')
f1 = open(source_file,'rb')
f2 = open('/tmp/cfs/test_binary','rb')
a1 = f1.read()
a2 = f2.read()
f1.close()
f2.close()
assert len(a1) == len(a2)
assert all([a1[i] == a2[i] for i in range(len(a1))])
os.system('rm test_binary')
print GRE,'Test 15: copy binary files test passed!'
print END