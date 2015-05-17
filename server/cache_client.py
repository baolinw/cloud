# This file implements the API called by the file system
# It will hide the APIS from simple_cliet_for_test.py, the handles the calls from file system
# if something is missing, it will call the functions in simple_cliet_for_test.py 's functions
import simple_client_for_test
from threading import Lock # for updating 
import config
import os
import os.path

CACHE_FILES = None;
lock = Lock()
CACHE_CHUNK_INFO = {}
ROOT_DIR = simple_client_for_test.CLIENT_ROOT_DIR
CURRENT_OPEN_FILES = {} # map file name to R/W

FILE_READ_TRANSACTION = {} # file_name to transaction id
FILE_WRITE_TRANSACTION = {} # file_name to transaction id

def Mount():
	list_all_files()
	synchronize()
	
# decorator of the Lock
def lock_dec(func):
	def inner_function(*args,**kwargs):
		lock.acquire()
		result = func(*args,**kwargs)
		lock.release()
		return result
	return inner_function

def file_exists_local(file_name):
	global CACHE_FILES
	file_names = file_name.split('/');
	if len(file_names[0]) == 0:
		file_names = file_names[1:]
	current = CACHE_FILES
	for folder_name in file_names[0:-1]:
		if current['dirs'].has_key(folder_name):
			current = current['dirs'][folder_name]
		else:
			return False
	return current['files'].has_key(file_names[-1])

def list_all_files(force_update = False):
	global CACHE_FILES
	if force_update:
		CACHE_FILES = None
	if CACHE_FILES == None:
		CACHE_FILES = simple_client_for_test.cache_list_all_files()
	return CACHE_FILES		

def get_chunks_info(file_name):
	global CACHE_CHUNK_INFO
	file_name = config.name_local_to_remote(file_name)
	if CACHE_CHUNK_INFO.has_key(file_name) == False:
		CACHE_CHUNK_INFO[file_name] = simple_client_for_test.cache_get_chunks_info(file_name)
	#print CACHE_CHUNK_INFO[file_name]
	return CACHE_CHUNK_INFO[file_name]

def sync_download_file(file_name):
	global CURRENT_OPEN_FILES,CACHE_CHUNK_INFO
	if CACHE_CHUNK_INFO.has_key(file_name):
		del CACHE_CHUNK_INFO[file_name]
	# sync the file only when it is not opened
	if CURRENT_OPEN_FILES.has_key(file_name):
		print 'WARN: Sync download when file is opened'
		return
	# there is no optimization now! TODO: read only dirty
	server_file_name = config.name_local_to_remote(file_name)
	chunks_info = get_chunks_info(file_name)
	# chunk_info format: ['file_size'], chunk_id:[server_indexes,]
	chunks_id = []
	##print chunks_info
	for k in chunks_info.keys():
		if k == 'file_size':
			continue
		chunks_id.append(k)
	chunks_id = list(set(chunks_id))
	chunks_id.sort()
	# there should be no holes, TODO, conceal the holes to save spaces
	assert(chunks_id == range(len(chunks_id)))
	# open the file 
	true_file_name = ROOT_DIR + file_name
	#print true_file_name
	f = open(true_file_name,'w')
	content = simple_client_for_test.cache_read_file(config.name_local_to_remote(file_name), 0, len(chunks_id) * config.FILE_CHUNK_SIZE)
	#print content,len(content)
	f.write(content);
	f.close()

def sync_upload_file(file_name):
	global CACHE_CHUNK_INFO
	true_local_file_name = ROOT_DIR + file_name
	true_server_file_name = config.name_local_to_remote(file_name)
	
	# there is no optimization now! TODO: read only dirty
	server_file_name = config.name_local_to_remote(file_name)
	chunks_info = get_chunks_info(file_name)
	file_size = chunks_info['file_size']
	
	true_file_name = ROOT_DIR + file_name
	f = open(true_file_name,'r')
	content = simple_client_for_test.cache_write_file(config.name_local_to_remote(file_name), 0, f.read())
	f.close()
	if CACHE_CHUNK_INFO.has_key(file_name):
		del CACHE_CHUNK_INFO[file_name]
	
@lock_dec
def create_file(file_name):
	if os.path.exists(ROOT_DIR + file_name):
		raise file_name + ' Already Exist!'
	if file_exists_local(file_name):
		raise file_name + ' Already Exist in Cache, you may try later'
	# create the file in local
	file_names = file_name.split('/');
	if len(file_names[0]) == 0:
		file_names = file_names[1:]
	current = ROOT_DIR
	for folder_name in file_names[0:-1]:
		try:
			os.mkdir(current + folder_name)
		except:
			pass
		current = current + folder_name + '/'
	f = open(current + file_names[-1],'w')
	f.write('0' * config.FILE_CHUNK_SIZE)
	f.close()
	simple_client_for_test.cache_create_file(file_name)
	sync_upload_file(file_name)
	
def open_file(file_name,mode):
	global CURRENT_OPEN_FILES
	# force a download_file
	if 'w' in mode:
		#simple_cliet_for_test.cache_del_file(config.name_local_to_remote(file_name))
		#create_file(file_name)
		CURRENT_OPEN_FILES[file_name] = 'W'
		return open(ROOT_DIR + file_name, 'w')
	# read mode, I thought
	sync_download_file(file_name)
	true_file_name = ROOT_DIR + file_name
	CURRENT_OPEN_FILES[file_name] = 'R'
	f = open(true_file_name,mode)
	return f

def del_file(file_name):
	true_name = ROOT_DIR + file_name
	remote = config.name_local_to_remote(file_name)
	try:
		simple_client_for_test.cache_del_file(remote)
	except:
		pass
	list_all_files(True)
	os.remove(true_name)	
	
@lock_dec
def close_file(f,file_name):
	global CACHE_CHUNK_INFO, CURRENT_OPEN_FILES
	f.close()
	if CURRENT_OPEN_FILES.has_key(file_name):
		del CURRENT_OPEN_FILES[file_name]
	if CACHE_CHUNK_INFO.has_key(file_name):
		del CACHE_CHUNK_INFO[file_name]
	sync_upload_file(file_name)
	
@lock_dec
def read_file(f, file_name, start, size):
	f.seek(start)
	return f.read(size)

@lock_dec
def write_file(f, file_name, start, str_to_write):
	f.seek(start)
	return f.write(str_to_write)
	
# do the synchronization, upload the files that has been changed or added
# (CURRENTLY NOT DO, upload/download will only be done in the close function
# Although it is not ideal method, it will work for current implementation.
# conditional uploading will be implemented in the near future
# the download is done only when 'open' calls
# TODO, update only changed
@lock_dec
def synchronize():
	global FILE_READ_TRANSACTION,FILE_WRITE_TRANSACTION
	# update the CACHES
	global CACHE_CHUNK_INFO, CACHE_FILES
	# get all the files starting from ROOT_DIR
	# CACHE_CHUNK_INFO not updated
	tmp = simple_client_for_test.raw_cache_list_all_files()
	# for those not in the folder now, create a fake file
	for (filename,size) in tmp:
		### print (filename,size)
		cur_dir = ROOT_DIR
		filename = filename.split('/')
		if len(filename[0]) == 0:
			filename = filename[1:]
		for folder_name in filename[0:-1]:
			try:
				os.mkdir(cur_dir + folder_name)
			except:
				pass
			cur_dir = cur_dir + folder_name + '/'
		# test whether this file exist
		if os.path.exists(cur_dir + filename[-1]):
			continue
		# currently for simplicity, downloading it		
		sync_download_file(cur_dir[len(ROOT_DIR):] + filename[-1])
		# create a fake file
		###f = open(cur_dir+filename[-1],'w')
		###f.close()			
	
	# update the CACHE_CHUNK_INFO
	list_all_files(True)	

	
if __name__ == "__main__":
	# test 0 ----------------, create a file, write to it, sync, read, check
	Mount()
	try:
		del_file('wubaolin')		
	except Exception as e:
		print '211',e
		pass
	try:
		del_file('wubaolin2')
	except Exception as e:
		print '216',e
		pass
	create_file('wubaolin')
	create_file('wubaolin2')
	f1 = open_file('wubaolin','r+');
	f2 = open_file('wubaolin2','r+');
	write_file(f1,'wubaolin', 0, 'HAHA1st');
	write_file(f2,'wubaolin2',1020,'HAHAHAHA2nd');
	close_file(f1,'wubaolin');
	close_file(f2,'wubaolin2');
	# a = 1/0
	# delete the files
	os.remove(ROOT_DIR + 'wubaolin')
	os.remove(ROOT_DIR + 'wubaolin2')
	synchronize()

	assert(os.path.exists(ROOT_DIR + 'wubaolin') == True)
	assert(os.path.exists(ROOT_DIR + 'wubaolin2') == True)
	
	f1 = open_file('wubaolin','r')
	f2 = open_file('wubaolin2','r')
	assert read_file(f1,'wubaolin',0,7) == 'HAHA1st'
	assert read_file(f2,'wubaolin2',1020,len('HAHAHAHA2nd')) == 'HAHAHAHA2nd'
	close_file(f1,'wubaolin')
	close_file(f2,'wubaolin2')
	
	del_file('wubaolin')
	
	assert(os.path.exists(ROOT_DIR + 'wubaolin') == False)
	
	os.remove(ROOT_DIR + 'wubaolin2')
	synchronize()
	
	assert(os.path.exists(ROOT_DIR + 'wubaolin') == False)
	assert(os.path.exists(ROOT_DIR + 'wubaolin2') == True)
	
	f2 = open_file('wubaolin2','r')
	assert read_file(f2,'wubaolin2',1020,len('HAHAHAHA2nd')) == 'HAHAHAHA2nd'
	
	assert read_file(f2,'wubaolin2',0,10) == '0' * 10
	print 'Basic Cache Client Passed!'
	
	# test1 -----------------, download/modify/upload/download/check
	# at first download the file of tutu2.txt
	'''
	sync_download_file('tutu2.txt')
	ftmp = open(ROOT_DIR + 'tutu2.txt','r')
	assert(read_file(ftmp,'tutu2.txt',1020,8) == 'Z'*8)
	ftmp.close()
	# modify the local file
	ftmp = open(ROOT_DIR + 'tutu2.txt','r+');
	write_file(ftmp,'tutu2.txt', 1020, '2222222')
	ftmp.close()
	# get it again
	sync_upload_file('tutu2.txt')
	sync_download_file('tutu2.txt')
	ftmp = open(ROOT_DIR + 'tutu2.txt','r')
	assert(read_file(ftmp,'tutu2.txt',1020,7) == '2'*7)
	ftmp.close()
	'''

