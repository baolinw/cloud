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

# decorator of the Lock
def lock_dec(func):
	def inner_function(*args,**kwargs):
		lock.acquire()
		result = func(*args,**kwargs)
		lock.release()
		return result
	return inner_function

def file_exists_local(file_name):
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

@lock_dec
def list_all_files():
	global CACHE_FILES
	if CACHE_FILES == None:
		CACHE_FILES = simple_client_for_test.cache_list_all_files()
	return CACHE_FILES		

@lock_dec	
def get_chunks_info(file_name):
	global CACHE_CHUNK_INFO
	file_name = config.name_local_to_remote(file_name)
	if CACHE_CHUNK_INFO.has_key(file_name) == False:
		CACHE_CHUNK_INFO[file_name] = simple_client_for_test.cache_get_chunks_info(file_name)
	#print CACHE_CHUNK_INFO[file_name]
	return CACHE_CHUNK_INFO[file_name]

def sync_download_file(file_name):
	global CURRENT_OPEN_FILES
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
	print true_file_name
	f = open(true_file_name,'w')
	content = simple_client_for_test.cache_read_file(config.name_local_to_remote(file_name), 0, len(chunks_id) * config.FILE_CHUNK_SIZE)
	#print content,len(content)
	f.write(content);
	f.close()

def sync_upload_file(file_name):
	true_local_file_name = ROOT_DIR + file_name
	true_server_file_name = config.name_local_to_remote(file_name)
	pass
	
	
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
	f = open(current + file_name[-1],'w')
	f.write('0' * config.FILE_CHUNK_SIZE)
	f.close()
	
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
	
@lock_dec
def close_file(f,file_name):
	f.close()
	sync_upload_file(file_name)

@lock_dec
def read_file(f, file_name, start, size):
	f.seek(start)
	return f.read(size)

@lock_dec
def write_file(f, file_name, start, str_to_write):
	f.seek(start)
	return f.write(str_to_write)
	
# test1 -----------------, download/modify/upload/download/check
# at first download the file of tutu2.txt
sync_download_file('tutu2.txt')
ftmp = open(ROOT_DIR + 'tutu2.txt','r')
assert(read_file(ftmp,'tutu2.txt',1020,8) == 'Z'*8)
ftmp.close()
# modify the local file
ftmp = open(ROOT_DIR + 'tutu2.txt','w');
write_file(ftmp,'tutu2.txt', 1020, '22222222')
ftmp.close()
# get it again
sync_upload_file('tutu2.txt')
sync_download_file('tutu2.txt')
ftmp = open(ROOT_DIR + 'tutu2.txt','r')
assert(read_file(ftmp,'tutu2.txt',1020,8) == '2'*8)
ftmp.close()

	
