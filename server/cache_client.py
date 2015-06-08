# This file implements the API called by the file system
# It will hide the APIS from simple_cliet_for_test.py, the handles the calls from file system
# if something is missing, it will call the functions in simple_cliet_for_test.py 's functions
import simple_client_for_test
from threading import Lock # for updating 
import config
import os
import os.path
import simple_httpserver
import log
import sys
import traceback

CACHE_FILES = {};
lock = Lock()
CACHE_CHUNK_INFO = {}
ROOT_DIR = simple_client_for_test.CLIENT_ROOT_DIR
CURRENT_OPEN_FILES = {} # map file name to R/W
DIRTIES = {} # dirty flag
FILE_READ_TRANSACTION = {} # file_name to transaction id
FILE_WRITE_TRANSACTION = {} # file_name to transaction id

def get_root_dir():
	return ROOT_DIR

def Mount():
	try:
		list_all_files()
		synchronize()
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '
	print 'Hello'
	
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

def force_update():
	try:
		list_all_files(True)
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '
	
def list_all_files(force_update = False):
	try:
		global CACHE_FILES
		global CACHE_CHUNK_INFO
		#print '\033[1;32;40m list_all_files called ',CACHE_FILES,'\033[0m '	
		if force_update:
			CACHE_FILES = {}
			CACHE_CHUNK_INFO = {}
		if CACHE_FILES == {}:
			#print '\033[1;32;40m list_all_files before \033[0m '	
			CACHE_FILES = simple_client_for_test.cache_list_all_files()		
		#print '\033[1;32;40m list_all_files after \033[0m '	
		#print '\033[1;32;40m Get All files', CACHE_FILES,'\033[0m '
		return CACHE_FILES		
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '

def get_chunks_info(file_name,force_update = False):
	global CACHE_CHUNK_INFO
	try:
		file_name = config.name_local_to_remote(file_name)
		if force_update == True:
			CACHE_CHUNK_INFO = {}
		if CACHE_CHUNK_INFO.has_key(file_name) == False:
			CACHE_CHUNK_INFO[file_name] = simple_client_for_test.cache_get_chunks_info(file_name)
		#print CACHE_CHUNK_INFO[file_name]
		return CACHE_CHUNK_INFO[file_name]
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '

def sync_download_file(file_name):
	global CURRENT_OPEN_FILES,CACHE_CHUNK_INFO
	try:
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
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '

def sync_upload_file(file_name):
	global CACHE_CHUNK_INFO,DIRTIES
	try:
		true_local_file_name = ROOT_DIR + file_name
		true_server_file_name = config.name_local_to_remote(file_name)
		
		# there is no optimization now! TODO: read only dirty
		server_file_name = config.name_local_to_remote(file_name)
		chunks_info = get_chunks_info(file_name)
		file_size = chunks_info['file_size']
		true_file_name = ROOT_DIR + file_name
		print 'in sync_upload_file ', true_file_name
		f = open(true_file_name,'r')
		f_readed_content = f.read()
		print 'in sync_upload_file ', len(f_readed_content)
		print DIRTIES
		# loop over the file 
		chunk_ids = []
		contents = []
		for start_file in range(0,len(f_readed_content),config.FILE_CHUNK_SIZE):
			if DIRTIES.has_key(file_name) == False or (start_file//config.FILE_CHUNK_SIZE) in DIRTIES[file_name]:
				#print '#??',start_file,DIRTIES
				chunk_ids.append(start_file // config.FILE_CHUNK_SIZE)
				contents.extend(f_readed_content[start_file:min(start_file+config.FILE_CHUNK_SIZE, len(f_readed_content))])
		print 'sync up load ', chunk_ids, ' ', file_name
		if len(chunk_ids) > 0:		
			content = simple_client_for_test.cache_write_file_algined(config.name_local_to_remote(file_name), contents,chunk_ids)
			force_update()
		print '##',file_name,chunk_ids
		if DIRTIES.has_key(file_name):
			DIRTIES[file_name] = []
		f.close()
		if CACHE_CHUNK_INFO.has_key(file_name):
			del CACHE_CHUNK_INFO[file_name]
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '
	

def create_file(file_name,cpp_mode = 0):
	'''if os.path.exists(ROOT_DIR + file_name):
		if cpp_mode == 1:
			return -1
		raise file_name + ' Already Exist!'''
	try:
		if file_exists_local(file_name):
			if cpp_mode == 1:
				return -2
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
		f.write('00010000')
		f.close()
		simple_client_for_test.cache_create_file(file_name)
		force_update()
		#sync_upload_file(file_name)		
		return 0
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '
	
def open_file(file_name,mode,cpp_mode = 0):
	print 'open_file mode ', str(mode)
	try:
		global CURRENT_OPEN_FILES
		# force a download_file
		DIRTIES[file_name] = []
		if 'w' in mode:
			#simple_cliet_for_test.cache_del_file(config.name_local_to_remote(file_name))
			#create_file(file_name)
			CURRENT_OPEN_FILES[file_name] = 'W'		
			if cpp_mode == 1:
				return 0
			return open(ROOT_DIR + file_name, 'w')
		# read mode, I thought
		sync_download_file(file_name)
		true_file_name = ROOT_DIR + file_name
		CURRENT_OPEN_FILES[file_name] = 'R'
		if cpp_mode == 1:
			return 0
		f = open(true_file_name,mode)
		return f
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '

def del_file(file_name):
	try:
		true_name = ROOT_DIR + file_name
		remote = config.name_local_to_remote(file_name)
		try:
			simple_client_for_test.cache_del_file(remote)
		except:
			pass
		list_all_files(True)
		try:
			os.remove(true_name)
		except:
			pass
		return 0
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '
	

def close_file(f,file_name,cpp_mode=0):
	try:
		global CACHE_CHUNK_INFO, CURRENT_OPEN_FILES
		if cpp_mode == 0:
			f.close()
		if CURRENT_OPEN_FILES.has_key(file_name):
			del CURRENT_OPEN_FILES[file_name]
		if CACHE_CHUNK_INFO.has_key(file_name):
			del CACHE_CHUNK_INFO[file_name]
		#print 'close before upload'
		sync_upload_file(file_name)
		#print 'close after upload'
		return 0
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '	

def read_file(f, file_name, start, size = 0):
	try:
		f.seek(start)
		if size == 0:
			return f.read()
		return f.read(size)
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '

def make_dirty(file_name,start,size):
	try:
		start_block = start // config.FILE_CHUNK_SIZE
		end_block = (start + size -1) // config.FILE_CHUNK_SIZE	
		DIRTIES[file_name].extend(range(start_block,end_block+1))
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '	

def write_file(f, file_name, start, str_to_write):
	try:
		f.seek(start)
		
		start_block = start // config.FILE_CHUNK_SIZE
		num = f.write(str_to_write)
		end_block = (start + len(str_to_write)-1) // config.FILE_CHUNK_SIZE	
		DIRTIES[file_name].extend(range(start_block,end_block+1));
		return num
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '
	
# do the synchronization, upload the files that has been changed or added
# (CURRENTLY NOT DO, upload/download will only be done in the close function
# Although it is not ideal method, it will work for current implementation.
# conditional uploading will be implemented in the near future
# the download is done only when 'open' calls
# TODO, update only changed
@lock_dec
def synchronize():
	try:
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
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '

def get_local_size(file_name):
	try:
		dir = simple_client_for_test.CLIENT_ROOT_DIR + file_name
		size = os.stat(dir).st_size
		print '@@@@@@@@@@@@get_local_size ', file_name, size,dir	
		return size
	except Exception as e:
		print '\033[91m'
		print 'Uncaught Exception ',e
		traceback.print_exc()
		print '\033[0m '
	
import sys
	
OUTPUT_FILE = '/tmp/result'
	
def test():
	# Test ################################################################################
	Mount()
	Mount()
	#return
	try:
		os.remove(log.WRITE_LOG_FILE);
		os.remove('local_write_log');
		os.remove('server_write_log');
	except Exception as e:
		pass
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
	try:
		del_file('pp.txt')
	except Exception as e:
		print '216',e
		pass
	try:
		del_file('file_size1');
		del_file('hh1@.holder');
	except Exception as e:
		pass
	try:
		del_file('file_size2');
	except Exception as e:
		pass
	
	create_file("hh1@.holder")
	del_file("hh1@.holder")
	create_file("hh1@.holder")

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
	
	#assert read_file(f2,'wubaolin2',0,10) == '0' * 10
	
	print 'testing the pride-and-prejudice.......'
	create_file('pp.txt')
	fpp = open_file('pp.txt','r+')
	write_file(fpp,'pp.txt',0,open('/tmp/hehehe/pp.txt').read())
	close_file(fpp,'pp.txt')
	os.remove(ROOT_DIR + 'pp.txt')
	
	synchronize()
	fpp = open_file('pp.txt','r+')
	tmp = read_file(fpp,'pp.txt',0)
	tmp2 = open('/tmp/hehehe/pp.txt').read()
	assert(all([tmp[i] == tmp2[i] for i in range(len(tmp2))]))	
	close_file(fpp,'pp.txt')
	
	print 'write some place in the middle of p-and-p.txt..., should only update small number of segments'
	TEST_NUM = 1037
	START = 1012
	fpp = open_file('pp.txt','r+')
	write_file(fpp,'pp.txt', START, 'H' * TEST_NUM)
	close_file(fpp,'pp.txt');
	
	if(os.path.exists(ROOT_DIR + 'pp.txt')):
		os.remove(ROOT_DIR + 'pp.txt')
	synchronize();
	assert(os.path.exists(ROOT_DIR + 'pp.txt'))
	fpp = open_file('pp.txt','r+')
	tmp = read_file(fpp,'pp.txt',0)
	assert(all([tmp[START+i] == 'H' for i in range(TEST_NUM)]))
	close_file(fpp,'pp.txt')
	
	print '\033[1;32;40mBasic Cache Client Passed!\033[0m '
	
	FAIL_1 = 2
	print 'Make server ',str(FAIL_1), ' failed '
	simple_httpserver.handle_fail_server({'server_id':str(FAIL_1)})
	
	# try to get meta infos, they should be both appearing in 0 and 1,
	for file_name in list_all_files(True)['files']:
		chunks = get_chunks_info(file_name,True)		
		for id in chunks.keys():
			if id == 'file_size':
				continue
			#print chunks[id]
			ids = chunks[id]
			ids.sort()
			assert len(ids) == 2 and ids[0] != FAIL_1 and ids[1] != FAIL_1 and ids[0] != ids[1],str(ids) + ' ' + file_name
			
	print '\033[1;32;40mServer fail one Passed!\033[0m '
	
	FAIL_2 = 0
	print 'Make server ',str(FAIL_2),' failed!'
	simple_httpserver.handle_fail_server({'server_id':str(FAIL_2)})
	
	# try to get meta infos,
	for file_name in list_all_files(True)['files']:
		chunks = get_chunks_info(file_name,True)		
		for id in chunks.keys():
			if id == 'file_size':
				continue
			#print chunks[id]
			ids = chunks[id]
			ids.sort()
			assert len(ids) == 1 and ids[0] != FAIL_1 and ids[0] != FAIL_2
			
	# re write the whole file, so that after resuming, it will also work now
	fpp = open_file('pp.txt','r+')
	write_file(fpp,'pp.txt', 0, 'M' * 5)
	close_file(fpp,'pp.txt');
			
	print '\033[1;32;40mServer fail two Passed!\033[0m '
	
	print 'make server ', str(FAIL_2), ' ok'
	simple_httpserver.handle_ok_server({'server_id':str(FAIL_2)})
	
	# try to get meta infos, they should be both appearing in 0 and 1,
	for file_name in list_all_files(True)['files']:
		chunks = get_chunks_info(file_name,True)		
		for id in chunks.keys():
			if id == 'file_size':
				continue
			#print chunks[id]
			ids = chunks[id]
			ids.sort()
			assert len(ids) >= 2 and ids[0] != FAIL_1 and ids[1] != FAIL_1
			
	print 'reread the file modified between these change!'
	fpp = open_file('pp.txt','r+')
	wc = read_file(fpp,'pp.txt', 0, 6)
	assert all([wc[i] == 'M' for i in range(len(wc)-1)])
	assert wc[-1] != 'M'
	close_file(fpp,'pp.txt');
	print '\033[1;32;40mServer write between one resumes Passed!\033[0m '
	print '\033[1;32;40mServer fail two Passed!\033[0m '
	
	print 'make server ', str(FAIL_2), ' fail agin'
	simple_httpserver.handle_fail_server({'server_id':str(FAIL_2)})
	
	# try to get meta infos,
	for file_name in list_all_files(True)['files']:
		chunks = get_chunks_info(file_name,True)		
		for id in chunks.keys():
			if id == 'file_size':
				continue
			#print chunks[id]
			ids = chunks[id]
			ids.sort()
			assert len(ids) == 1 and ids[0] != FAIL_1 and ids[0] != FAIL_2
			
	print '\033[1;32;40mServer fail Second time Passed!\033[0m '
	
	print 'make server ', str(FAIL_1), 'ok'
	print 'make server ', str(FAIL_2), 'ok'
	simple_httpserver.handle_ok_server({'server_id':str(FAIL_1)})
	simple_httpserver.handle_ok_server({'server_id':str(FAIL_2)})
	
	for file_name in list_all_files(True)['files']:
		chunks = get_chunks_info(file_name,True)		
		for id in chunks.keys():
			if id == 'file_size':
				continue
			#print str(id),'%',file_name,'%',chunks[id]
			ids = chunks[id]
			ids.sort()
			assert len(ids) >= 2
			
	print '\033[1;32;40mServer bring up again Passed!\033[0m '	
	
	print 'Next test is for single step fails -----------------'
	print '1st test case, write without update, no commit'
	simple_client_for_test.WRITE_FAIL_MODE = 1
	fpp = open_file('pp.txt','r+')
	write_file(fpp,'pp.txt', 1023, 'C' * 5)
	close_file(fpp,'pp.txt');
	
	fpp = open_file('pp.txt','r+')
	wc = read_file(fpp,'pp.txt', 1023, 5)
	assert all([wc[i] != 'C' for i in range(len(wc))])
	close_file(fpp,'pp.txt');
	print '\033[1;32;40m FT single test 1 Passed!\033[0m '	
	
	print '2nd test case, write partial, no commit'
	simple_client_for_test.WRITE_FAIL_MODE = 2
	fpp = open_file('pp.txt','r+')
	write_file(fpp,'pp.txt', 1023, 'E' * 5)
	close_file(fpp,'pp.txt');
	
	fpp = open_file('pp.txt','r+')
	wc = read_file(fpp,'pp.txt', 1023, 5)
	assert all([wc[i] != 'C' for i in range(len(wc))])
	close_file(fpp,'pp.txt');	
	
	print '\033[1;32;40m FT single test 2 Passed!\033[0m '	
	
	simple_client_for_test.WRITE_FAIL_MODE = 0
	simple_client_for_test.redo_logs()	
	fpp = open_file('pp.txt','r+')
	wc = read_file(fpp,'pp.txt',1023,5)
	assert all([wc[i] == 'E' for i in range(len(wc))])
	close_file(fpp,'pp.txt');
	print '\033[1;32;40m FT single test 3 Passed!\033[0m '	
	
	print '4th test case, write all , commit, server fails at the begining of writing'
	simple_httpserver.handle_ft_mode({'mode':str(1)})
	fpp = open_file('pp.txt','r+')
	write_file(fpp,'pp.txt', 2040, 'W' * 5)
	close_file(fpp,'pp.txt');
	fpp = open_file('pp.txt','r+')
	wc = read_file(fpp,'pp.txt', 2040, 5)
	assert all([wc[i] != 'W' for i in range(len(wc))])
	close_file(fpp,'pp.txt');	
	
	print '\033[1;32;40m FT single test 4 Passed!\033[0m '	
	
	print '5th test case, write all , commit, server fails after one renaming'
	simple_httpserver.handle_ft_mode({'mode':str(2)})
	fpp = open_file('pp.txt','r+')
	write_file(fpp,'pp.txt', 1010, 'Z' * 5)
	close_file(fpp,'pp.txt');
	# resume the server
	simple_httpserver.handle_ft_mode({'mode':str(0)})
	simple_httpserver.handle_resume({})
	
	'''fpp = open_file('pp.txt','r+')
	wc = read_file(fpp,'pp.txt', 1010, 5)
	assert all([wc[i] == 'Z' for i in range(len(wc))])
	wc = read_file(fpp,'pp.txt', 2040, 5)
	assert all([wc[i] == 'W' for i in range(len(wc))])
	close_file(fpp,'pp.txt');	'''
	
	print '\033[1;32;40m FT single test 5 Passed!\033[0m '	
	
	
	print 'Test for file size'	
	create_file('file_size1'); 
	f = open_file('file_size1','r')
	close_file(f,'file_size1')
	
	create_file('file_size2'); 
	f = open_file('file_size2','r')
	close_file(f,'file_size2')
	
	assert get_local_size('file_size1') == 0
	
	for size in range(102,12345,1024):
		f = open_file('file_size1','w')
		write_file(f,'file_size1',0,'W' * size)
		close_file(f,'file_size1')
		force_update()
		assert get_local_size('file_size1') == size
	
	former = 0
	for size in range(102,12345,1024):
		f = open_file('file_size2','r+')
		write_file(f,'file_size2',former,'W' * size)
		close_file(f,'file_size2')
		print size
		assert get_local_size('file_size2') == size + former
		former = size + former
		
	print '\033[1;32;40m File Size Test 1 Passed!\033[0m '	
	
	
	print 'Some corner case for read/write-----------'
	# create empty 
	
	
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

if __name__ == "__main__":
	# test 0 ----------------, create a file, write to it, sync, read, check, Only 3 servers are allowed!!!!!!!!
	print sys.argv
	# I use this file as the core function to be called by the File System, Write as little code in C++ as possible
	if len(sys.argv) == 1:
		test()
		#Mount()
		#create_file('momoda')
		sys.exit(0)