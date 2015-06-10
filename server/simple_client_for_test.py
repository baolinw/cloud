# This file is mainly for test, before implementation the real, I will implement
# all the logic here because python development is more efficient
import os
import google_api
import google_api.lib_google
import google_api.lib_google2
import google_api.lib_google3
import dropbox_api
import dropbox_api.lib_dropbox
import local_api
import local_api.lib_local
import config
import simple_httpserver
import meta_puller
import traceback
import time
from config import name_local_to_remote;
from config import name_remote_to_local;
# I need the service object to do actual upload/download
SERVERS = []
WRITE_FAIL_MODE = 0 # 0 normal, 1: no update no commit and no error recovery, only hold, 2: partial update without commit, 
# we don't do anything relating to CREATE fail, we don't have time to do it, just use the write to illustrate it
SERVERS.append( { \
		'id':0, \
		'live':1, \
		'name' : 'Google', \
		'server_object' : google_api.lib_google.create_service_object(0), \
		'get_all_file_names' : google_api.lib_google.get_all_file_names, \
		'download_file' : google_api.lib_google.download_file, \
		'delete_file' :  google_api.lib_google.delete_file, \
		'upload_file' : google_api.lib_google.upload_file,\
		'copy_file' : google_api.lib_google.copy_file, \
	})
SERVERS = []
SERVERS.append( { \
		'id':0, \
		'live':1, \
		'name' : 'Local', \
		'server_object' : local_api.lib_local.create_service_object('wbl'), \
		'get_all_file_names' : local_api.lib_local.get_all_file_names, \
		'download_file' : local_api.lib_local.download_file, \
		'delete_file' :  local_api.lib_local.delete_file, \
		'upload_file' : local_api.lib_local.upload_file
	})
SERVERS.append( { \
		'id':1, \
		'live':1, \
		'name' : 'Local', \
		'server_object' : local_api.lib_local.create_service_object('wbl2'), \
		'get_all_file_names' : local_api.lib_local.get_all_file_names, \
		'download_file' : local_api.lib_local.download_file, \
		'delete_file' :  local_api.lib_local.delete_file, \
		'upload_file' : local_api.lib_local.upload_file
	})
SERVERS.append( { \
		'id':2, \
		'live':1, \
		'name' : 'Local', \
		'server_object' : local_api.lib_local.create_service_object('wbl3'), \
		'get_all_file_names' : local_api.lib_local.get_all_file_names, \
		'download_file' : local_api.lib_local.download_file, \
		'delete_file' :  local_api.lib_local.delete_file, \
		'upload_file' : local_api.lib_local.upload_file
	}) 
'''
SERVERS.append( { \
		'id':0, \
		'live':1, \
		'name' : 'Local', \
		'server_object' : local_api.lib_local.create_service_object('wbl'), \
		'get_all_file_names' : local_api.lib_local.get_all_file_names, \
		'download_file' : local_api.lib_local.download_file, \
		'delete_file' :  local_api.lib_local.delete_file, \
		'upload_file' : local_api.lib_local.upload_file
	})
SERVERS.append( { \
		'id':1, \
		'live':1, \
		'name' : 'Local', \
		'server_object' : local_api.lib_local.create_service_object('wbl2'), \
		'get_all_file_names' : local_api.lib_local.get_all_file_names, \
		'download_file' : local_api.lib_local.download_file, \
		'delete_file' :  local_api.lib_local.delete_file, \
		'upload_file' : local_api.lib_local.upload_file
	})
SERVERS.append( { \
		'id':2, \
		'live':1, \
		'name' : 'Local', \
		'server_object' : local_api.lib_local.create_service_object('wbl3'), \
		'get_all_file_names' : local_api.lib_local.get_all_file_names, \
		'download_file' : local_api.lib_local.download_file, \
		'delete_file' :  local_api.lib_local.delete_file, \
		'upload_file' : local_api.lib_local.upload_file
	}) '''
'''
SERVERS.append( { \
	'id':1, \
	'name' : 'Google', \
	'server_object' : google_api.lib_google.create_service_object(), \
	'get_all_file_names' : google_api.lib_google.get_all_file_names, \
	'download_file' : google_api.lib_google.download_file, \
	'delete_file' :  google_api.lib_google.delete_file, \
	'upload_file' : google_api.lib_google.upload_file
})
SERVERS.append( { \
	'id':2, \
	'name' : 'DropBox1', \
	'server_object' : dropbox_api.lib_dropbox.create_service_object(), \
	'get_all_file_names' : dropbox_api.lib_dropbox.get_all_file_names, \
	'download_file' : dropbox_api.lib_dropbox.download_file, \
	'delete_file' :  dropbox_api.lib_dropbox.delete_file, \
	'upload_file' : dropbox_api.lib_dropbox.upload_file
})''';

meta_puller.init_config()
meta_puller.pull_meta()

CHUNK_SIZE = config.FILE_CHUNK_SIZE
# return [(file_name,file_size), (file_name, file_size)]
# get the following structure
# { .dirs = {dir_name: SIMILAR structure } }, .files = {file_name:size } }
def cache_list_all_files():
	param = {'folder_name':['/']} # the server doesn't care about the folder name, clients care
	ret = {'dirs':{}, 'files':{}}
	print 'Python cache_list_all_files BEFORE'
	for file_name_index in range(4):
		print file_name_index,param
	try:
		print 'get_all_files begin FFFFAKE'		
		buf = simple_httpserver.handle_get_all_files(param)
	except Exception as e:
		print 'Except ', e
		traceback.print_exc()
	print 'Python cache_list_all_files ', buf
	buf = buf.split(':')
	try:
		assert(buf[0] == '0')
		len_files = int(buf[1])
		for i in range(len_files):
			file_name,size,is_foler = buf[2 + i*3:5 + i*3] # is_folder is deprecated
			file_name = name_remote_to_local(file_name); file_name = file_name.split('/');
			if len(file_name[0]) == 0:
				file_name = file_name[1:]
			len_path = len(file_name)
			cur_map = ret
			for path in file_name[0:-1]:
				if not cur_map['dirs'].has_key(path):
					cur_map['dirs'][path] = {'dirs':{},'files':{}}
				cur_map = cur_map['dirs'][path]
			assert cur_map['files'].has_key(file_name[-1]) == False
			cur_map['files'][file_name[-1]] = size		
	except Exception as e:
		print 'Python Exception', e
	print 'Python cache_list_all_files ret', ret
	return ret
	
def raw_cache_list_all_files():
	param = {'folder_name':['/']} # the server doesn't care about the folder name, clients care
	ret = {'dirs':{}, 'files':{}}
	buf = simple_httpserver.handle_get_all_files(param)
	buf = buf.split(':')
	assert(buf[0] == '0')
	len_files = int(buf[1])
	file_names = []
	for i in range(len_files):
		file_name,size,is_foler = buf[2 + i*3:5 + i*3] # is_folder is deprecated
		file_names.append([file_name,size])
	return file_names
		
	
# create the directory structure under the ROOT directory
CLIENT_ROOT_DIR = '/tmp/hehehe/'
	
def cache_get_chunks_info(file_name):
	param = {}
	file_name = name_local_to_remote(file_name)
	param['file_name'] = [file_name]
	param['request_chunk_index'] = ['0']
	buf = simple_httpserver.handle_meta_file_info(param)
	buf = buf.split(':')
	assert buf[0] == '0',buf
	file_name = buf[1]
	file_size = buf[2]
	is_folder = buf[3]
	len_chunks = int(buf[4])
	index = 5
	ret = {}
	ret['file_size'] = 0
	#print buf
	while len_chunks > 0:
		chunk_file_name,chunk_index,chunk_id, len_servers = buf[index:index+4]
		chunk_id = int(chunk_id)
		chunk_index = int(chunk_index)
		len_servers = int(len_servers) / 2
		index = index + 4
		ret[chunk_index] = []
		while len_servers > 0:
			ret[chunk_index].append(int(buf[index]))
			ret['file_size'] += int(buf[index+1])
			index += 2
			len_servers -= 1
		len_chunks -= 1
	return ret

def cache_create_file(file_name):
	global SERVERS
	file_name = name_local_to_remote(file_name)
	buf = simple_httpserver.handle_create_file({'file_name':[file_name]})
	buf = buf.split(':')
	print buf
	if buf[0] != '0':
		raise 'File: ' + file_name + ' could not be created! ' + buf[1]
	trans_id = int(buf[1])
	num_server = int(buf[2])
	file_name = buf[2 + num_server + 1]
	servers = [int(buf[i]) for i in range(3,2 + num_server + 1)]
	# do the upload 
	file_name = name_local_to_remote(file_name)
	target_file_name = '0_' + file_name + '.trans' + str(trans_id)
	# do the upload
	for server in servers:
		s = SERVERS[server]
		s['upload_file'](s['server_object'], 'fake_new_file_1k', target_file_name)	
	# confirm the transaction
	simple_httpserver.handle_commit_trans({'id':[trans_id]})
	try:
		os.remove(CLIENT_ROOT_DIR + file_name)
	except Exception as e:
		pass
	
def cache_del_file(file_name):
	file_name = name_local_to_remote(file_name)
	simple_httpserver.handle_del_file({'file_name':[file_name]})

def get_how_many_chunks_involved(file_name,start,size,is_read, chunk_info):
	file_name = name_local_to_remote(file_name);
	NUM = config.FILE_CHUNK_SIZE
	first_chunk = start / NUM
	last_chunk = (start + size - 1) / NUM
	chunk_number = len(chunk_info.keys()) - 1
	if is_read:
		if first_chunk >= chunk_number:
			return []
		if last_chunk >= chunk_number:
			last_chunk = chunk_number - 1
		return range(first_chunk, last_chunk + 1, 1)
	else: # write
		if first_chunk >= chunk_number + 1:
			return []
		return range(first_chunk,last_chunk + 1, 1)
	
# for test only
def cache_read_file(file_name, start, size):
	file_name = name_local_to_remote(file_name);
	chunk_info = cache_get_chunks_info(file_name)
	chunk_ids = get_how_many_chunks_involved(file_name, start, size, True, chunk_info)
	if len(chunk_ids) == 0:
		raise ' Read error, chunk info wrong!'
	str_chunk_ids = [str(i) for i in chunk_ids]
	buf = simple_httpserver.handle_read_file({'file_name':[file_name], 'chunk_ids':[','.join(str_chunk_ids)]})
	print buf
	assert(buf[0] == '0')
	
	tmp = buf.split(':')
	trans_id = int(tmp[1])
	len_server = int(tmp[2])
	servers = [int(i) for i in tmp[3:3+len(chunk_ids)]]
	
	byte_file = ''
	
	for chunk_index in range(len(chunk_ids)):
		# download
		target_file = str(chunk_ids[chunk_index]) + '_' + file_name + '.trans' + str(trans_id)
		local_file = str(chunk_ids[chunk_index]) + '_' + file_name
		s = SERVERS[servers[chunk_index]]
		s['download_file'](s['server_object'], target_file, '/tmp/' + local_file)
		f = open('/tmp/' + local_file,'r')
		
		# Skip the first 8 bytes
		version = f.read(4)
		size = int(f.read(config.HEADER_LENGTH-4))
		# stop reading when less than config.FILE_CHUNK_SIZE
		
		mm = f.read(size)
		byte_file += mm
		f.close()
		if size < config.FILE_CHUNK_SIZE or version[0] == '1':
			break
	
	simple_httpserver.handle_commit_trans({'id':[trans_id]})
	s = start % config.FILE_CHUNK_SIZE
	return ''.join(list(byte_file))
	
# test only
def cache_write_file(file_name, start, to_write):
	size = len(to_write)
	file_name = name_local_to_remote(file_name)
	chunk_info = cache_get_chunks_info(file_name)
	chunk_ids = get_how_many_chunks_involved(file_name, start, size, False, chunk_info)
	if len(chunk_ids) == 0:
		raise ' Write error, chunk info wrong! Start:' + str(start) + ' Size:' + str(size)
		
	# a tmp buf to do the update
	buf_to_write = ' ' * (len(chunk_ids) * config.FILE_CHUNK_SIZE)
	buf_to_write = list(buf_to_write)
	# first and last chunk should be considered
	if start % config.FILE_CHUNK_SIZE != 0 and start / config.FILE_CHUNK_SIZE < len(chunk_info):
		buf_to_write[0:config.FILE_CHUNK_SIZE] = cache_read_file(file_name, start, 1)
	if (start + size) % config.FILE_CHUNK_SIZE != 0 and (start + size) / config.FILE_CHUNK_SIZE < len(chunk_info):
		buf_to_write[-config.FILE_CHUNK_SIZE:] = cache_read_file(file_name, (start + size - 1) / config.FILE_CHUNK_SIZE, 1)
	
	s = start % config.FILE_CHUNK_SIZE 
	e = s + size
	iii = 0
	while s < e:
		buf_to_write[s] = to_write[iii]
		s += 1
		iii += 1

	str_chunk_ids = [str(i) for i in chunk_ids]
	str_chunk_sizes = ','.join([str(config.FILE_CHUNK_SIZE)] * len(chunk_ids))
	#print str_chunk_sizes
	buf = simple_httpserver.handle_write_file({'file_name':[file_name], 'chunk_ids':[','.join(str_chunk_ids)], 'chunk_size':[str_chunk_sizes]})
	#print buf
	assert(buf[0] == '0')
	trans_id = int(buf.split(':')[1])
	len_server = int(buf.split(':')[2])
	servers = buf.split(':')[3:3+len_server]
	
	NUM_PER_SERVER = len_server / len(chunk_ids)
	for index in range(len(chunk_ids)):
		chunk_id = chunk_ids[index]
		target_server = servers[NUM_PER_SERVER*index:(index+1)*NUM_PER_SERVER]
		chunk_content = str(index) * config.FILE_CHUNK_SIZE
		for server in target_server:
			server = int(server)
			s = SERVERS[server]
			f = open('/tmp/hehe','w')
			f.write(''.join(buf_to_write[index*config.FILE_CHUNK_SIZE:(index+1)*config.FILE_CHUNK_SIZE]))
			f.close()
			target_file_name = str(chunk_ids[index]) + '_' + file_name + '.trans' + str(trans_id)
			#print target_file_name,"HEHEHE"
			s['upload_file'](s['server_object'], '/tmp/hehe', target_file_name)
			
	buf = simple_httpserver.handle_commit_trans({'id':[trans_id]})
	assert(buf[0] == '0')
	
import pickle
WRITE_LOG_FILE = 'local_write_log'

def redo_logs():
	global WRITE_FAIL_MODE
	WRITE_FAIL_MODE = 0
	logs = pickle.load(open(WRITE_LOG_FILE,'r'))
	#logs.append([file_name,to_write,chunk_ids,[],0])
	for index in range(len(logs)):
		m = logs[index]
		file_name,to_write,chunk_ids,tmp,status = m
		if status != 1:
			cache_write_file_algined(file_name,to_write,chunk_ids,False)
			logs[index] = [file_name,'',chunk_ids,[],1]
			f = open(WRITE_LOG_FILE,'w')
			pickle.dump(logs,f)
			f.close()

# the write log is something like:
# [ [file_name,to_write,chunk_ids,chunk_ids_wroted,status], ... ]
# simplified version of cache_write_file, with specified chunk_id
def cache_write_file_algined(file_name,to_write,chunk_ids,real_file_length, log_it = True):
	log_it = config.SAVE_FAKE_LOG
	global WRITE_FAIL_MODE
	assert len(to_write) > 0
	# save the transactions now
	if os.path.exists(WRITE_LOG_FILE) == False:
		f = open(WRITE_LOG_FILE,'w')
		pickle.dump([],f)
		f.close()
	if log_it:
		logs = pickle.load(open(WRITE_LOG_FILE,'r'))
		logs.append([file_name,to_write,chunk_ids,[],0])
		f = open(WRITE_LOG_FILE,'w')
		pickle.dump(logs,f)
		f.close()
		
	# whether to write only parts
	opt_chunk_id = -1
	opt_chunk_length = -1
	if(real_file_length % config.FILE_CHUNK_SIZE != 0):
		opt_chunk_id = real_file_length // config.FILE_CHUNK_SIZE
		opt_chunk_length = real_file_length % config.FILE_CHUNK_SIZE
	
	size = len(to_write)
	file_name = name_local_to_remote(file_name)
	chunk_info = cache_get_chunks_info(file_name)
	# chunk_ids = get_how_many_chunks_involved(file_name, start, size, False, chunk_info)
	if len(chunk_ids) == 0:
		raise ' Write error, chunk info wrong! Start:' + str(start) + ' Size:' + str(size)
		
	# a tmp buf to do the update
	buf_to_write = ' ' * (len(chunk_ids) * config.FILE_CHUNK_SIZE)
	buf_to_write = list(buf_to_write)
		
	s = 0 
	e = s + size
	iii = 0
	while s < e:
		buf_to_write[s] = to_write[iii]
		s += 1
		iii += 1
		
	sizes = [config.FILE_CHUNK_SIZE] * (1 + ((size-1) // config.FILE_CHUNK_SIZE))
	if size % config.FILE_CHUNK_SIZE != 0:
		sizes[-1] = size % config.FILE_CHUNK_SIZE

	str_chunk_ids = [str(i) for i in chunk_ids]
	str_chunk_sizes = ','.join([str(config.FILE_CHUNK_SIZE)] * len(chunk_ids))
	#print str_chunk_sizes
	a = time.time()
	buf = simple_httpserver.handle_write_file({'file_name':[file_name], 'chunk_ids':[','.join(str_chunk_ids)], 'chunk_size':[str_chunk_sizes]})
	print 'handle_write costs ', time.time() - a
	#print buf
	assert(buf[0] == '0')
	trans_id = int(buf.split(':')[1])
	len_server = int(buf.split(':')[2])
	servers = buf.split(':')[3:3+len_server]
	
	NUM_PER_SERVER = len_server / len(chunk_ids)
	for index in range(len(chunk_ids)):
		chunk_id = chunk_ids[index]
		target_server = servers[NUM_PER_SERVER*index:(index+1)*NUM_PER_SERVER]
		chunk_content = str(index) * config.FILE_CHUNK_SIZE
		version = '0001'
		size = '%012d'%(sizes[index])
		count = 0
		for server in target_server:
			if WRITE_FAIL_MODE == 1:
				continue
			if WRITE_FAIL_MODE == 2 and count >= 1:
				continue
			count = count + 1
			server = int(server)
			s = SERVERS[server]
			f = open('/tmp/hehe','w')
			f.write(version)
			
			if chunk_id == opt_chunk_id:
				size = '%012d'%(opt_chunk_length)
				f.write(size)
				f.write(''.join(buf_to_write[index*config.FILE_CHUNK_SIZE:(index*config.FILE_CHUNK_SIZE + opt_chunk_length)]))
			else:
				f.write(size)
				f.write(''.join(buf_to_write[index*config.FILE_CHUNK_SIZE:(index+1)*config.FILE_CHUNK_SIZE]))
			f.close()
			target_file_name = str(chunk_ids[index]) + '_' + file_name + '.trans' + str(trans_id)
			#print target_file_name,"HEHEHE"
			a = time.time()
			s['upload_file'](s['server_object'], '/tmp/hehe', target_file_name)
			print 'upload one chunk costs ', time.time() - a
	a = time.time()		
	if WRITE_FAIL_MODE == 0:			
		buf = simple_httpserver.handle_commit_trans({'id':[trans_id]})
		print 'the commit cost ', time.time() - a
		assert(buf[0] == '0')
		if log_it:
			logs = pickle.load(open(WRITE_LOG_FILE,'r'))
			#logs.append([file_name,to_write,chunk_ids,[],0])
			logs[-1][4] = 1
			logs[-1][1] = ''
			f = open(WRITE_LOG_FILE,'w')
			pickle.dump(logs,f)
			f.close()
	
#### All the read/write functions should be only called by the Cache, clients use the api to write the file
# in local directory, the Cache will update the file and do some synchronization periodly

 
if __name__ == "__main__":
	print cache_list_all_files()
	cache_create_file('tutu2.txt')
	cache_write_file('tutu2.txt',1020,'Z'*8)
	#write_file('tutu2.txt', 1025, 10, 'B')
	#write_file('tutu2.txt', 1026, 10, 'L')
	