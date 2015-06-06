# this file implentents the file meta puller 

import os
import google_api
import google_api.lib_google
import dropbox_api
import dropbox_api.lib_dropbox
import config
import local_api
import local_api.lib_local
import random
import log

# server descriptor 
# every server is a dict, having an id, name, some function pointers
SERVERS = []

# file object
# every file object is a dict
FILES = []

def init_config():
	global SERVERS
	# only be called once when starting the server
	# currently I wrote the configurations manually, TODO: using config file
	SERVERS = []
	# init the google server
	SERVERS.append( { \
		'id':0, \
		'live': 1, \
		'name' : 'Google', \
		'server_object' : google_api.lib_google.create_service_object('test'), \
		'get_all_file_names' : google_api.lib_google.get_all_file_names, \
		'download_file' : google_api.lib_google.download_file, \
		'delete_file' :  google_api.lib_google.delete_file, \
		'upload_file' : google_api.lib_google.upload_file
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
	
def pull_meta(DELETE_NON_FINISH_TRANSCATION = False):
	global SERVERS
	global FILES
	# map from file_name to a map ( chunk_id to a list of server id)
	map_file_name_to_server = {}
	# Get all the files
	#logs = pickle.load(open(config.SERVER_LOG_FILE_NAME,'r'))
	for server in SERVERS:
		if server['live'] == 0:
			continue
		all_files = server['get_all_file_names']((server['server_object']))
		for file_name,file_size in all_files:
			if file_name.split('.')[-1].startswith('trans') and DELETE_NON_FINISH_TRANSCATION == True:
				raise "Not implemented!"
			if file_name.split('.')[-1].startswith('trans'):
				#ignore
				continue					
			file_size = file_size - config.HEADER_LENGTH
			chunk_id = int(file_name.split('_')[0])
			file_name = '_'.join(file_name.split('_')[1:])			
			
			if map_file_name_to_server.has_key(file_name) == False:
				map_file_name_to_server[file_name] = {}
			if map_file_name_to_server[file_name].has_key(chunk_id) == False:
				map_file_name_to_server[file_name][chunk_id] = []
			map_file_name_to_server[file_name][chunk_id].append([server['id'],int(file_size)])
	#print map_file_name_to_server
			
	# convert map_file_name_to_server to FILES
	FILES = map_file_name_to_server
	# check all chunk size match
	for file_name in FILES.keys():
		file_size = 0
		max_chunk_id = max(FILES[file_name].keys())
		boolean_vec = [False for i in range(max_chunk_id + 1)]
		for chunk_id in FILES[file_name].keys():
			boolean_vec[chunk_id] = True
			tmp = FILES[file_name][chunk_id]
			#####assert all([tmp[i-1][1] == tmp[i][1] for i in range(1,len(tmp))])
			file_size += tmp[0][1]
		FILES[file_name]['file_size'] = file_size
		#####assert(all(boolean_vec));

# get all_file_name
def get_all_file_names():
	global FILES
	return FILES.keys()

# return [file_name, file_size, is_folder]
def get_file_meta_info(file_name):
	assert file_name in FILES.keys()
	return [file_name, FILES[file_name]['file_size'], 0]

def get_chunks_id(file_name):
	return len(FILES[file_name].keys()) - 1
	
def get_file_chunk_info(file_name, chunk_id):
	tmp = FILES[file_name][chunk_id]
	return tmp		
	
def get_server_num():
	return len(SERVERS)


def create_file_by_renaming(trans_id,file_name, servers):
	global SERVERS,FILES
	FILES[file_name] = {}
	FILES[file_name]['file_size'] = config.FILE_CHUNK_SIZE
	FILES[file_name][0] = []
	for server in servers:
		if len(SERVERS) <= server:
			continue
		s = SERVERS[server]
		tmp_file_name = '0_' + file_name + ".trans" + str(trans_id);
		# do the copy operation
		# currently we only download and upload, further will update it if the service does allow "copy" 
		s['download_file'](s['server_object'],tmp_file_name, '/tmp/' + tmp_file_name)
		s['upload_file'](s['server_object'], '/tmp/' + tmp_file_name, '0_' + file_name);
		
		log.log_write(server,file_name,0,trans_id)
		s['delete_file'](s['server_object'], tmp_file_name);
		FILES[file_name][0].append([server,os.stat('/tmp/' + tmp_file_name).st_size - config.HEADER_LENGTH])
		#print 'The upload', '/tmp/' + tmp_file_name, '0_' + file_name
		
def update_file_by_renaming(trans_id,file_name,chunk_ids, chunk_sizes, servers):
	global SERVERS, FILES
	NUM_PER_SERVER = len(servers) / len(chunk_ids)
	assert len(servers) % len(chunk_ids) == 0
	for index in range(len(chunk_ids)):
		target_servers = servers[NUM_PER_SERVER*index:NUM_PER_SERVER*(index+1)]
		for server in target_servers:
			if len(SERVERS) <= server:
				continue
			to_get_file_name = str(chunk_ids[index]) + '_' + file_name + ".trans" + str(trans_id)
			overide_file_name = str(chunk_ids[index]) + '_' + file_name
			s = SERVERS[server]
			s['download_file'](s['server_object'],to_get_file_name, '/tmp/' + to_get_file_name)
			s['upload_file'](s['server_object'], '/tmp/' + to_get_file_name, overide_file_name);
			log.log_write(server,file_name,chunk_ids[index],trans_id)
			s['delete_file'](s['server_object'], to_get_file_name);
			# update the info in FILES
			if FILES[file_name].has_key(index):
				find_one = False
				for index_server_chunk in range(len(FILES[file_name][index])):
					s_,size_ = FILES[file_name][index][index_server_chunk]
					if(s_ == server):
						FILES[file_name][index][index_server_chunk][1] = chunk_sizes[index]
						find_one = True
						break
				if not find_one:
					FILES[file_name][index].append([server, chunk_sizes[index]])
			else:
				FILES[file_name][index] = []
				FILES[file_name][index].append([server,chunk_sizes[index]])
	
	# recalculate the file size
	sum_file_size = 0
	for key in FILES[file_name].keys():
		if key == 'file_size':
			continue
		sum_file_size += FILES[file_name][key][0][1]
	FILES[file_name]['file_size'] = sum_file_size

def del_tmp_file_to_read(trans_id, file_name, chunks, target_server):
	for index in range(len(chunks)):
		c_id = chunks[index]
		s_id = target_server[index]
		if s_id < 0:
			continue
		if len(SERVERS) <= s_id:
			continue
		s = SERVERS[s_id]
		target_file_name = str(c_id) + '_' + file_name + '.trans' + str(trans_id)
		s['delete_file'](s['server_object'], target_file_name)
	
# make a copy for reading
def copy_file_by_renaming(trans_id, file_name, chunks, target_server):
	for index in range(len(chunks)):
		c_id = chunks[index]		
		s_id = target_server[index]
		if s_id < 0:
			continue
		if len(SERVERS) <= s_id:
			continue
		s = SERVERS[s_id]
		to_get_file_name = str(c_id) + '_' + file_name 
		overide_file_name = str(c_id) + '_' + file_name + '.trans' + str(trans_id)
		
		s['download_file'](s['server_object'],to_get_file_name, '/tmp/' + to_get_file_name)
		s['upload_file'](s['server_object'], '/tmp/' + to_get_file_name, overide_file_name);


def del_file(file_name):
	# get all the server storing the file
	if FILES.has_key(file_name) == False:
		return -1,'File Not Exist ' + file_name
	for key in FILES[file_name].keys():
		if key == 'file_size':
			continue
		for server_id,_ in FILES[file_name][key]:
			cloud_file_name = str(key) + "_" + file_name
			if server_id >= len(SERVERS):
				continue
			s = SERVERS[server_id]
			s['delete_file'](s['server_object'], cloud_file_name)
			
	del FILES[file_name]
	return 0,'Success'

def ok_server(server_id):
	global SERVERS,FILES
	assert server_id < len(SERVERS)
	if SERVERS[server_id]['live'] == 1:
		return
	SERVERS[server_id]['live'] = 1
	pull_meta(False)
	# detect whether the files is not up to date now, the log system should come ...
	for file_name in FILES.keys():
		f = FILES[file_name]		
		for chunk_id in f.keys():
			if chunk_id == 'file_size':
				continue			
			if chunk_id == 0 and file_name == 'pp.txt':
				a = 10
			servers = [i[0] for i in f[chunk_id]]
			# get the update id from
			updated_id_servers = log.get_last_update_id(file_name,chunk_id)
			if len(updated_id_servers) == 0:
				continue
			in_date = [i for i in updated_id_servers if SERVERS[i]['live'] == 1]
			out_of_dated = [i for i in servers if i not in in_date and SERVERS[i]['live'] == 1]
			if server_id in out_of_dated:
				# just del
				target_file = str(chunk_id) + '_' + file_name
				s = SERVERS[server_id]
				if server_id == 0 and target_file == '0_pp.txt':
					target_file = '0_pp.txt'
				#print 'DEL: server:', str(server_id),' ', target_file
				s['delete_file'](s['server_object'], target_file)
			# migrate
			if len(in_date) < config.FILE_DUPLICATE_NUM:
				migration_to(file_name, chunk_id, in_date,server_id)
	pull_meta(False)
	

# file_name can be '', report the server's who failure 
def report_fail(server_id, reported_file_name = '', chunk_file = 0):
	global SERVERS,FILES
	# current implementation is very slow, it won't allow other access from clients too
	if reported_file_name != '':
		assert False,'Not implemented by single reporting'
	assert server_id < len(SERVERS)
	if SERVERS[server_id]['live'] == 0:
		return
	SERVERS[server_id]['live'] = 0
	# find those originally in the target server_id
	for file_name in FILES.keys():
		f = FILES[file_name]
		for chunk_id in f.keys():
			if chunk_id == 'file_size':
				continue
			servers = [i[0] for i in f[chunk_id]]
			if server_id not in servers:
				continue
			migration(file_name,chunk_id,servers)
	# re-get the server contents
	pull_meta(False)
	
# migrate all the files originally in the server_id, to other servers
def migration(file_name,chunk_id,servers):
	#print 'In migration()', file_name, str(chunk_id), servers
	global SERVERS
	# make those -1 
	# how many alive now
	alives = 0
	for i in range(len(servers)):
		if SERVERS[servers[i]]['live'] == 0:
			servers[i] = -1
		else:
			alives += 1
	remain = config.FILE_DUPLICATE_NUM - alives
	if remain < 0:
		return
	# choose the first one to replicate
	source = -1
	for i in range(len(servers)):
		if servers[i] != -1:
			source = servers[i]
			break
	if source == -1:
		assert False, 'File Lost !'
	candidates = [i for i in range(len(SERVERS)) if i not in servers and SERVERS[i]['live'] == 1]
	random.shuffle(candidates)
	candidates = candidates[0:min(len(candidates),remain)]
	# download the original file
	s = SERVERS[source]
	target_file = str(chunk_id) + '_' + file_name
	s['download_file'](s['server_object'], target_file, '/tmp/' + 'for_fix_up')
	write_version = log.get_write_version(source,file_name,chunk_id)
	for m in candidates:
		s = SERVERS[m]
		s['upload_file'](s['server_object'], '/tmp/' + 'for_fix_up',  target_file)
		log.log_write(m, file_name,chunk_id,write_version)
		
# migrate all the files originally in the server_id, to other servers
def migration_to(file_name,chunk_id,the_from,the_to):
	#print 'In migration222()', file_name, str(chunk_id), 'from ',the_from,' to ',the_to
	global SERVERS
	#print 'migration_to ', the_from, 
	# download the original file
	s = SERVERS[the_from[0]]
	target_file = str(chunk_id) + '_' + file_name
	s['download_file'](s['server_object'], target_file, '/tmp/' + 'for_fix_up')
	write_version = log.get_write_version(the_from[0],file_name,chunk_id)
	m = the_to
	s = SERVERS[m]
	s['upload_file'](s['server_object'], '/tmp/' + 'for_fix_up',  target_file)
	log.log_write(m, file_name,chunk_id,write_version)
	
# we just simulate some hard-coded server joining event
def server_join_test1():
	pass
# we can only simulate some server failure case, 
def server_fail_test1():
	pass
		