# this file implentents the file meta puller 

import os
import google_api
import google_api.lib_google
import dropbox_api
import dropbox_api.lib_dropbox


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
		'name' : 'Google', \
		'server_object' : google_api.lib_google.create_service_object(), \
		'get_all_file_names' : google_api.lib_google.get_all_file_names, \
		'download_file' : google_api.lib_google.download_file, \
		'delete_file' :  google_api.lib_google.delete_file, \
		'upload_file' : google_api.lib_google.upload_file
	})
	SERVERS.append( { \
		'id':1, \
		'name' : 'DropBox1', \
		'server_object' : dropbox_api.lib_dropbox.create_service_object(), \
		'get_all_file_names' : dropbox_api.lib_dropbox.get_all_file_names, \
		'download_file' : dropbox_api.lib_dropbox.download_file, \
		'delete_file' :  dropbox_api.lib_dropbox.delete_file, \
		'upload_file' : dropbox_api.lib_dropbox.upload_file
	})
	
def pull_meta(DELETE_NON_FINISH_TRANSCATION = False):
	global SERVERS
	global FILES
	# map from file_name to a map ( chunk_id to a list of server id)
	map_file_name_to_server = {}
	# Get all the files
	for server in SERVERS:
		all_files = server['get_all_file_names']((server['server_object']))
		for file_name,file_size in all_files:
			if file_name.split('.')[-1].startswith('trans') and DELETE_NON_FINISH_TRANSCATION == True:
				raise "Not implemented!"
			
			chunk_id = int(file_name.split('_')[0])
			file_name = ''.join(file_name.split('_')[1:])
			
			if map_file_name_to_server.has_key(file_name) == False:
				map_file_name_to_server[file_name] = {}
			if map_file_name_to_server[file_name].has_key(chunk_id) == False:
				map_file_name_to_server[file_name][chunk_id] = []
			map_file_name_to_server[file_name][chunk_id].append((server['id'],int(file_size)))
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
			assert all([tmp[i-1][1] == tmp[i][1] for i in range(1,len(tmp))])
			file_size += tmp[0][1]
		FILES[file_name]['file_size'] = file_size
		assert(all(boolean_vec));

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
	
def create_file_by_renaming(trans_id,file_name, servers):
	global SERVERS
	FILES[file_name] = {}
	FILES[file_name]['file_size'] = 2
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
		s['delete_file'](s['server_object'], tmp_file_name);
		FILES[file_name][0].append((server,2))
		#print 'The upload', '/tmp/' + tmp_file_name, '0_' + file_name
		
	
	