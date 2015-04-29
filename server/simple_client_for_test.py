# This file is mainly for test, before implementation the real, I will implement
# all the logic here because python development is more efficient
import os
import google_api
import google_api.lib_google
import dropbox_api
import dropbox_api.lib_dropbox
import config
import simple_httpserver
import meta_puller
# I need the service object to do actual upload/download
SERVERS = []
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

meta_puller.init_config()
meta_puller.pull_meta()

CHUNK_SIZE = config.FILE_CHUNK_SIZE
# return [(file_name,file_size), (file_name, file_size)]
def list_all_files():
	param = {'folder_name':['/']}
	ret = []
	buf = simple_httpserver.handle_get_all_files(param)
	buf = buf.split(':')
	assert(buf[0] == '0')
	len_files = int(buf[1])
	for i in range(len_files):
		file_name,size,is_foler = buf[2 + i*3:5 + i*3]
		ret.append((file_name,int(size)))
	return ret
	
def get_chunks_info(file_name):
	param = {}
	param['file_name'] = [file_name]
	param['request_chunk_index'] = ['0']
	buf = simple_httpserver.handle_meta_file_info(param)
	buf = buf.split(':')
	# print buf
	assert(buf[0] == '0')
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

def create_file(file_name):
	global SERVERS
	buf = simple_httpserver.handle_create_file({'file_name':[file_name]})
	buf = buf.split(':')
	if buf[0] != '0':
		raise 'File: ' + file_name + ' could not be created! ' + buf[1]
	trans_id = int(buf[1])
	num_server = int(buf[2])
	file_name = buf[2 + num_server + 1]
	servers = [int(buf[i]) for i in range(3,2 + num_server + 1)]
	# do the upload 
	target_file_name = '0_' + file_name + '.trans' + str(trans_id)
	# do the upload
	for server in servers:
		s = SERVERS[server]
		s['upload_file'](s['server_object'], 'fake_new_file_1k', target_file_name)	
	# confirm the transaction
	simple_httpserver.handle_commit_trans({'id':[trans_id]})

def get_how_many_chunks_involved(file_name,start,size,is_read, chunk_info):
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
def read_file(file_name, start, size):
	chunk_info = get_chunks_info(file_name)
	chunk_ids = get_how_many_chunks_involved(file_name, start, size, True, chunk_info)
	if len(chunk_ids) == 0:
		raise ' Read error, chunk info wrong!'
	str_chunk_ids = [str(i) for i in chunk_ids]
	buf = simple_httpserver.handle_read_file({'file_name':[file_name], 'chunk_ids':[','.join(str_chunk_ids)]})
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
		mm = f.read()
		byte_file += mm
		f.close()	
	
	simple_httpserver.handle_commit_trans({'id':[trans_id]})
	s = start % config.FILE_CHUNK_SIZE
	return list(byte_file)
	
# test only
def write_file(file_name, start, size, char_to_write):
	chunk_info = get_chunks_info(file_name)
	chunk_ids = get_how_many_chunks_involved(file_name, start, size, False, chunk_info)
	if len(chunk_ids) == 0:
		raise ' Write error, chunk info wrong! Start:' + str(start) + ' Size:' + str(size)
		
	# a tmp buf to do the update
	buf_to_write = ' ' * (len(chunk_ids) * config.FILE_CHUNK_SIZE)
	buf_to_write = list(buf_to_write)
	# first and last chunk should be considered
	if start % config.FILE_CHUNK_SIZE != 0 and start / config.FILE_CHUNK_SIZE < len(chunk_info):
		buf_to_write[0:config.FILE_CHUNK_SIZE] = read_file(file_name, start, 1)
	if (start + size) % config.FILE_CHUNK_SIZE != 0 and (start + size) / config.FILE_CHUNK_SIZE < len(chunk_info):
		buf_to_write[-config.FILE_CHUNK_SIZE:] = read_file(file_name, (start + size - 1) / config.FILE_CHUNK_SIZE, 1)
	
	s = start % config.FILE_CHUNK_SIZE 
	e = s + size
	while s < e:
		buf_to_write[s] = char_to_write
		s += 1

	str_chunk_ids = [str(i) for i in chunk_ids]
	str_chunk_sizes = ','.join([str(config.FILE_CHUNK_SIZE) * len(chunk_ids)])
	buf = simple_httpserver.handle_write_file({'file_name':[file_name], 'chunk_ids':[','.join(str_chunk_ids)], 'chunk_size':[str_chunk_sizes ]})
	
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
			target_file_name = str(index) + '_' + file_name + '.trans' + str(trans_id)
			s['upload_file'](s['server_object'], '/tmp/hehe', target_file_name)
			
	buf = simple_httpserver.handle_commit_trans({'id':[trans_id]})
	assert(buf[0] == '0')

write_file('tutu2.txt', 12, 10, 'B')
write_file('tutu2.txt', 22, 10, 'L')
	