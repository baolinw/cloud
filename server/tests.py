# Test case, create a file into a single server
import google_api
import google_api.lib_google
import dropbox_api
import dropbox_api.lib_dropbox
import meta_puller
import simple_httpserver
import config

def delete_all_file():
	service = google_api.lib_google.create_service_object()
	service_box = dropbox_api.lib_dropbox.create_service_object()
	# delete all the files
	for file_name,file_size in google_api.lib_google.get_all_file_names(service):
		google_api.lib_google.delete_file(service, file_name)
		
	for file_name,file_size in dropbox_api.lib_dropbox.get_all_file_names(service_box):
		dropbox_api.lib_dropbox.delete_file(service_box,file_name)

def test_1():
	''' upload a fake file to the google storage and get this file '''
	service = google_api.lib_google.create_service_object()
	service_box = dropbox_api.lib_dropbox.create_service_object()
	# delete all the files
	for file_name,file_size in google_api.lib_google.get_all_file_names(service):
		google_api.lib_google.delete_file(service, file_name)
		
	for file_name,file_size in dropbox_api.lib_dropbox.get_all_file_names(service_box):
		dropbox_api.lib_dropbox.delete_file(service_box,file_name)
	
	import file_handler
	import meta_puller	
	
	google_api.lib_google.upload_file(service, 'fake_new_file_1k', '0_tuzi1.txt')
	# TEST1: repull the metas
	meta_puller.pull_meta()
	assert('tuzi1.txt' in meta_puller.get_all_file_names())
	file_meta_info = meta_puller.get_file_meta_info('tuzi1.txt');
	assert(file_meta_info[0] == 'tuzi1.txt');
	assert(file_meta_info[1] == config.FILE_CHUNK_SIZE);
	assert(file_meta_info[2] == 0);
	assert(meta_puller.get_chunks_id('tuzi1.txt') == 1)
	tmp = meta_puller.get_file_chunk_info('tuzi1.txt', 0)
	assert(tmp[0][0] == 0);
	assert(tmp[0][1] == config.FILE_CHUNK_SIZE);
	print 'test_1_single_file_single_chunk passed'
	
	# TEST2: single file multiple chunks
	#service = google_api.lib_google.create_service_object()
	google_api.lib_google.upload_file(service, 'fake_new_file_1k', '0_tuzi2.txt')
	google_api.lib_google.upload_file(service, 'fake_new_file_1k', '1_tuzi2.txt')
	google_api.lib_google.upload_file(service, 'fake_new_file_1k', '2_tuzi2.txt')
	# repull the metas
	meta_puller.pull_meta()
	assert('tuzi2.txt' in meta_puller.get_all_file_names())
	file_meta_info = meta_puller.get_file_meta_info('tuzi2.txt');
	assert(file_meta_info[0] == 'tuzi2.txt');
	assert(file_meta_info[1] == config.FILE_CHUNK_SIZE*3);
	assert(file_meta_info[2] == 0);
	assert(meta_puller.get_chunks_id('tuzi2.txt') == 3)
	tmp0 = meta_puller.get_file_chunk_info('tuzi2.txt', 0)
	tmp1 = meta_puller.get_file_chunk_info('tuzi2.txt', 1)
	tmp2 = meta_puller.get_file_chunk_info('tuzi2.txt', 2)
	assert(tmp0[0][0] == 0);  assert(tmp0[0][1] == config.FILE_CHUNK_SIZE); 
	assert(tmp1[0][0] == 0);  assert(tmp1[0][1] == config.FILE_CHUNK_SIZE); 
	assert(tmp2[0][0] == 0);  assert(tmp2[0][1] == config.FILE_CHUNK_SIZE); 
	print 'test_1_single_file_3_chunk passed'
	
	# TEST3: 2 server , 3 chunks
	google_api.lib_google.upload_file(service, 'fake_new_file_1k', '0_tuzi3.txt')
	google_api.lib_google.upload_file(service, 'fake_new_file_1k', '1_tuzi3.txt')
	dropbox_api.lib_dropbox.upload_file(service_box, 'fake_new_file_1k', '1_tuzi3.txt')
	google_api.lib_google.upload_file(service, 'fake_new_file_1k', '2_tuzi3.txt')
	# repull the metas
	meta_puller.pull_meta()
	assert('tuzi3.txt' in meta_puller.get_all_file_names())
	file_meta_info = meta_puller.get_file_meta_info('tuzi3.txt');
	assert(file_meta_info[0] == 'tuzi3.txt');
	assert(file_meta_info[1] == config.FILE_CHUNK_SIZE * 3);
	assert(file_meta_info[2] == 0);
	assert(meta_puller.get_chunks_id('tuzi3.txt') == 3)
	tmp0 = meta_puller.get_file_chunk_info('tuzi3.txt', 0)
	tmp1 = meta_puller.get_file_chunk_info('tuzi3.txt', 1)
	tmp2 = meta_puller.get_file_chunk_info('tuzi3.txt', 2)
	assert(tmp0[0][0] == 0);  assert(tmp0[0][1] == config.FILE_CHUNK_SIZE); 
	assert(tmp1[0][0] == 0);  assert(tmp1[0][1] == config.FILE_CHUNK_SIZE); 
	assert(tmp1[1][0] == 1);  assert(tmp1[1][1] == config.FILE_CHUNK_SIZE); 
	assert(tmp2[0][0] == 0);  assert(tmp2[0][1] == config.FILE_CHUNK_SIZE); 
	print 'test_1_single_file_2_server_3_chunk passed'
	
	# TEST4: 1 chunk two server, size not match
	google_api.lib_google.upload_file(service, 'fake_new_file_1k', '0_tuzi4.txt')
	dropbox_api.lib_dropbox.upload_file(service_box, 'fake_new_file_1', '0_tuzi4.txt')
	# repull the metas
	'''find_exception = False
	try:
		meta_puller.pull_meta()
	except Exception as e:
		find_exception = True
	assert find_exception '''
	print 'test_1_single_file_2_server_1_chunk_size_not_match passed'

def test_basic_get_all_meta_info():
	''' test the 'communication' between client and server '''
	import simple_httpserver
	service = google_api.lib_google.create_service_object()
	delete_all_file()
	google_api.lib_google.upload_file(service, 'fake_new_file_1k', '0_tuzi22.txt')
	google_api.lib_google.upload_file(service, 'fake_new_file_1k', '1_tuzi22.txt')
	meta_puller.pull_meta()
	buf = simple_httpserver.handle_get_all_files({'folder_name':['/']})
	#print buf
	assert buf == '0:1:tuzi22.txt:' + str(2*config.FILE_CHUNK_SIZE) + ':0'
	tmp = str(config.FILE_CHUNK_SIZE)
	buf = simple_httpserver.handle_meta_file_info({'file_name':['tuzi22.txt'],'request_chunk_index':[0]})
	assert buf == '0:tuzi22.txt:2048:0:2:tuzi22.txt:0:0:2:0:1024:tuzi22.txt:1:1:2:0:1024'
	print 'test basic interface passed'
	
def test_create_file_from_user(FILE_NAME):
	import simple_httpserver
	
	service = google_api.lib_google.create_service_object()
	delete_all_file()
	meta_puller.pull_meta()
	# create a file 
	buf = simple_httpserver.handle_create_file({'file_name':[FILE_NAME]})
	buf = buf.split(':')
	#print buf
	assert(buf[0] == '0') # success
	buferr = simple_httpserver.handle_create_file({'file_name':[FILE_NAME]})
	buferr = buferr.split(':')
	assert(buferr[0] == '-1') # duplication
	
	trans_id = int(buf[1])
	num_server = int(buf[2])
	file_name = buf[2 + num_server + 1]
	servers = [int(buf[i]) for i in range(3,2 + num_server + 1)]
	# do the upload 
	target_file_name = '0_' + file_name + '.trans' + str(trans_id)
	# do the upload
	for server in servers:
		if server != 0:
			continue
		google_api.lib_google.upload_file(service, 'fake_new_file_1k', target_file_name)	
	# confirm the transaction
	simple_httpserver.handle_commit_trans({'id':[trans_id]})
	
	# create a file with the same name
	buf = simple_httpserver.handle_create_file({'file_name':[FILE_NAME]})
	buf = buf.split(':')
	assert(buf[0] == '-2') # not success, already exist

	# check the metainfo
	assert(FILE_NAME in meta_puller.get_all_file_names())
	tmp = meta_puller.get_file_chunk_info(FILE_NAME, 0)
	assert(tmp[0][0] == 0);  assert(tmp[0][1] == config.FILE_CHUNK_SIZE); 
	
	print 'basic create file test passed'

def test_delete_file_from_user(FILE_NAME):
	# first create the file
	test_create_file_from_user(FILE_NAME)
	# delete it, there is no transaction here
	buf = simple_httpserver.handle_del_file({'file_name':[FILE_NAME]})
	assert(buf[0] == '0')
	buf = simple_httpserver.handle_del_file({'file_name':[FILE_NAME]})
	assert(buf[0] != '0')
	
	# test the meta info
	assert (FILE_NAME not in meta_puller.get_all_file_names())
	print 'test for deletion file single thread passed'
	
def test_simple_write(FILE_NAME):
	# first create the file
	test_create_file_from_user(FILE_NAME)
	# delete it, there is no transaction here
	buf = simple_httpserver.handle_write_file({'file_name':[FILE_NAME],'chunk_ids':['0,1,2'],'chunk_size':['1024,1024,1024']});
	#print buf
	assert(buf[0] == '0')
	buf = simple_httpserver.handle_write_file({'file_name':[FILE_NAME],'chunk_ids':['0,1'],'chunk_size':['1024,1024']});
	assert(buf[0] != '0')
	
	print 'test for deletion file single thread passed'
	
def test_write_file_3_chunk(FILE_NAME):
	service = google_api.lib_google.create_service_object()
	delete_all_file()
	#google_api.lib_google.upload_file(service, 'fake_new_file', '0_tuzi22.txt')
	
	# first create the file
	test_create_file_from_user(FILE_NAME)
	# request write to chunk 0,1,2
	buf = simple_httpserver.handle_write_file({'file_name':[FILE_NAME], 'chunk_ids':['0,1,2'], 'chunk_size':['1024,1024,1024']})

	assert(buf[0] == '0')
	trans_id = int(buf.split(':')[1])
	len_server = int(buf.split(':')[2])
	servers = buf.split(':')[3:3+len_server]
	
	NUM_PER_SERVER = len_server / 3
	for index in range(3):
		chunk_id = index
		target_server = servers[NUM_PER_SERVER*index:(index+1)*NUM_PER_SERVER]
		chunk_content = str(index) * config.FILE_CHUNK_SIZE
		for server in target_server:
			server = int(server)
			if server != 0:
				continue
			f = open('/tmp/hehe','w')
			f.write(chunk_content)
			f.close()
			target_file_name = str(index) + '_' + FILE_NAME + '.trans' + str(trans_id)
			google_api.lib_google.upload_file(service, '/tmp/hehe', target_file_name)
			
	# commit the transaction
	buf = simple_httpserver.handle_commit_trans({'id':[trans_id]})
	assert(buf[0] == '0')
	# test for anomalous size
	buf = simple_httpserver.handle_write_file({'file_name':[FILE_NAME], 'chunk_ids':['0'], 'chunk_size':['1021']})
	assert(buf[0] != '0')
	# test for the write twice
	#buf = simple_httpserver.handle_write_file({'file_name':[FILE_NAME], 'chunk_ids':['0,1,2'], 'chunk_size':['1024,1024,1024']})
	#assert(buf[0] == '0')
	#buf = simple_httpserver.handle_write_file({'file_name':[FILE_NAME], 'chunk_ids':['0,1,2'], 'chunk_size':['1024,1024,1024']})
	#assert(buf[0] != '0')
	# non-exist
	buf = simple_httpserver.handle_write_file({'file_name':[FILE_NAME+'never'], 'chunk_ids':['0,1,2'], 'chunk_size':['1024,1024,1024']})
	assert(buf[0] != '0')
	# test for the updated information about FILE_NAME
	#print meta_puller.get_file_meta_info(FILE_NAME)
	assert(int(meta_puller.get_file_meta_info(FILE_NAME)[1]) == config.FILE_CHUNK_SIZE * 3)
	print 'try to read the chunks'
	buf = simple_httpserver.handle_read_file({'file_name':[FILE_NAME+'never'], 'chunk_ids':['0,1,2']})
	assert(buf[0] != '0')
	buf = simple_httpserver.handle_read_file({'file_name':[FILE_NAME], 'chunk_ids':['0,1,2']})
	#print buf
	assert(buf[0] == '0')
	print 'read all the chunks'
	tmp = buf.split(':')
	trans_id = int(tmp[1])
	len_server = int(tmp[2])
	servers = [int(i) for i in tmp[3:3+3]]
	byte_file = []
	for chunk_index in range(3):
		# download
		target_file = str(chunk_index) + '_' + FILE_NAME + '.trans' + str(trans_id)
		local_file = str(chunk_index) + '_' + FILE_NAME
		google_api.lib_google.download_file(service, target_file, '/tmp/' + local_file)
		f = open('/tmp/' + local_file,'r')
		mm = f.read()
		assert(mm == str(chunk_index) * 1024)
		f.close()
		
	simple_httpserver.handle_commit_trans({'id':[trans_id]})
		
	# last should be -1
	buf = simple_httpserver.handle_read_file({'file_name':[FILE_NAME], 'chunk_ids':['0,1,2,3']})
	
	
	print 'test for sipmle write 3 chunk finished'
	

	
delete_all_file()	
'''print '=' * 60
test_1()
delete_all_file()
print '=' * 60
test_basic_get_all_meta_info()
print '=' * 60
test_create_file_from_user('tutu_create.txt')	
print '=' * 60
test_delete_file_from_user('tutu_del.txt')
print '=' * 60
test_simple_write('tutu_create.txt') '''
print '=' * 60
test_write_file_3_chunk('tuzi_write.txt')
print '=' * 60




