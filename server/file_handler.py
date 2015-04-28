# this file is to handle the file transactions
# it is the main application level in the server
# our goal is to make the server as light weight as possible
import time
import mysql_api
import trans
import meta_puller

# the "transaction" is implemented as follows:
# using some trick to give the client some files to handle, 
# when the client commit the transaction, the server issue a mv operation
# to "commit" the change in the server in synchronise way

CreatFileLock = {} # map 'string' to arbitrary integer

trans.Init()

# pulling datas from cloud storage
meta_puller.init_config()
meta_puller.pull_meta()
 
def get_all_files(folder_name):
	ret = []
	for file_name in meta_puller.get_all_file_names():
		file_map = {}
		file_map['file_name'] = file_name
		file_map['size'] = meta_puller.get_file_meta_info(file_name)[1]
		file_map['is_folder'] = meta_puller.get_file_meta_info(file_name)[2]
		ret.append(file_map)
	return ret	

def get_all_chunks_of_file(file_name):
	''' return representation of the chunks '''
	num_ids = meta_puller.get_chunks_id(file_name)
	ret = []
	for i in range(num_ids):
		one_chunk = {}	
		tmp = meta_puller.get_file_chunk_info(file_name, i)
		one_chunk['file_name'] = file_name;
		one_chunk['chunk_index'] = i
		one_chunk['chunk_id'] = i
		one_chunk['server_id_of_chunk'] = []
		for server_id,size in tmp:
			one_chunk['server_id_of_chunk'].append(server_id)
			one_chunk['server_id_of_chunk'].append(size)
		ret.append(one_chunk)
	return ret
	
def abort_transaction(key, trans_value):
	if trans_value[0] == 'CREATE_FILE':
		return abort_create_file(trans_value)
	
def abort_create_file(trans):
	print "ABORT: " + trans[1]

# which server to put the file
def choose_create_target_server(file_name):
	return [0]
	
# all the transactions returns the 0:0:xx: format for the client to understand
def request_create_file(file_name):
	#print file_name
	global CreatFileLock
	# check the existence of file_name, 
	
	# lock, we assume single thread mode in server, so never mind the lock here..
	if CreatFileLock.has_key(file_name):
		return '-1:File Creation Pending'
	if file_name in meta_puller.get_all_file_names():
		return '-2:File already exist!'
	CreatFileLock[file_name] = 0
	# create a transaction id
	trans_id = trans.get_next_trans_id()
	# add the transaction to transaction manager
	target_server = choose_create_target_server(file_name)
	trans.AddTrans(trans_id,[ \
		'CREATE_FILE', \
		file_name,
		target_server,
		time.time()	])
	# return the response for client to do their own stuff
	# success, trans_id, file_name
	tmp = [str(0), str(trans_id), str(len(target_server)) ]
	for server in target_server:
		tmp.append(str(server))
	tmp.append(file_name);
	return ':'.join(tmp)
	

# distribute the file into different server

# commit the create file
def commit_create_file(trans_id):
	if trans.has_key(trans_id) == False or trans.get_key(trans_id) == None:
		return '-1:' + 'No such Trans:' + str(trans_id)
	# change the file name
	#try:
	#	mysql_api.create_file((trans.get_key(trans_id))[1])
	#except Exception as e:
	#	abort_transaction(trans_id,trans.get_key(trans_id))
	#	trans.DelTrans(trans_id)	
	#	return '-1:' + str(e)
	meta_puller.create_file_by_renaming(trans_id,trans.get_key(trans_id)[1], trans.get_key(trans_id)[2])
	file_name = trans.get_key(trans_id)[1]
	del CreatFileLock[file_name]
	trans.DelTrans(trans_id)	
	return '0'

def handle_commit(transaction_id, msg):
	trans.LockResource()
	ret_result = '0:Transaction Succeed'
	#try:
	ret_result = commit_create_file(transaction_id)
	#except Exception as e:
	#	print 'Exception!!',str(e)
	#	trans.DelTrans(transaction_id)
	#	ret_result = '-1:' + str(e)
	trans.UnlockResource()
	return ret_result
	