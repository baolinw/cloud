# this file is to handle the file transactions
# it is the main application level in the server
# our goal is to make the server as light weight as possible
import time
import trans
import meta_puller
import config
import random
import pickle
import os
import time

# the "transaction" is implemented as follows:
# using some trick to give the client some files to handle, 
# when the client commit the transaction, the server issue a mv operation
# to "commit" the change in the server in synchronise way

CreatFileLock = {} # map 'string' to arbitrary integer
WriteFileLock = {}
ReadFileLock = {}

trans.Init()

SERVER_LOG_FILE_NAME = config.SERVER_LOG_FILE_NAME
FT_MODE = 0 #0: ok, 1 no commit at all, 2 commit only one

# pulling datas from cloud storage
meta_puller.init_config()
meta_puller.pull_meta()

def request_fail_server(server_id):
	meta_puller.report_fail(server_id)
	return '0'

def request_ok_server(server_id):
	meta_puller.ok_server(server_id)
	return '0'
 
def get_all_files(folder_name):
	ret = []
		
	all_files =  meta_puller.get_all_file_names()
	#print 'all files ', all_files	

	for file_name_index in range(len(all_files)):
		file_name = all_files[file_name_index]
		file_name_index += 1
		#print 'file_name ',file_name
		tmp = meta_puller.get_file_meta_info(file_name)
		file_map = {}
		file_map['file_name'] = file_name
		file_map['size'] = tmp[1]
		file_map['is_folder'] = tmp[2]
		ret.append(file_map)
		#print 'file_name ', file_name, ' Loop Ends', all_files
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
	#return range(len(meta_puller.SERVERS))
	# randomly select
	ret = []
	for id in range(len(meta_puller.SERVERS)):
		if meta_puller.SERVERS[id]['live'] == 0:
			continue
		ret.append(id)
	random.shuffle(ret)
	upper = min(len(ret),config.FILE_DUPLICATE_NUM)
	ret = ret[0:upper]
	if len(ret) < config.FILE_DUPLICATE_NUM:
		ret.extend([-10] * (config.FILE_DUPLICATE_NUM - len(ret)))
	return ret	
	
def alive_server_ids():
	ret = []
	for id in range(len(meta_puller.SERVERS)):
		if meta_puller.SERVERS[id]['live'] == 0:
			continue
		ret.append(id)
	return ret

# randomly
def choose_servers(alread_choosed,num_total):
	remain = num_total - len(alread_choosed)
	if remain <= 0:
		return alread_choosed
	alives = alive_server_ids()
	for i in range(len(alives)):
		if alives[i] in alread_choosed:
			alives[i] = -1
	ret = alread_choosed
	count = 0
	for i in range(remain):
		if count < len(alives) and alives[count] != -1:
			ret.append(alives[count])
		count += 1
	if len(ret) < num_total:
		ret.extend([-1] * (num_total - len(ret)))
	return ret			
	
# where to put the file to write, 
# if it already exist, put it in original server, 
# else call choose_create_target_server
def choose_write_target_server(file_name, chunks):	
	ret = []
	for chunk in chunks:
		if chunk < meta_puller.get_chunks_id(file_name):
			original_location = meta_puller.get_file_chunk_info(file_name,chunk)
			original_location = [i[0] for i in original_location]
			if len(original_location) >= config.FILE_DUPLICATE_NUM:
				ret.extend(original_location[0:config.FILE_DUPLICATE_NUM])
			else:
				ret.extend(choose_servers(original_location,config.FILE_DUPLICATE_NUM))
				#remain = config.FILE_DUPLICATE_NUM - len(original_location)
				#num_server = meta_puller.get_server_num()
				#other_server = random.shuffle(range(num_server))[0:remain]
				# for test purpose, I only use 0
				#other_server = [0] * remain
				#ret.extend(other_server);
		else: # add new chunk
			# TODO, implement server choosing algorithm
			tmp = choose_create_target_server(file_name)			
			ret.extend(tmp)
			#ret.extend([0] * config.FILE_DUPLICATE_NUM)
	return ret	

# where to read the files
def choose_read_target_server(file_name, chunks):
	# currently I only choose the 1st one
	ret = []
	max_chunk_id = meta_puller.get_chunks_id(file_name)
	for chunk in chunks:
		if max_chunk_id <= chunk:
			ret.append(-1)
			continue
		tmp = meta_puller.get_file_chunk_info(file_name,chunk)
		ret.append(tmp[0][0])
	return ret		
	
# all the transactions returns the 0:0:xx: format for the client to understand
def request_create_file(file_name):
	#print file_name
	global CreatFileLock
	# check the existence of file_name, 
	
	# lock, we assume single thread mode in server, so never mind the lock here..
	if CreatFileLock.has_key(file_name):
		if config.IGNORE_LOCK == False:
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

def request_write_file(file_name,chunks, chunk_sizes):
	if file_name not in meta_puller.get_all_file_names():
		return '-2:file Not exist'
		
	# first check the readFileLock
	
	if ReadFileLock.has_key(file_name):
		for chunk in chunks:
			if chunk in ReadFileLock[file_name].keys():
				if config.IGNORE_LOCK == False:
					return '-1:Read Locked by others'
		
	if WriteFileLock.has_key(file_name):
		for chunk in chunks:
			if chunk in WriteFileLock[file_name].keys():
				if config.IGNORE_LOCK == False:
					return '-1:Write Locked by others'
				
	if not WriteFileLock.has_key(file_name):
		WriteFileLock[file_name] = {}
		
	for chunk in chunks:
		WriteFileLock[file_name][chunk] = 0
	
	# create a transaction id
	trans_id = trans.get_next_trans_id()
	# add the transaction to transaction manager
	target_server = choose_write_target_server(file_name, chunks)
	# [chunk_0_server_1, chunk_0_server_2.., chunk_1_server_1]
	
	trans.AddTrans(trans_id,[ \
		'WRITE_FILE', \
		file_name,
		target_server,
		chunks,
		chunk_sizes,
		time.time()	])
	# return the response for client to do their own stuff
	# success, trans_id, file_name
	tmp = [str(0), str(trans_id), str(len(target_server)) ]
	for server in target_server:
		tmp.append(str(server))
	return ':'.join(tmp)	
	
def request_read_file(file_name,chunks):
	if file_name not in meta_puller.get_all_file_names():
		return '-2:file Not exist'
		
	if WriteFileLock.has_key(file_name):
		for chunk in chunks:
			if chunk in WriteFileLock[file_name].keys():
				if config.IGNORE_LOCK == False:
					return '-1:Write Locked by others'
				
	if not ReadFileLock.has_key(file_name):
		ReadFileLock[file_name] = {}
		
	for chunk in chunks:
		ReadFileLock[file_name][chunk] = 0
	
	# create a transaction id
	trans_id = trans.get_next_trans_id()
	# add the transaction to transaction manager
	target_server = choose_read_target_server(file_name, chunks)
	
	meta_puller.copy_file_by_renaming(trans_id, file_name, chunks, target_server);
	
	trans.AddTrans(trans_id,[ \
		'READ_FILE', \
		file_name,
		target_server,
		chunks,
		time.time()	])
	# return the response for client to do their own stuff
	# success, trans_id, file_name
	tmp = [str(0), str(trans_id), str(len(target_server)) ]
	for server in target_server:
		tmp.append(str(server))
	return ':'.join(tmp)

def del_file(file_name):
	# will implement the lock mechanism later
	if file_name not in meta_puller.get_all_file_names():
		return '-1:not file named ' + file_name
	
	ret_val,ret_msg = meta_puller.del_file(file_name)
	return str(ret_val) + ":" + ret_msg

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
	
def handle_ft_mode(mode_id):
	global FT_MODE
	FT_MODE = mode_id
	return '0'
	
def handle_resume():
	global FT_MODE,SERVER_LOG_FILE_NAME
	logs = pickle.load(open(SERVER_LOG_FILE_NAME,'r'))
	# [trans_id,file_name,chunks,chunk_sizes,target_servers,finished_servers,status]
	for index in range(len(logs)):
		m = logs[index]
		trans_id,file_name,chunks,chunk_sizes,target_servers, finished_servers, status = m
		if status != 1:
			remain = [i for i in target_servers if i not in finished_servers]
			meta_puller.update_file_by_renaming(trans_id, file_name, chunks, chunk_sizes, remain)			
			logs[index][-2] = target_servers
			logs[index][-1] = 1
			f = open(SERVER_LOG_FILE_NAME,'w')
			pickle.dump(logs,f)
			f.close()
	FT_MODE = 0
	return '0'
	
# commit the write file
# the log file looks like [trans_id,file_name,chunks,chunk_sizes,target_servers,finished_servers,status]
def commit_write_file(trans_id):
	global SERVER_LOG_FILE_NAME
	if trans.has_key(trans_id) == False or trans.get_key(trans_id) == None:
		return '-1:' + 'No such Trans:' + str(trans_id)
	if config.SAVE_FAKE_LOG == True and os.path.exists(SERVER_LOG_FILE_NAME) == False:
		f = open(SERVER_LOG_FILE_NAME,'w')
		pickle.dump([],f)
		f.close()
			
	Trans = trans.get_key(trans_id)
	# extract the transaction information
	file_name = Trans[1]
	target_servers = Trans[2]
	chunks = Trans[3]
	chunk_sizes = Trans[4]
	
	if config.SAVE_FAKE_LOG == True :
		f = open(SERVER_LOG_FILE_NAME,'r')
		logs = pickle.load(f)
		f.close()
		logs.append([trans_id,file_name,chunks,chunk_sizes,target_servers,[],0])
		f = open(SERVER_LOG_FILE_NAME,'w')
		pickle.dump(logs,f)
		f.close()
	
	global FT_MODE
	if FT_MODE == 0:
		a = time.time()
		meta_puller.update_file_by_renaming(trans_id, file_name, chunks, chunk_sizes, target_servers)
		#print 'handle commit renaming cost ', time.time() - a, ' s'
		if config.SAVE_FAKE_LOG == True:
			logs[-1][-2] = target_servers
			logs[-1][-1] = 1		
	elif FT_MODE == 1:
		pass
	else: #2, partial
		target_servers = target_servers[0:1]
		meta_puller.update_file_by_renaming(trans_id, file_name, chunks, chunk_sizes, target_servers)
		if config.SAVE_FAKE_LOG == True:
			logs[-1][-2] = target_servers
			logs[-1][-1] = 0

	if config.SAVE_FAKE_LOG == True:
		f = open(SERVER_LOG_FILE_NAME,'w')
		pickle.dump(logs,f)
		f.close()		
	
	for c in chunks:
		del WriteFileLock[file_name][c]
	trans.DelTrans(trans_id)
	meta_puller.pull_meta()
	return '0'
	
def commit_read_file(trans_id):
	if trans.has_key(trans_id) == False or trans.get_key(trans_id) == None:
		return '-1:' + 'No such Trans:' + str(trans_id)
	Trans = trans.get_key(trans_id)
	# extract the transaction infomation
	file_name = Trans[1]
	target_servers = Trans[2]
	chunks = Trans[3]
	
	# meta_puller.update_file_by_renaming(trans_id, file_name, chunks, chunk_sizes, target_servers)
	# just delete something
	meta_puller.del_tmp_file_to_read(trans_id, file_name, chunks, target_servers)
	
	for c in chunks:
		del ReadFileLock[file_name][c]
	
	trans.DelTrans(trans_id)
	return '0'
	
def handle_commit(transaction_id, msg):
	if trans.has_key(transaction_id) == False:
		return '-1:No Transaction or Tans be deleted'
	trans.LockResource()
	ret_result = '0:Transaction Succeed'
	#try:
	command = trans.get_key(transaction_id)[0]
	if command == 'CREATE_FILE':
		ret_result = commit_create_file(transaction_id)
	elif command == 'WRITE_FILE':
		ret_result = commit_write_file(transaction_id)
	elif command == 'READ_FILE':
		ret_result = commit_read_file(transaction_id)
	else:
		ret_result = '-1:unknown tansaction cmd'
	#except Exception as e:
	#	print 'Exception!!',str(e)
	#	trans.DelTrans(transaction_id)
	#	ret_result = '-1:' + str(e)
	trans.UnlockResource()
	return ret_result
	
