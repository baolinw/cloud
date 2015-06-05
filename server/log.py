# I use this for simulating log, currently I only use the file, not the database
import pickle
import os
import os.path

WRITE_LOG_FILE = 'WRITE_LOG'  # the write transaction id of WRITE_LOG
# when the write is involked 

## Write Log ###################################################
def log_write(server_id, file_name,chunk_id,trans_id):
	if trans_id == -1:
		trans_id = -1
	if os.path.exists(WRITE_LOG_FILE) == False:
		f = open(WRITE_LOG_FILE,'w')
		pickle.dump({},f)
		f.close()
	f1 = open(WRITE_LOG_FILE,'r')
	w = pickle.load(f1)
	f1.close()
	
	if file_name not in w.keys():
		w[file_name] = {}
	if chunk_id not in w[file_name].keys():
		w[file_name][chunk_id] = {}
	#if server_id not in w[file_name][chunk_id].keys():
	w[file_name][chunk_id][server_id] = trans_id
			
	f2 = open(WRITE_LOG_FILE,'w')
	pickle.dump(w,f2)
	f2.close()
	
def get_write_version(server_id,file_name,chunk_id):
	f1 = open(WRITE_LOG_FILE,'r')
	w = pickle.load(f1)
	f1.close()
	if file_name in w.keys() and chunk_id in w[file_name].keys() and server_id in w[file_name][chunk_id].keys():
		return w[file_name][chunk_id][server_id]
	return -1

# get the last server ids, for a file and its chunk_id, return those chunks having the same last transaction id
def get_last_update_id(file_name,chunk_id):
	f1 = open(WRITE_LOG_FILE,'r')
	w = pickle.load(f1)
	f1.close()
	server_ids = []
	max_trans = -1
	if w.has_key(file_name) == False:
		return []
	if w[file_name].has_key(chunk_id) == False:
		return []
	for server_id in w[file_name][chunk_id]:
		server_ids.append((server_id,w[file_name][chunk_id][server_id]))
		max_trans = max(max_trans,w[file_name][chunk_id][server_id])
	if max_trans == -1:
		max_trans = -1
	return [i[0] for i in server_ids if i[1] == max_trans]
