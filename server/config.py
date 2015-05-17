#All Configurations
FILE_DUPLICATE_NUM = 1 # how many replications stored
FILE_CHUNK_SIZE = 1024 # 1KB, the chunk size, will be larger when finishing debugging

# some common convert functions shared by server and client
def name_local_to_remote(name):
	return name.replace('/','[[')
	
def name_remote_to_local(name):
	return name.replace('[[','/')