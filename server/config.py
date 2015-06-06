#All Configurations
FILE_DUPLICATE_NUM = 2 # how many replications stored
FILE_CHUNK_SIZE = 1024*512 # 1KB, the chunk size, will be larger when finishing debugging
HEADER_LENGTH = 16 # header size

IGNORE_LOCK = True # for test purpose

SERVER_LOG_FILE_NAME = 'server_write_log'

# some common convert functions shared by server and client
def name_local_to_remote(name):
	return name.replace('/','[[')
	
def name_remote_to_local(name):
	return name.replace('[[','/')