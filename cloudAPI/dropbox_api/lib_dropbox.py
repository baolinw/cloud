import dropbox

_API_ACCESS_TOKEN = 'Od8i3MHlBiAAAAAAAAAABZISCA_JLdWe5vROh3RpDGkyE1m3gwztshfcx863Eyy6'

# all the bucket_name is unused in the dropbox implementation
_BUCKET_NAME = ''

def upload_file(service,from_file_name,to_file_name):
	# try delete it first
	try:
		delete_file(service,'',"/" + to_file_name)
	except Exception as e:
		#print e
		pass
	f = open(from_file_name, 'rb')
	response = service.put_file(to_file_name, f)

def upload_string(service,str_to_upload,to_file_name):
	fake_name = 'justtmpfile123testjust'
	f = open(fake_name,'w')
	f.write(str_to_upload);
	f.close();
	upload_file(service,'',fake_name,to_file_name)
	os.remove(fake_name)

def delete_file(service,object_name):
	service.file_delete(object_name)
	
	
def download_file(service, object_name, to_file_name):
	f, metadata = service.get_file_and_metadata(object_name)
	out = open('magnum-opus.txt', 'wb')
	return f.read()

def get_all_file_names(service):	
	folder_metadata = service.metadata('/')
	ret =[]
	for entry in folder_metadata['contents']:
		path = entry['path']
		if path[0] == '/':
			path = path[1:]
		size = entry['bytes']
		ret.append((path,size))
	return ret	

def create_service_object(extra_info):
	client = dropbox.client.DropboxClient(_API_ACCESS_TOKEN)
	return client
 
#s = create_service_object()
#get_all_file_names(s,_BUCKET_NAME)
#download_file(s,_BUCKET_NAME,'hehe.txt')
#upload_file(s,_BUCKET_NAME,'tuzi.txt','tuzi.txt')
#print get_all_file_names(s,_BUCKET_NAME);