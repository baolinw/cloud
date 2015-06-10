import argparse
import httplib2
import os
import sys
import json
import io
import time
from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools
import apiclient
import apiclient.http

# Define sample variables.
BUCKETS = ['mmmbbb','bbbmmm','cccbbb']
_API_VERSION = 'v1'
g_service = None
TRIED_TIME = 10
CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')

def upload_file(service,from_file_name,to_file_name):
	global g_service,BUCKETS,TRIED_TIME
	# try delete it first
	try:
		delete_file(service,'',"/" + to_file_name)
	except Exception as e:
		pass
	for i in range(TRIED_TIME):
		exception = False
		try:
			# The BytesIO object may be replaced with any io.Base instance.
			f = open(from_file_name,'r')
			media = apiclient.http.MediaIoBaseUpload(io.BytesIO(f.read()), 'text/plain')
			f.close()
			# All object_resource fields here are optional.
			object_resource = {
					#'metadata': {'my-key': 'my-value'},
					'contentLanguage': 'en',
					#'md5Hash': 'HlAhCgICSX+3m8OLat5sNA==',
					#'crc32c': 'rPZE1w==',

			}
			req = g_service.objects().insert(
					bucket=BUCKETS[service],
					name=to_file_name,
					body=object_resource,     # optional
					media_body=media)
			resp = req.execute()
		except Exception as e:
			exception = True
			print e
		if i == TRIED_TIME - 1 and exception == True:
			raise Exception('Try many times, false too')
		if exception == False:
			break			
	#print json.dumps(resp, indent=2)
	
def copy_file(service,service_to,file_name,file_name_to):
	global g_service,BUCKETS
	object_resource = {
			'contentLanguage': 'en',
			'contentType' : 'file',			
	}
	for i in range(TRIED_TIME):
		exception = False
		try:
			req = g_service.objects().copy(
					sourceBucket=BUCKETS[service],
					sourceObject = file_name,
					destinationBucket=BUCKETS[service_to],
					destinationObject = file_name_to,
					body=object_resource)
			resp = req.execute()
		except Exception as e:
			exception = True
			print e
		if i == TRIED_TIME - 1 and exception == True:
			raise Exception('Try many times, false too')
		if exception == False:
			break	

def upload_string(service, str_to_upload,to_file_name):
	global g_service,BUCKETS
	# The BytesIO object may be replaced with any io.Base instance.
	media = apiclient.http.MediaIoBaseUpload(io.BytesIO(str_to_upload), 'text/plain')
	# All object_resource fields here are optional.
	object_resource = {
			#'metadata': {'my-key': 'my-value'},
			'contentLanguage': 'en',
			#'md5Hash': 'HlAhCgICSX+3m8OLat5sNA==',
			#'crc32c': 'rPZE1w==',

	}
	req = g_service.objects().insert(
			bucket=BUCKETS[service],
			name=to_file_name,
			body=object_resource,     # optional
			media_body=media)
	resp = req.execute()

def delete_file(service,object_name):
	global g_service,BUCKETS
	for i in range(TRIED_TIME):
		exception = False
		try:
			g_service.objects().delete(
				bucket=BUCKETS[service],
				object=object_name).execute()
		except Exception as e:
			exception = True
			print e
		if i == TRIED_TIME - 1 and exception == True:
			raise Exception('Try many times, false too')
		if exception == False:
			break	
	pass
	
def download_file(service ,object_name, to_file_name):
	global g_service,BUCKETS
	# Get Payload Data
	for i in range(TRIED_TIME):
		exception = False
		try:
			req = g_service.objects().get_media(
					bucket=BUCKETS[service],
					object=object_name)
			# The BytesIO object may be replaced with any io.Base instance.
			fh = io.BytesIO()
			downloader = apiclient.http.MediaIoBaseDownload(fh, req, chunksize=1024*1024)
			done = False
			while not done:
				status, done = downloader.next_chunk()
				#if status:
				#	print 'Download %d%%.' % int(status.progress() * 100)
				#print 'Download Complete!'
			f = open(to_file_name,'w')
			v = fh.getvalue();
			f.write(v);
			f.close()
			return v
		except Exception as e:
			exception = True
			print e
		if i == TRIED_TIME - 1 and exception == True:
			raise Exception('Try many times, false too')
		if exception == False:
			break	

def get_all_file_names(service):
	global g_service, BUCKETS
	for i in range(TRIED_TIME):
		exception = False
		try:
			fields_to_return = 'nextPageToken,items(name,size,contentType,metadata(my-key))'
			req = g_service.objects().list(bucket=BUCKETS[service], fields=fields_to_return)
			# If you have too many items to list in one request, list_next() will
			# automatically handle paging with the pageToken.
			file_names = []
			while req is not None:
				resp = req.execute()
				#print json.dumps(resp, indent=2)
				if  'items' not in resp:
					break;
				for name in resp['items']:
					file_names.append((name['name'],int(name['size'])))
				req = g_service.objects().list_next(req, resp)
			return file_names

		except Exception as e:
			exception = True
			print e
		if i == TRIED_TIME - 1 and exception == True:
			raise Exception('Try many times, false too')
		if exception == False:
			break
			 

def create_service_object(extra_info):
	global g_service
	if g_service != None:
		return extra_info
	storage = file.Storage(os.path.join(os.path.dirname(__file__), 'sample.dat'))
	credentials = storage.get()
	if credentials is None or credentials.invalid:
		raise "You need to refresh the access token"

	# Create an httplib2.Http object to handle our HTTP requests and authorize it
	# with our good Credentials.
	http = httplib2.Http()
	http = credentials.authorize(http)

	# Construct the service object for the interacting with the Cloud Storage API.
	service = discovery.build('storage', _API_VERSION, http=http)
	g_service = service
	return extra_info
	
def reinit_storage(argv):
	parser = argparse.ArgumentParser(
		description=__doc__,
		formatter_class=argparse.RawDescriptionHelpFormatter,
		parents=[tools.argparser])
	
	CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')
	FLOW = client.flow_from_clientsecrets(CLIENT_SECRETS,
	  scope=[
		  'https://www.googleapis.com/auth/devstorage.full_control',
		  'https://www.googleapis.com/auth/devstorage.read_only',
		  'https://www.googleapis.com/auth/devstorage.read_write',
		],
		message=tools.message_if_missing(CLIENT_SECRETS))
	storage = file.Storage(os.path.join(os.path.dirname(__file__), 'sample.dat'))
	flags = parser.parse_args(argv[1:])
	credentials = tools.run_flow(FLOW, storage, flags)
 
if __name__ == "__main__":
	# --noauth_local_webserver
	# reinit_storage(sys.argv)
	s = create_service_object(0)
	a = time.time()
	get_all_file_names(s)
	b = time.time()
	print b - a, ' ss'
	a = open('/tmp/abc','w')
	a.write('a'*2000000)
	a.close()
	a = time.time()
	upload_file(s,'/tmp/abc','abc')
	b = time.time()
	print b - a, ' S'
#download_file(s,_BUCKET_NAME,'hehe.txt')
#upload_string(s,_BUCKET_NAME,'ylsistuzi','tuzi.txt')
#delete_file(s,_BUCKET_NAME,'tuzi.txt')
#print get_all_file_names(s,_BUCKET_NAME);