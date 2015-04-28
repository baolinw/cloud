import argparse
import httplib2
import os
import sys
import json
import io

from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools
import apiclient
import apiclient.http

# Define sample variables.
_BUCKET_NAME = 'mmmbbb'
_API_VERSION = 'v1'

CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')

def upload_file(service,from_file_name,to_file_name):
	# try delete it first
	try:
		delete_file(service,'',"/" + to_file_name)
	except Exception as e:
		pass
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
	req = service.objects().insert(
			bucket=_BUCKET_NAME,
			name=to_file_name,
			body=object_resource,     # optional
			media_body=media)
	resp = req.execute()
	#print json.dumps(resp, indent=2)

def upload_string(service, str_to_upload,to_file_name):
	# The BytesIO object may be replaced with any io.Base instance.
	media = apiclient.http.MediaIoBaseUpload(io.BytesIO(str_to_upload), 'text/plain')
	# All object_resource fields here are optional.
	object_resource = {
			#'metadata': {'my-key': 'my-value'},
			'contentLanguage': 'en',
			#'md5Hash': 'HlAhCgICSX+3m8OLat5sNA==',
			#'crc32c': 'rPZE1w==',

	}
	req = service.objects().insert(
			bucket=_BUCKET_NAME,
			name=to_file_name,
			body=object_resource,     # optional
			media_body=media)
	resp = req.execute()

def delete_file(service,object_name):
	service.objects().delete(
        bucket=_BUCKET_NAME,
        object=object_name).execute()
	pass
	
def download_file(service ,object_name, to_file_name):
	# Get Payload Data
	req = service.objects().get_media(
			bucket=_BUCKET_NAME,
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

def get_all_file_names(service):
	try:
		fields_to_return = 'nextPageToken,items(name,size,contentType,metadata(my-key))'
		req = service.objects().list(bucket=_BUCKET_NAME, fields=fields_to_return)
		# If you have too many items to list in one request, list_next() will
		# automatically handle paging with the pageToken.
		file_names = []
		while req is not None:
			resp = req.execute()
			#print json.dumps(resp, indent=2)
			if  'items' not in resp:
				break;
			for name in resp['items']:
				file_names.append((name['name'],name['size']))
			req = service.objects().list_next(req, resp)
		return file_names

	except client.AccessTokenRefreshError:
		print ("The credentials have been revoked or expired, please re-run"
		"the application to re-authorize")
	

def create_service_object():
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
	return service
	
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
	s = create_service_object()
	print get_all_file_names(s,_BUCKET_NAME)
#download_file(s,_BUCKET_NAME,'hehe.txt')
#upload_string(s,_BUCKET_NAME,'ylsistuzi','tuzi.txt')
#delete_file(s,_BUCKET_NAME,'tuzi.txt')
#print get_all_file_names(s,_BUCKET_NAME);