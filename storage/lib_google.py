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

def upload_file(service,bucket_name,from_file_name,to_file_name):
	# The BytesIO object may be replaced with any io.Base instance.
	media = apiclient.http.MediaIoBaseUpload(io.BytesIO('wawatuzimomo'), 'text/plain')
	# All object_resource fields here are optional.
	object_resource = {
			#'metadata': {'my-key': 'my-value'},
			'contentLanguage': 'en',
			#'md5Hash': 'HlAhCgICSX+3m8OLat5sNA==',
			#'crc32c': 'rPZE1w==',

	}
	req = service.objects().insert(
			bucket=bucket_name,
			name=to_file_name,
			body=object_resource,     # optional
			media_body=media)
	resp = req.execute()
	#print json.dumps(resp, indent=2)

def upload_string(str_to_upload,to_file_name):
	pass

def delete_file(service,bucket_name,object_name):
	service.objects().delete(
        bucket=bucket_name,
        object=object_name).execute()
	pass
	
def download_file(service, bucket_name,object_name):
	# Get Payload Data
	req = service.objects().get_media(
			bucket=bucket_name,
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
	#print fh.getvalue()

def get_all_file_names(service,bucket_name):
	try:
		fields_to_return = 'nextPageToken,items(name,size,contentType,metadata(my-key))'
		req = service.objects().list(bucket=bucket_name, fields=fields_to_return)
		# If you have too many items to list in one request, list_next() will
		# automatically handle paging with the pageToken.
		file_names = []
		while req is not None:
			resp = req.execute()
			#print json.dumps(resp, indent=2)
			for name in resp['items']:
				file_names.append(name['name'])
			req = service.objects().list_next(req, resp)
		return file_names

	except client.AccessTokenRefreshError:
		print ("The credentials have been revoked or expired, please re-run"
		"the application to re-authorize")
	

def create_service_object():
	storage = file.Storage('sample.dat')
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
 
s = create_service_object()
get_all_file_names(s,_BUCKET_NAME)
download_file(s,_BUCKET_NAME,'hehe.txt')
upload_file(s,_BUCKET_NAME,'','tuzi.txt')
#delete_file(s,_BUCKET_NAME,'tuzi.txt')
print get_all_file_names(s,_BUCKET_NAME);