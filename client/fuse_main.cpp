/*
  FUSE: Filesystem in Userspace
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>

  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.

  gcc -Wall hello.c `pkg-config fuse --cflags --libs` -o hello
*/

#define FUSE_USE_VERSION 26

#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include "http_lib.h"
#include "meta_info.h"

static const char *hello_str = "Hello World!\n";
static const char *hello_path = "/hello";

using namespace std;
//need to abstract a new class to handle, current prototype is find now
struct SimpeFile {
	string file_name;
	int file_size;
};
vector<SimpeFile> g_all_files;

static int hello_getattr(const char *path, struct stat *stbuf)
{
	int res = 0;

	memset(stbuf, 0, sizeof(struct stat));
	if (strcmp(path, "/") == 0) {
		stbuf->st_mode = S_IFDIR | 0755;
		stbuf->st_nlink = 2;
	} else if (strcmp(path, hello_path) == 0) {
		stbuf->st_mode = S_IFREG | 0444;
		stbuf->st_nlink = 1;
		stbuf->st_size = strlen(hello_str);
	} else
		res = -ENOENT;

	return res;
}

static int hello_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
			 off_t offset, struct fuse_file_info *fi)
{
	(void) offset;
	(void) fi;

	if (strcmp(path, "/") != 0)
		return -ENOENT;

	filler(buf, ".", NULL, 0);
	filler(buf, "..", NULL, 0);
	//filler(buf, hello_path + 1, NULL, 0);
	for(SimpeFile sf : g_all_files) {
		filler(buf, sf.file_name.c_str(), NULL,0);
	}

	return 0;
}

static int hello_open(const char *path, struct fuse_file_info *fi)
{
	if (strcmp(path, hello_path) != 0)
		return -ENOENT;

	if ((fi->flags & 3) != O_RDONLY)
		return -EACCES;

	return 0;
}

static int hello_read(const char *path, char *buf, size_t size, off_t offset,
		      struct fuse_file_info *fi)
{
	size_t len;
	(void) fi;
	if(strcmp(path, hello_path) != 0)
		return -ENOENT;

	len = strlen(hello_str);
	if ((int)offset < (int)len) {
		if (offset + size > len)
			size = len - offset;
		memcpy(buf, hello_str + offset, size);
	} else
		size = 0;

	return size;
}

static struct fuse_operations hello_oper;



static void get_all_file_from_server(vector<SimpeFile>& out) {
	out.clear();
	vector<MetaFileInfo> result = get_all_files("/");
	for(MetaFileInfo mfi : result) {
		SimpeFile sf;
		sf.file_name = mfi.file_name;
		//cout << sf.file_name << endl;
		sf.file_size = mfi.file_size;
		out.push_back(sf);
	}
}

int main(int argc, char *argv[])
{
	/* Init file operations */
	hello_oper.getattr	= hello_getattr,
	hello_oper.readdir	= hello_readdir,
	hello_oper.open		= hello_open,
	hello_oper.read		= hello_read,
	/* Init the libcurl */
	HTTPClient::InitHTTPConnection(MASTER_SERVER_URL,false);
	//test_main_meta_info();
	// Get all the file_names first 
	get_all_file_from_server(g_all_files);
	//return 0;
	return fuse_main(argc, argv, &hello_oper, NULL);
}