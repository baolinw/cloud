#ifndef _PYTHON_WRAPPER_H_
#define _PYTHON_WRAPPER_H_
#include <vector>
#include <string>
#include <stdio.h>
#include "Python.h"
#define CHUNK_SIZE 1024
#define SCRIPT_DIR "/home/ubuntu/cloud/server"

struct FileSize
{
	char* file_name;
	int file_size;
};

// The class of the store engine, wrapper of Python codes
class StoreEngine
{
public:
	void Init(int argc,char** argv);
	void DeInit();
	void Mount();
	void force_update();
	std::vector<FileSize> list_all_files();
	int create_file(const std::string& file_name);
	FILE* open_file(const std::string& file_name, const std::string& mode);
	int del_file(const std::string& file_name);
	int close_file(FILE* file, const std::string& file_name);
	void remove_local(const std::string& file_name);
	void make_dirty(const std::string& file_name, int start, int size);
public: // Singleton
	static StoreEngine* get_instance();
private:
	void sync_download_file(const std::string& file_name);
	void sync_upload_file(const std::string& file_name);
	
	
	std::string get_root_dir();	
private: // data
	std::string _root_dir;
	PyObject* _pModule;
	PyObject* _pDict;
private:
	static StoreEngine* _instance;
	StoreEngine() {};
};

#endif