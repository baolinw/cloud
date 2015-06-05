// I use this file to call some functions in Python
// The python is so important in our job because the interface is through python
#include <iostream>
#include <vector>
#include <assert.h>
#include <unistd.h>
#include "Python.h"
#include "python_wrapper.h"

using namespace std;

// Wrappers for python object
static string get_root_dir(PyObject* pDict)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"get_root_dir");
	if(!pFunc) cerr << "Can't find get_root_dir in the module" << endl;
	PyObject* result = PyObject_CallFunction(pFunc,NULL);
	char* str = NULL;
	PyArg_Parse(result,"s",&str);
	if(result) Py_DECREF(result);
	return string(str);
}

static void Mount(PyObject* pDict)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"Mount");
	if(!pFunc) {
		cerr << "Can't find Mount in the module" << endl;
	}
	PyObject* pResult = PyObject_CallFunction(pFunc,NULL);
	cout << "The result of pResult " << pResult << endl;
	//Py_DECREF(pFunc);
	if(pResult) Py_DECREF(pResult);
}

static void test(PyObject* pDict)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"test");
	if(!pFunc) {
		cerr << "Can't find test in the module" << endl;
	}
	PyObject* pResult = PyObject_CallFunction(pFunc,NULL);
	cout << "test returned" << endl;
	//Py_DECREF(pFunc);
	if(pResult) Py_DECREF(pResult);
}

static void force_update(PyObject* pDict)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"force_update");
	if(!pFunc) cerr << "Can't find force_update in the module" << endl;
	PyObject_CallFunction(pFunc,NULL);
}
static vector<FileSize> list_all_files(PyObject* pDict, bool force_update)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"list_all_files");
	if(!pFunc) {
		cerr << "Can't find list_all_files in the module" << endl;
	}
	PyObject* pResult = PyObject_CallFunction(pFunc,NULL);
	//Py_DECREF(pFunc);
	PyObject* dict = NULL;
	//cout << "The result is " << pResult << endl;
	if(!PyArg_Parse(pResult,"O!",&PyDict_Type,&dict)) 
		cerr << "PyArg_parse Dict error!" << endl;		
	//cout << "The dict is " << dict << endl;	
	// get files key
	PyObject* files = PyMapping_GetItemString(dict,"files");
	// get the keys
	PyObject* file_names = PyMapping_Keys(files);
	int size = PyList_Size(file_names);
	vector<FileSize> ret;
	//cout << " the file names are " << endl;
	for(int i = 0; i < size; i++) {
		PyObject* file_name = PyList_GetItem(file_names,i);
		char* str_file_name;
		PyArg_Parse(file_name,"s",&str_file_name);
		FileSize fs;
		//cout << string(str_file_name) << endl;
		PyObject* py_size = PyMapping_GetItemString(files,str_file_name);
		char* file_size = 0;
		PyArg_Parse(py_size,"s",&file_size);
		fs.file_name = str_file_name;
		fs.file_size = atoi(file_size);
		ret.push_back(fs);
		Py_DECREF(file_name); Py_DECREF(py_size);
		//cout << "size is " << atoi(file_size) << endl;
	}
	for(int i = 0; i < ret.size(); i++) {
		cout << string(ret[i].file_name) << " size: " << ret[i].file_size << endl;
	}
	Py_DECREF(pResult);
	Py_DECREF(dict);
	return ret;
}
static void sync_download_file(PyObject* pDict,const string& file_name)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"sync_download_file");
	if(!pFunc) cerr << "Can't find sync_download_file in the module" << endl;
	PyObject* result = PyObject_CallFunction(pFunc,"s",file_name.c_str());
	if(result) Py_DECREF(result);
}
static void sync_upload_file(PyObject* pDict,const string& file_name)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"sync_upload_file");
	if(!pFunc) cerr << "Can't find sync_upload_file in the module" << endl;
	PyObject* result = PyObject_CallFunction(pFunc,"s",file_name.c_str());
	if(result) Py_DECREF(result);
}

static int create_file(PyObject* pDict,const string& file_name)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"create_file");
	if(!pFunc) cerr << "Can't find create_file in the module" << endl;
	PyObject* result = PyObject_CallFunction(pFunc,"si",file_name.c_str(),1);
	int ret = 0;
	PyArg_Parse(result,"i",&ret);
	if(result) Py_DECREF(result);
	return ret;
}

static int open_file(PyObject* pDict,const string& file_name,const string& mode)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"open_file");
	if(!pFunc) cerr << "Can't find open_file in the module" << endl;
	PyObject* result = PyObject_CallFunction(pFunc,"ssi",file_name.c_str(),mode.c_str(),1);
	int ret = 0;
	PyArg_Parse(result,"i",&ret);
	if(result) Py_DECREF(result);
	return ret;
}

static int del_file(PyObject* pDict, const string& file_name)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"del_file");
	if(!pFunc) cerr << "Can't find del_file in the module" << endl;
	PyObject* result = PyObject_CallFunction(pFunc,"s",file_name.c_str());
	int ret = 0;
	PyArg_Parse(result,"i",&ret);
	if(result) Py_DECREF(result);
	return ret;
}

static int close_file(PyObject* pDict, const string& file_name)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"close_file");
	if(!pFunc) cerr << "Can't find close_file in the module" << endl;
	PyObject* result = PyObject_CallFunction(pFunc,"ssi","",file_name.c_str(),1);
	int ret = 0;
	PyArg_Parse(result,"i",&ret);
	if(result) Py_DECREF(result);
	return ret;
}

static void make_dirty(PyObject* pDict,const char* file_name,int start,int size)
{
	PyObject* pFunc = PyDict_GetItemString(pDict,"make_dirty");
	if(!pFunc) cerr << "Can't find make_dirty in the module" << endl;
	PyObject* result = PyObject_CallFunction(pFunc,"sii",file_name,start,size);
	if(result) Py_DECREF(result);
}

// THe implementation of the class functions
StoreEngine* StoreEngine::_instance;

void StoreEngine::Init(int argc,char** argv)
{
	Py_Initialize();
	PySys_SetArgv(argc,argv);	
	PyRun_SimpleString("import sys");
	string cmd = "sys.path.append('" + string(SCRIPT_DIR) + "')";
	PyRun_SimpleString(cmd.c_str());
	chdir(SCRIPT_DIR);
	
	// get the module of cache_client
	PyObject* pName = PyString_FromString("cache_client");
	_pModule = PyImport_Import(pName);
	if(!_pModule) {
		cout << "get module failed!" << endl;
	}
	_pDict = PyModule_GetDict(_pModule);
	_root_dir = get_root_dir();
}

void StoreEngine::DeInit()
{
	//clean ups
	Py_DECREF(_pDict);
	Py_DECREF(_pModule);
	Py_Finalize();
}

void StoreEngine::Mount()
{
	::Mount(_pDict);
}
void StoreEngine::force_update()
{
	::force_update(_pDict);
}
std::vector<FileSize> StoreEngine::list_all_files()
{
	return ::list_all_files(_pDict,false);
}
int StoreEngine::create_file(const std::string& file_name)
{
	return ::create_file(_pDict,file_name);
}
FILE* StoreEngine::open_file(const std::string& file_name, const std::string& mode)
{
	int ret = ::open_file(_pDict,file_name,mode);
	if(ret != 0) return NULL;
	string new_file_name = _root_dir + file_name;
	FILE* f = fopen(new_file_name.c_str(),mode.c_str());
	return f;
}
int StoreEngine::del_file(const std::string& file_name)
{
	return ::del_file(_pDict,file_name);
}
int StoreEngine::close_file(FILE* file, const std::string& file_name)
{
	fclose(file);
	return ::close_file(_pDict,file_name);
}
StoreEngine* StoreEngine::get_instance()
{
	if(_instance == NULL)
		_instance = new StoreEngine();
	return _instance;
}
// Private functions
void StoreEngine::sync_download_file(const std::string& file_name)
{
	::sync_download_file(_pDict,file_name);
}
void StoreEngine::sync_upload_file(const std::string& file_name)
{
	::sync_upload_file(_pDict,file_name);
}
void StoreEngine::make_dirty(const std::string& file_name, int start, int size)
{
	::make_dirty(_pDict,file_name.c_str(),start,size);
}
std::string StoreEngine::get_root_dir()
{
	return ::get_root_dir(_pDict);
}
void StoreEngine::remove_local(const string& file_name)
{
	string new_file_name = _root_dir + file_name;
	::unlink(new_file_name.c_str());	
}
int main(int argc,char** argv)
{	
	StoreEngine* se = StoreEngine::get_instance();
	se->Init(argc,argv);
	se->Mount();
	se->del_file("momoda");
	se->create_file("momoda");

	FILE* ff = se->open_file("momoda","w");
	char buffer[2048];
	for(int i = 0; i < 1023; i++) buffer[i] = 'A';
	for(int i = 1023; i < 1027; i++) buffer[i] = 'B';
	for(int i = 1027; i < 2048; i++) buffer[i] = 'C';
	fwrite(buffer,2048,1,ff);
	se->make_dirty("momoda",0,2048);
	se->close_file(ff,"momoda");

	se->remove_local("momoda");
	FILE* ff2 = se->open_file("momoda","r");
	char buffer2[2048];
	fread(buffer2,2048,1,ff2);
	for(int i = 0; i < 1023; i++) assert(buffer2[i] == 'A');
	for(int i = 1023; i < 1027; i++) assert(buffer2[i] == 'B');
	for(int i = 1027; i < 2048; i++) assert(buffer2[i] == 'C');
	se->make_dirty("momoda",0,2048);
	se->close_file(ff2,"momoda");	
	se->DeInit();
	delete se;	
	return 0;
}