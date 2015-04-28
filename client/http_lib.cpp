#include "http_lib.h"

// Many codes refers to the website: http://blog.csdn.net/huyiyang2010/article/details/7664201
using namespace std;

/* declartions of static varibles */
string HTTPClient::base_url_;
bool HTTPClient::dbg_mode_;

HttpResponse::HttpResponse(string s) {
	response_str_ = s;
}

size_t HTTPClient::OnWriteData(void* buffer, size_t size, size_t nmemb, void* lpVoid)
{
	std::string* str = dynamic_cast<std::string*>((std::string *)lpVoid);
	if( NULL == str || NULL == buffer )
	{
		return -1;
	}
    char* pData = (char*)buffer;
    str->append(pData, size * nmemb);
	return nmemb;
}
int HTTPClient::OnDebug(CURL *, curl_infotype itype, char * pData, size_t size, void *)
{
	if(itype == CURLINFO_TEXT)
	{
		printf("[TEXT]%s\n", pData);
	}
	else if(itype == CURLINFO_HEADER_IN)
	{
		printf("[HEADER_IN]%s\n", pData);
	}
	else if(itype == CURLINFO_HEADER_OUT)
	{
		printf("[HEADER_OUT]%s\n", pData);
	}
	else if(itype == CURLINFO_DATA_IN)
	{
		printf("[DATA_IN]%s\n", pData);
	}
	else if(itype == CURLINFO_DATA_OUT)
	{
		printf("[DATA_OUT]%s\n", pData);
	}
	return 0;
}
void HTTPClient::InitHTTPConnection(string master_server_url, bool dgb_mode)
{
	curl_global_init(CURL_GLOBAL_ALL);
	base_url_ = master_server_url;	
}
HttpResponse HTTPClient::Request(string url)
{
	CURL* curl = curl_easy_init();
	if(NULL == curl) {
		cerr << "Init LibCurl fails!" << endl;
	}
	if(dbg_mode_) {
		curl_easy_setopt(curl, CURLOPT_VERBOSE, 1);
		curl_easy_setopt(curl, CURLOPT_DEBUGFUNCTION, OnDebug);
	}
	string strResponse; // location to store the returned result
	
	curl_easy_setopt(curl, CURLOPT_READFUNCTION, NULL);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HTTPClient::OnWriteData);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, (void *)&strResponse);
	curl_easy_setopt(curl, CURLOPT_URL, (base_url_ + url).c_str());
	curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1);
	curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 3);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 3);
	int res = curl_easy_perform(curl);
	if(res != CURLE_OK) {
		cerr << "Wrong in HTTPClient::Request with url: " << url << " code: " << res << endl;
	}
	curl_easy_cleanup(curl);
	return HttpResponse(strResponse);
}