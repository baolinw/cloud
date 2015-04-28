#ifndef _HTTP_LIB_H_
#define _HTTP_LIB_H_

#include <curl/curl.h>
#include <string>
#include <iostream>

// This files contain the http related lib for using in client
// HttpResponse for HTTP request
class HttpResponse
{
public:
	HttpResponse(std::string s);
	std::string get_raw_str() {
		return response_str_;
	}
private:
	std::string response_str_;
};

// The client to send/recv HTTP message
#define MASTER_SERVER_URL "http://ec2-52-8-36-241.us-west-1.compute.amazonaws.com:12345/"
class HTTPClient 
{
public:
	static void InitHTTPConnection(std::string master_server_url, bool dbg_mode);
	static HttpResponse Request(std::string url);
private:
	static std::string base_url_;
	static bool dbg_mode_;
private:
	static size_t OnWriteData(void* buffer, size_t size, size_t nmemb, void* lpVoid);
	static int OnDebug(CURL *, curl_infotype itype, char * pData, size_t size, void *);
};

#endif