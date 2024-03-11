#ifndef __BASE64_H__
#define __BASE64_H__

#include <string>

void base64_encode_(unsigned char const* bytes_to_encode, size_t in_len, std::string* result);
void base64_decode_(std::string const& encoded_string, std::string* result);

#endif