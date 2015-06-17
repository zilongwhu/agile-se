#ifndef __AGILE_SE_ENCODE_H__
#define __AGILE_SE_ENCODE_H__

int base64_encode(void* void_out, int *outlen, const void* void_in, int inlen, const char *base64char);
int base64_decode(void* void_out, int *outlen, const void* void_in, int inlen, const int *base64val);

int std_base64_encode(void* void_out, int *outlen, const void* void_in, int inlen);
int std_base64_decode(void* void_out, int *outlen, const void* void_in, int inlen);

int url_decode(char *out, int *outlen, const char *in);

#endif
