#include    <ctype.h>
#include    "utils/log.h"
#include	"utils/encode_util.h"

#define __ -1

/* 用于提取低位bits的过滤元 */
static const unsigned char LOW[] = {0x0, 0x1, 0x3, 0x7, 0xF, 0x1F, 0x3F};

int base64_encode(void* void_out, int *outlen, const void* void_in, int inlen, const char *base64char)
{
    unsigned char *out = (unsigned char *)void_out;
    const unsigned char *in = (const unsigned char *)void_in;

    if (out == NULL ||outlen == NULL || in == NULL || inlen <=0)
    {
        P_WARNING("Arguments error: out=[%p] outlen=[%p] in=[%p] inlen=[%d]",
                out, outlen, in, inlen);
        return -1;
    }

    int len = inlen * 8 / 6 + (inlen % 3 > 0);
    if (len + 1 > *outlen)
    {
        P_WARNING("out buf is over flow. size=[%d] require len=[%d]", *outlen, len + 1);
        return -1;
    }

    *outlen = len;
    memset(out, 0, (size_t) (*outlen + 1));

    char b;
    int l = *outlen;
    int n = 0;

    while (l-- > 0)
    {
        b = 0;
        if (n > 0)
        {
            b |= ((*in) & LOW[n]) << (6 - n);
            in++;
        }
        n = 6 - n;
        if (n > 0 && inlen > 0)
        {
            b |= (*in) >> (8 - n);
            n = 8 - n;
        }
        *out = base64char[b];
        out++;
    }

    return 0;
}

int base64_decode(void* void_out, int *outlen, const void* void_in, int inlen, const int *base64val)
{
    unsigned char *out = (unsigned char *)void_out;
    const unsigned char *in = (const unsigned char *)void_in;

    if (out == NULL || outlen == NULL || in == NULL || inlen <=0)
    {
        P_WARNING("Arguments error: out=[%p] outlen=[%p] in=[%p] inlen=[%d]",
                out, outlen, in, inlen);
        return -1;
    }

    if (inlen < 0 || (inlen == 1 && *in != 0) || (inlen * 6) % 8 >= 6)
    {
        P_WARNING("Invalid in buf for base64 decode.");
        return -1;
    }

    if (inlen * 6 / 8 + 1 > *outlen)
    {
        P_WARNING("Out buf is over flow. size=[%d] require len=[%d]", *outlen, inlen * 6 / 8 + 1);
        return -1;
    }

    for (int i = 0; i < inlen; i++)
    {
        if ((base64val[*(in + i)]) < 0)
        {
            P_WARNING("Invalid base64 content[%d].", *(in + i));
            return -1;
        }
    }

    *outlen = inlen * 6 / 8;
    memset(out, 0, (size_t) (*outlen + 1));

    int l = *outlen;
    int n = 0;
    while (l-- > 0)
    {
        if (n > 0)
        {
            *out |= (base64val[*in] & LOW[n]) << (8 - n);
            in++;
        }
        n = 8 - n;
        if (n >= 6)
        {
            *out |= base64val[*in] << (n - 6);
            n -= 6;
            in++;
        }
        if (n > 0)
        {
            *out |= base64val[*in] >> (6 - n);
            n = 6 - n;
        }
        out++;
    }

    return 0;
}

static const char STD_BASE64CHAR[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
static const int STD_BASE64VAL[] =
{
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, 62, __, __, __, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, __, __, __,  0, __, __,
    __,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, __, __, __, __, __,
    __, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
    __, __, __, __, __, __, __, __, __, __, __, __, __, __, __, __,
};

int std_base64_encode(void* void_out, int *outlen, const void* void_in, int inlen)
{
    return base64_encode(void_out, outlen, void_in, inlen, STD_BASE64CHAR);
}

int std_base64_decode(void* void_out, int *outlen, const void* void_in, int inlen)
{
    return base64_decode(void_out, outlen, void_in, inlen, STD_BASE64VAL);
}

int url_decode(char *out, int *outlen, const char *in)
{
    if (out == NULL || outlen == NULL || in == NULL )
    {
        P_WARNING("Arguments error: out=[%p] outlen=[%p] in=[%p]",
                out, outlen, in);
        return -1;
    }
    if ( *outlen < 0 )
    {
        P_WARNING("Arguments error: outlen[%d].", *outlen);
        return -1;
    }
    const char *const des_beg = out;
    const char *const des_end = out + *outlen;
    const char *const src_beg = in;

    static const char *const HEX = "0123456789ABCDEF";
    const char *p;

    char ch;
    int val;
    while ( *in )
    {
        if ( '+' == *in )
        {
            if ( out >= des_end )
            {
                goto TRUNC;
            }
            *out++ = ' ';
        }
        else if ( '%' == *in )
        {
            ++in;
            ch = ::toupper(*in);
            p = ::strchr(HEX, ch);
            if ( NULL == p )
            {
                P_WARNING("invalid format[%s].", src_beg);
                return -1;
            }
            val = (p - HEX);
            val <<= 4;

            ++in;
            ch = ::toupper(*in);
            p = ::strchr(HEX, ch);
            if ( NULL == p )
            {
                P_WARNING("invalid format[%s].", src_beg);
                return -1;
            }
            val |= (p - HEX);

            if ( out >= des_end )
            {
                goto TRUNC;
            }
            *out++ = val;
        }
        else if ( out >= des_end )
        {
            goto TRUNC;
        }
        else
        {
            *out++ = *in;
        }
        ++in;
    }
    if ( out >= des_end )
    {
        goto TRUNC;
    }
    *out = '\0';

    *outlen = out - des_beg;
    return 0;
TRUNC:
    P_WARNING("not enough buffer[%s], outlen[%d].", src_beg, *outlen);
    return -1;
}
