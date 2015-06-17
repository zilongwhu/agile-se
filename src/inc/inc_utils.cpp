#include	<errno.h>
#include    "inc/inc_utils.h"

namespace inc
{
    bool parseInt32(const char *str, int32_t &val, bool strict)
    {
        val = 0;
        if ( NULL == str )
        {
            return false;
        }
        char *endptr = NULL;
        errno = 0;
        long v = ::strtol(str, &endptr, 10);
        if (errno || v > INT32_MAX || v < INT32_MIN || strict && endptr)
        {
            return false;
        }
        val = static_cast<int32_t>(v);
        return true;
    }

    bool parseUInt32(const char *str, uint32_t &val, bool strict)
    {
        val = 0;
        if ( NULL == str )
        {
            return false;
        }
        char *endptr = NULL;
        errno = 0;
        unsigned long v = ::strtoul(str, &endptr, 10);
        if (errno || v > UINT32_MAX || strict && endptr)
        {
            return false;
        }
        val = static_cast<uint32_t>(v);
        return true;
    }

    bool parseInt64(const char *str, int64_t &val, bool strict)
    {
        val = 0;
        if ( NULL == str )
        {
            return false;
        }
        char *endptr = NULL;
        errno = 0;
        long long v = ::strtoll(str, &endptr, 10);
        if (errno || v > INT64_MAX || v < INT64_MIN || strict && endptr)
        {
            return false;
        }
        val = static_cast<int64_t>(v);
        return true;
    }

    bool parseUInt64(const char *str, uint64_t &val, bool strict)
    {
        val = 0;
        if ( NULL == str )
        {
            return false;
        }
        char *endptr = NULL;
        errno = 0;
        unsigned long long v = ::strtoull(str, &endptr, 10);
        if (errno || v > UINT64_MAX || strict && endptr)
        {
            return false;
        }
        val = static_cast<uint64_t>(v);
        return true;
    }

    bool parseFloat(const char *str, float &val, bool strict)
    {
        val = 0;
        if ( NULL == str )
        {
            return false;
        }
        char *endptr = NULL;
        errno = 0;
        val = ::strtof(str, &endptr);
        return !(errno || strict && endptr)
    }

    void split(const std::string& s, const std::string& delim, std::vector<std::string> &elems)
    {
        elems.clear();

        size_t pos = 0;
        size_t len = s.length();
        size_t delim_len = delim.length();
        if (delim_len == 0) return;
        while (pos < len)
        {
            std::string::size_type find_pos = s.find(delim, pos);
            if (find_pos == std::string::npos)
            {
                elems.push_back(s.substr(pos, len - pos));
                return;
            }
            elems.push_back(s.substr(pos, find_pos - pos));
            pos = find_pos + delim_len;
        }
        elems.push_back(std::string());
        return;
    }

    int readline(FILE *fp, char *line, int num, bool ignoreRest)
    {
        if (NULL == fp || NULL == line || num <= 0)
        {
            return -1;
        }
        off_t pos = ::ftello(fp);
        if (-1 == pos)
        {
            P_WARNING("ftello error[%d].", errno);
            return -1;
        }
        if (::fgets(line, num, fp) == NULL)
        { /* no data is read */
            return 0;
        }
        int len = ::strlen(line);
        if ('\n' == line[len - 1])
        { /* read a line */
            return len;
        }
        /* part of line is read */
        int cnt = 0;
        int ch;
        while (1)
        {
            ch = ::fgetc(fp);
            ++cnt;
            if (EOF == ch)/* end of file */
            {
                /* uncomplete line, undo read */
                cnt = -len; /* play a trick, should return 0 */
                break;
            }
            else if ('\n' == ch)/* end of line */
            {
                if (ignoreRest)
                {
                    return len + cnt; /* tell caller real line size */
                }
                /* buffer is not large enough, undo read and tell caller */
                break;
            }
            else if (ch < 0 || ch > 0xFF)/* error */
            {
                P_WARNING("fgetc return %d, error[%d].", ch, errno);
                return -1;
            }
        }
        if (::fseeko(fp, pos, SEEK_SET) < 0)
        {
            P_WARNING("fseeko error[%d].", errno);
            return -1;
        }
        /* uncomplete line is read, undo */
        return len + cnt;
    }

    char *rtrim_str(char *str, size_t *plen)
    {
        if ( NULL == str )
        {
            return NULL;
        }
        char ch;
        size_t len = ::strlen(str);
        while ( len > 0 )
        {
            ch = str[len - 1];
            if ( '\r' == ch || '\n' == ch )
            {
                --len;
            }
            else
            {
                break;
            }
        }
        str[len] = '\0';
        if ( NULL != plen )
        {
            *plen = len;
        }
        return str;
    }

    char *ltrim_str(char *str, size_t *plen)
    {
        if ( NULL == str )
        {
            return NULL;
        }
        size_t i;
        for ( i = 0; str[i]; ++i )
        {
            if ( '\r' != str[i] && '\n' != str[i] )
            {
                break;
            }
        }
        if ( 0 == i )
        {
            if ( NULL != plen )
            {
                *plen = ::strlen(str);
            }
            return str;
        }
        size_t j;
        for ( j = 0; str[i]; ++i, ++j )
        {
            str[j] = str[i];
        }
        if ( NULL != plen )
        {
            *plen = j;
        }
        return str;
    }

    char *rtrim_str2(char *str, size_t *plen)
    {
        if ( NULL == str )
        {
            return NULL;
        }
        size_t len = ::strlen(str);
        while ( len > 0 )
        {
            if ( ::isspace(str[len - 1]) )
            {
                --len;
            }
            else
            {
                break;
            }
        }
        str[len] = '\0';
        if ( NULL != plen )
        {
            *plen = len;
        }
        return str;
    }

    char *ltrim_str2(char *str, size_t *plen)
    {
        if ( NULL == str )
        {
            return NULL;
        }
        size_t i;
        for ( i = 0; str[i]; ++i )
        {
            if ( ::isspace(str[i]) == 0 )
            {
                break;
            }
        }
        if ( 0 == i )
        {
            if ( NULL != plen )
            {
                *plen = ::strlen(str);
            }
            return str;
        }
        size_t j;
        for ( j = 0; str[i]; ++i, ++j )
        {
            str[j] = str[i];
        }
        if ( NULL != plen )
        {
            *plen = j;
        }
        return str;
    }

    char *trim_str(char *str, size_t *plen)
    {
        return ltrim_str(rtrim_str(str), plen);
    }

    char *trim_str2(char *str, size_t *plen)
    {
        return ltrim_str2(rtrim_str2(str), plen);
    }

    char *str2lower(char *str)
    {
        if ( NULL == str )
        {
            return NULL;
        }
        char *head = str;

        while (*str)
        {
            *str = ::tolower(*str);
            ++str;
        }
        return head;
    }

    char *str2upper(char *str)
    {
        if ( NULL == str )
        {
            return NULL;
        }
        char *head = str;

        while (*str)
        {
            *str = ::toupper(*str);
            ++str;
        }
        return head;
    }
}
