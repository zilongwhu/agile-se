#ifndef __AGILE_SE_INC_UTILS_H__
#define __AGILE_SE_INC_UTILS_H__

#include	<stdio.h>
#include	<stdint.h>
#include    <string>
#include    <vector>

namespace inc
{
    bool parseInt32(const char *str, int32_t &val, bool strict = false);
    bool parseUInt32(const char *str, uint32_t &val, bool strict = false);
    bool parseInt64(const char *str, int64_t &val, bool strict = false);
    bool parseUInt64(const char *str, uint64_t &val, bool strict = false);
    bool parseFloat(const char *str, float &val, bool strict = false);
    bool parseDouble(const char *str, double &val, bool strict = false);

    inline bool parseInt32(const std::string &str, int32_t &val, bool strict = false)
    {
        return parseInt32(str.c_str(), val, strict);
    }
    inline bool parseUInt32(const std::string &str, uint32_t &val, bool strict = false)
    {
        return parseUInt32(str.c_str(), val, strict);
    }
    inline bool parseInt64(const std::string &str, int64_t &val, bool strict = false)
    {
        return parseInt64(str.c_str(), val, strict);
    }
    inline bool parseUInt64(const std::string &str, uint64_t &val, bool strict = false)
    {
        return parseUInt64(str.c_str(), val, strict);
    }
    inline bool parseFloat(const std::string &str, float &val, bool strict = false)
    {
        return parseFloat(str.c_str(), val, strict);
    }
    inline bool parseDouble(const std::string &str, double &val, bool strict = false)
    {
        return parseDouble(str.c_str(), val, strict);
    }

    int readline(FILE *fp, char *line, int num, bool ignoreRest);
    void split(const std::string& s, const std::string& delim, std::vector<std::string> &elems);

    char *rtrim_str(char *str, size_t *plen = NULL);
    char *ltrim_str(char *str, size_t *plen = NULL);
    char *rtrim_str2(char *str, size_t *plen = NULL);
    char *ltrim_str2(char *str, size_t *plen = NULL);
    char *trim_str(char *str, size_t *plen = NULL);
    char *trim_str2(char *str, size_t *plen = NULL);

    char *str2lower(char *str);
    char *str2upper(char *str);
}

#endif
