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
