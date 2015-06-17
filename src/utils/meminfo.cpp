#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include "utils/meminfo.h"

Meminfo::Meminfo()
{
    m_total = 0;
    m_free = 0;
    m_buffers = 0;
    m_cached = 0;

    FILE *fp = ::fopen("/proc/meminfo", "r");
    if (fp)
    {
        char line[1024];
        while (::fgets(line, sizeof(line), fp))
        {
            char *ptr = ::strpbrk(line, "0123456789");
            if (NULL == ptr)
            {
                continue;
            }
            long v = ::strtol(ptr, NULL, 10);
            if (::strncasecmp("MemTotal:", line, sizeof("MemTotal:") - 1) == 0)
            {
                m_total = v;
            }
            else if (::strncasecmp("MemFree:", line, sizeof("MemFree:") - 1) == 0)
            {
                m_free = v;
            }
            else if (::strncasecmp("Buffers:", line, sizeof("Buffers:") - 1) == 0)
            {
                m_buffers = v;
            }
            else if (::strncasecmp("Cached:", line, sizeof("Cached:") - 1) == 0)
            {
                m_cached = v;
            }
        }
        ::fclose(fp);
    }
}
