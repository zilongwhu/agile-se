#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#include <stdint.h>
#include <stdlib.h>
#include <errno.h>
#include <string>
#include <vector>
#include "str_utils.h"
#include "file_watcher.h"
#include "meminfo.h"
#include "cJSON.h"
#include "log_utils.h"
#include "index/index.h"

namespace inc
{
    enum { OP_INSERT = 0, OP_DELETE = 1, OP_UPDATE = 2 };
    void *das_processor(void *args)
    {
        Index &idx = *(Index *)args; 
        IncReader &reader = idx.m_inc_reader;

        const size_t log_buffer_length = 4096;
        char *log_buffer = new char[log_buffer_length];
    
        char *line = reader._line_buf;
        uint32_t now = g_now_time;

        uint32_t update_forward_count = 0;
        uint32_t update_invert_count = 0;
        uint32_t delete_count = 0;

        std::vector<std::string> columns;
        std::vector<forward_data_t> fields;
        std::vector<invert_data_t> inverts;
        while (1)
        {
            if (now != g_now_time)
            {
                update_forward_count = 0;
                update_invert_count = 0;
                delete_count = 0;

                idx.recycle();
                idx.try2merge();
                idx.try2dump();
                idx.try_exc_cmd();
                idx.try_print_meta();
                idx.try_print_list();

                long check_count = 0;
                while (1)
                {
                    Meminfo mi;
                    if (mi.free() + mi.buffers() + mi.cached() <= long(mi.total() * 0.1))
                    {
                        P_WARNING("memory panic, free: %ld KB, buffers: %ld KB, cached: %ld KB, total: %ld KB",
                                mi.free(), mi.buffers(), mi.cached(), mi.total());
                        if (check_count++ % 360 == 0) { /* log fatal every 3 minutes */
                            P_FATAL("incthread is suspending now");
                        }
                        ::usleep(500*1000); /* sleep 500ms */
                    }
                    else
                    {
                        break;
                    }
                }
            }
            int ret = reader.next();
            if (0 == ret) {
                if (idx.is_base_mode())
                {
                    P_WARNING("meeting end of base file");
                    idx.dump();

                    delete [] log_buffer;
                    return NULL;
                }
                ::usleep(1000); /* sleep 1 ms */
            } else if (1 == ret) {
                reader.split(columns, "\t");
                if (columns.size() < 3)
                {
                    P_MYLOG("must has {[eventid] level, optype, oid}");
                    continue;
                }
                uint32_t level = 0;
                uint32_t optype = 0;
                uint64_t oid = 0;
                if (!parseUInt32(columns[0], level)
                        || !parseUInt32(columns[1], optype)
                        || !parseUInt64(columns[2], oid))
                {
                    P_MYLOG("invalid level, optype, oid");
                    continue;
                }
                LevelIndex *lx = idx.get_level_index(level);
                if (NULL == lx)
                {
                    P_MYLOG("invalid level=%u, LevelIndex is NULL", level);
                    continue;
                }
                fields.clear();
                inverts.clear();
                switch (optype)
                {
                    case OP_INSERT:
                    case OP_UPDATE:
                        if (columns.size() <= 4) {
                            cJSON *fjson = NULL;
                            if (NULL == (fjson = parse_forward_json(columns[3], fields))) {
                                P_MYLOG("failed to parse forward json");
                            } else {
                                lx->forward_update(oid, fields);
                                P_TRACE("%s forward ok, level=%u, oid=%lu",
                                        OP_UPDATE == optype ? "update": "insert", level, oid);
                                ++update_forward_count;
                            }
                            cJSON_Delete(fjson);
                        } else if (columns.size() <= 5) {
                            cJSON *fjson = NULL;
                            cJSON *ijson = NULL;
                            if (NULL == (fjson = parse_forward_json(columns[3], fields))) {
                                P_MYLOG("failed to parse forward json");
                            } else if (NULL == (ijson = parse_invert_json(columns[4], inverts))) {
                                P_MYLOG("failed to parse invert json");
                            } else {
                                lx->update(oid, fields, inverts);
                                P_TRACE("%s ok, level=%u, oid=%lu",
                                        OP_UPDATE == optype ? "update": "insert", level, oid);
                                ++update_invert_count;
                            }
                            cJSON_Delete(fjson);
                            cJSON_Delete(ijson);
                        }
                        break;
                    case OP_DELETE:
                        lx->remove(oid);
                        P_TRACE("delete ok, level=%u, oid=%lu", level, oid);
                        ++delete_count;
                        break;
                    defalut:
                        P_MYLOG("invalid optype=%u", optype);
                        break;
                };
            } else if (ret < 0) {
                P_FATAL("disk file error");
                break;
            }
        }
        delete [] log_buffer;
        return NULL;
    }
}
