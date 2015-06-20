#ifndef __STDC_LIMIT_MACROS
#define __STDC_LIMIT_MACROS
#endif

#include <stdint.h>
#include "inc/inc_builder.h"
#include "str_utils.h"
#include "file_watcher.h"
#include "meminfo.h"

namespace inc
{
    int inc_build_index(inc_builder_t *args)
    {
        Index &idx = *args->idx;
        IncReader &reader = idx.m_inc_reader;

        const size_t log_buffer_length = 4096;
        char *log_buffer = new char[log_buffer_length];
    
        char *line = reader._line_buf;
        uint32_t now = g_now_time;
        uint32_t delete_count = 0;
        uint32_t update_invert_count = 0;
        uint32_t update_forward_count = 0;
    
        std::vector<forward_data_t> fields;
        std::vector<invert_data_t> inverts;
        while (1)
        {
            if (now != g_now_time)
            {
                P_MYLOG("processe stats: doc_num[%d], update[forward=%u, invert=%u], delete[%u], in[%u ~ %u]",
                        (int)idx.doc_num(), update_forward_count, update_invert_count, delete_count,
                        now, g_now_time);
    
                idx.recycle();

                if (args->timer)
                {
                    args->timer(args);
                }

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
                        if (check_count++ % 180 == 0) { /* log fatal every 3 minutes */
                            P_FATAL("incthread is suspending now");
                        }
                        ::sleep(1); /* sleep 1 second */
                    }
                    else
                    {
                        break;
                    }
                }
    
                delete_count = 0;
                update_invert_count = 0;
                update_forward_count = 0;
                now = g_now_time;
            }
            int ret = reader.next();
            if (0 == ret) {
                if (idx.is_base_mode())
                {
                    P_WARNING("meeting end of base file");
                    idx.dump();

                    delete [] log_buffer;
                    return 0;
                }
                ::usleep(1000);
            } else if (1 == ret) {
                std::vector<std::string> columns;
                split(line, "\t", columns);
                if (columns.size() < 4)
                {
                    P_MYLOG("must has [eventid, optype, level, oid]");
                    continue;
                }
                int32_t optype = ::strtol(columns[1].c_str(), NULL, 10);
                int32_t level = ::strtol(columns[2].c_str(), NULL, 10);
                int32_t oid = ::strtoul(columns[3].c_str(), NULL, 10);
                if (oid % reader.total_partition() != reader.current_partition())
                {
                    continue;
                }
                fields.clear();
                inverts.clear();
                ForwardIndex::ids_t ids;
                /*
                switch (level)
                {
                    case FORWARD_LEVEL:
                        if (OP_UPDATE == optype)
                        {
                            cJSON *json = NULL;
                            if (columns.size() < 5) {
                                P_MYLOG("forward update need at least 5 fields");
                            } else if (NULL == (json = parse_forward_json(columns[4], fields))) {
                                P_MYLOG("failed to parse forward json");
                            } else if (!idx.forward_update(oid, fields, &ids)) {
                                P_MYLOG("failed to update forward index");
                            } else if (!idx.update_docid(ids.old_id, ids.new_id)) {
                                P_MYLOG("failed to change docid from %d to %d, oid=%d",
                                        ids.old_id, ids.new_id, oid);
                            } else {
                                P_TRACE("update forward ok, oid=%d", oid);
                                ++update_forward_count;
                            }
                            cJSON_Delete(json);
                        }
                        break;
                    case INVERT_LEVEL:
                        if (OP_DELETE == optype)
                        {
                            int32_t old_id = -1;
                            if (idx.forward_remove(oid, &old_id))
                            {
                                idx.invert_remove(old_id);
                            }
                            P_TRACE("delete ok, oid=%d", oid);
                            ++delete_count;
                        }
                        else if (OP_UPDATE == optype)
                        {
                            cJSON *fjson = NULL;
                            cJSON *ijson = NULL;
                            if (columns.size() < 6) {
                                P_MYLOG("invert update need at least 6 fields");
                            } else if (NULL == (fjson = parse_forward_json(columns[4], fields)) {
                                P_MYLOG("failed to parse forward json");
                            } else if (NULL == (ijson = parse_invert_json(columns[5], inverts)) {
                                P_MYLOG("failed to parse invert json");
                            } else if (!idx.forward_update(oid, fields, &ids)) {
                                P_MYLOG("failed to update forward index");
                            } else {
                                idx.invert_remove(ids.old_id);
                                for (size_t i = 0; i < inverts.size(); ++i)
                                {
                                    idx.invert_insert(inverts[i].key, inverts[i].type,
                                        ids.new_id, inverts[i].value);
                                }
                                P_TRACE("update ok, oid=%d, id=%d", oid, ids.new_id);
                                ++update_invert_count;
                            }
                            cJSON_Delete(fjson);
                            cJSON_Delete(ijson);
                        }
                        break;
                };
                */
            } else if (ret < 0) {
                P_FATAL("disk file error");
                break;
            }
        }
        delete [] log_buffer;
        return -1;
    }
}
