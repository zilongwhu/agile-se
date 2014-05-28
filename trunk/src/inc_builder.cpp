// =====================================================================================
//
//       Filename:  inc_builder.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/04/2014 02:30:26 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include "inc_utils.h"
#include "inc_builder.h"

int inc_build_index(inc_builder_t *args)
{
    ForwardIndex &forward = *args->forward;
    InvertIndex &invert = *args->invert;
    IncReader &reader = *args->reader;

    const size_t log_buffer_length = 4096;
    char *log_buffer = new char[log_buffer_length];

    char *line = reader._line_buf;
    uint32_t now = g_now_time;
    uint32_t delete_count = 0;
    uint32_t update_invert_count = 0;
    uint32_t update_forward_count = 0;
    uint32_t last_print_time = now;
    uint32_t last_dump_time = now;

    std::vector<invert_data_t> items;
    std::vector<std::pair<std::string, std::string> > kvs;
    while (1)
    {
        if (now != g_now_time)
        {
            P_MYLOG("processe stats: sku_num[%d], update[forward=%u, invert=%u], delete[%u], in[%u ~ %u]",
                    int(invert.docs_num()), update_forward_count, update_invert_count, delete_count, now, g_now_time);

            forward.recycle();
            invert.recycle();

            if (last_print_time + args->print_meta_interval < g_now_time)
            {
                forward.print_meta();
                invert.print_meta();
                invert.print_list_length();
                last_print_time = g_now_time;
            }
            if (last_dump_time + args->dump_interval < g_now_time)
            {
                forward.dump(args->dump_path.c_str());
                invert.dump(args->dump_path.c_str());
                last_dump_time = g_now_time;
            }

            delete_count = 0;
            update_invert_count = 0;
            update_forward_count = 0;
            now = g_now_time;
        }
        int ret = reader.next();
        if (0 == ret)
        {
            ::usleep(10);
        }
        else if (1 == ret)
        {
            std::vector<std::string> fields;
            split(line, "\t", fields);
            if (fields.size() < 4)
            {
                P_MYLOG("must has [eventid, optype, level, oid]");
                continue;
            }
            int32_t optype = ::strtol(fields[1].c_str(), NULL, 10);
            int32_t level = ::strtol(fields[2].c_str(), NULL, 10);
            int32_t oid = ::strtoul(fields[3].c_str(), NULL, 10);
            if (oid % reader.total_partition() != reader.current_partition())
            {
                continue;
            }
            switch (level)
            {
                case FORWARD_LEVEL:
                    if (OP_UPDATE == optype)
                    {
                        if (parse_forward_json(fields[4], kvs) < 0)
                        {
                            P_MYLOG("failed to parse forward json");
                        }
                        else if (!forward.update(oid, kvs))
                        {
                            P_MYLOG("failed to update forward index");
                        }
                        else
                        {
                            TRACE("update forward ok, oid=%d", oid);
                            ++update_forward_count;
                        }
                    }
                    break;
                case INVERT_LEVEL:
                    if (OP_DELETE == optype)
                    {
                        forward.remove(oid);
                        invert.remove(oid);
                        TRACE("delete ok, oid=%d", oid);
                        ++delete_count;
                    }
                    else if (OP_UPDATE == optype)
                    {
                        if (parse_forward_json(fields[4], kvs) < 0)
                        {
                            P_MYLOG("failed to parse forward json");
                        }
                        else if (parse_invert_json(fields[5], items) < 0)
                        {
                            P_MYLOG("failed to parse invert json");
                        }
                        else
                        {
                            if (!forward.update(oid, kvs))
                            {
                                P_MYLOG("failed to update forward index");
                            }
                            else
                            {
                                invert.remove(oid);
                                for (size_t i = 0; i < items.size(); ++i)
                                {
                                    invert.insert(items[i].key.c_str(), items[i].type, oid, items[i].value);
                                }
                                TRACE("update ok, oid=%d", oid);
                                ++update_invert_count;
                            }
                        }
                    }
                    break;
            };
        }
        else if (ret < 0)
        {
            FATAL("disk file error");
            break;
        }
    }
    return -1;
}
