// =====================================================================================
//
//       Filename:  forward_index.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/05/2014 05:15:20 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include <iostream>
#include "configure.h"
#include "forward_index.h"

std::map<std::string, FieldParser_creater> g_field_parsers;

struct FieldConfig
{
    std::string name;
    std::string parser;
    int type;
    int offset;
    int size;
};

int ForwardIndex::init(const char *path, const char *file)
{
    Config config(path, file);
    if (config.parse() < 0)
    {
        return -1;
    }
    int field_num = 0;
    if (!config.get("field_num", field_num) || field_num <= 0)
    {
        return -1;
    }
    int max_size = 0;
    std::vector<FieldConfig> fields;
    for (int i = 0; i < field_num; ++i)
    {
        FieldConfig field;
        char buffer[256];

        ::snprintf(buffer, sizeof(buffer), "field_%d_name", i);
        if (!config.get(buffer, field.name))
        {
            return -1;
        }
        ::snprintf(buffer, sizeof(buffer), "field_%d_type", i);
        std::string type;
        if (!config.get(buffer, type))
        {
            return -1;
        }
        if ("int" == type)
        {
            field.type = INT_TYPE;
            field.size = sizeof(int);
        }
        else if ("float" == type)
        {
            field.type = FLOAT_TYPE;
            field.size = sizeof(float);
        }
        else if ("self_define" == type)
        {
            field.type = SELF_DEFINE_TYPE;
            field.size = sizeof(void *);
        }
        else
        {
            return -1;
        }
        if (SELF_DEFINE_TYPE == field.type)
        {
            ::snprintf(buffer, sizeof(buffer), "field_%d_parser", i);
            if (!config.get(buffer, field.parser))
            {
                return -1;
            }
        }
        ::snprintf(buffer, sizeof(buffer), "field_%d_offset", i);
        if (!config.get(buffer, field.offset))
        {
            return -1;
        }
        if (field.offset < 0 || field.offset % field.size != 0)
        {
            return -1;
        }
        fields.push_back(field);
        if (max_size < field.offset + field.size)
        {
            max_size = field.offset + field.size;
        }
    }
    std::vector<int> placeholder(max_size, -1);
    for (size_t i = 0; i < fields.size(); ++i)
    {
        for (int j = 0; j < fields[i].size; ++j)
        {
            if (placeholder[fields[i].offset + j] != -1)
            {
                return -1;
            }
            placeholder[fields[i].offset + j] = fields[i].offset;
        }
    }
    for (size_t i = 0; i < placeholder.size(); ++i)
    {
        if (placeholder[i] != -1)
        {
            for (size_t j = 0; j < fields.size(); ++j)
            {
                if (fields[j].offset == placeholder[i])
                {
                    std::cerr << "field[" << i << "] = " << fields[j].name << std::endl;
                    break;
                }
            }
        }
    }
    for (size_t i = 0; i < fields.size(); ++i)
    {
        FieldDes fd;

        if (m_fields.find(fields[i].name) != m_fields.end())
        {
            goto FAIL;
        }
        fd.offset = fields[i].offset;
        fd.type = fields[i].type;
        if (SELF_DEFINE_TYPE == fd.type)
        {
            std::map<std::string, FieldParser_creater>::iterator it = g_field_parsers.find(fields[i].name);
            if (it == g_field_parsers.end())
            {
                goto FAIL;
            }
            fd.parser = (*it->second)();
            if (NULL == fd.parser)
            {
                goto FAIL;
            }
        }
        else
        {
            fd.parser = NULL;
        }

        m_fields.insert(std::make_pair(fields[i].name, fd));
    }
    m_info_size = max_size;
    return 0;
FAIL:
    __gnu_cxx::hash_map<std::string, FieldDes>::iterator it = m_fields.begin();
    while (it != m_fields.end())
    {
        if (it->second.parser)
        {
            delete it->second.parser;
        }
        ++it;
    }
    m_fields.clear();
    return -1;
}

int ForwardIndex::get_offset_by_name(const char *name) const
{
    __gnu_cxx::hash_map<std::string, FieldDes>::const_iterator it = m_fields.find(name);
    if (it == m_fields.end())
    {
        return -1;
    }
    return it->second.offset;
}

void *ForwardIndex::get_info_by_id(long id) const
{
    return NULL;
}

bool ForwardIndex::update(long id, const std::vector<std::pair<std::string, std::string> > &kvs)
{
    return true;
}

void ForwardIndex::remove(long id)
{

}
