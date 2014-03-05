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

#include "forward_index.h"

std::map<std::string, FieldParser_creater> g_field_parsers;

int ForwardIndex::init(const char *path, const char *file)
{
    return 0;
}

int ForwardIndex::get_offset_by_name(const char *name) const
{
    return 0;
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
