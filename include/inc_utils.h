// =====================================================================================
//
//       Filename:  inc_utils.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/08/2014 12:22:16 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_INC_UTILS_H__
#define __AGILE_SE_INC_UTILS_H__

#include <stddef.h>
#include <string>
#include <vector>

enum { FORWARD_LEVEL = 2, INVERT_LEVEL = 3, };
enum { OP_INSERT = 0, OP_DELETE = 1, OP_UPDATE = 2 };

struct invert_data_t
{
    int8_t type;
    std::string key;
    std::string value;
};

int parse_forward_json(const std::string &json, std::vector<std::pair<std::string, std::string> > &kvs);
int parse_invert_json(const std::string &json, std::vector<invert_data_t> &kvs);

#endif
