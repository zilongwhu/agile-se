// =====================================================================================
//
//       Filename:  inc_builder.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  05/08/2014 01:07:15 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_INC_BUILDER_H__
#define __AGILE_SE_INC_BUILDER_H__

#include "reader.h"
#include "forward_index.h"
#include "invert_index.h"

struct inc_builder_t
{
    uint32_t dump_interval;
    uint32_t print_meta_interval;
    std::string dump_path;

    ForwardIndex *forward;
    InvertIndex *invert;
    IncReader *reader;
};

int inc_build_index(inc_builder_t *args);

#endif
