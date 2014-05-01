// =====================================================================================
//
//       Filename:  parser.h
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/08/2014 08:39:56 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#ifndef __AGILE_SE_PARSER_H__
#define __AGILE_SE_PARSER_H__

#include <string>
#include <vector>

void init_operator_priority();
int infix2postfix(const std::string &query, std::vector<std::string> &result);

#endif
