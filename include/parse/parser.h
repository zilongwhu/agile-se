#ifndef __AGiLE_SE_PARSER_H__
#define __AGiLE_SE_PARSER_H__

#include <string>
#include <vector>

void init_operator_priority();
int infix2postfix(const std::string &query, std::vector<std::string> &result);

#endif
