// =====================================================================================
//
//       Filename:  parser.cpp
//
//    Description:  
//
//        Version:  1.0
//        Created:  03/08/2014 08:03:56 PM
//       Revision:  none
//       Compiler:  g++
//
//         Author:  Zilong.Zhu (), zilong.whu@gmail.com
//        Company:  edu.whu
//
// =====================================================================================

#include <log.h>
#include <ctype.h>
#include <string.h>
#include <stack>
#include "parser.h"

static int8_t g_operator_priority[256];

void init_operator_priority()
{
    g_operator_priority[uint8_t('&')] = 1;
    g_operator_priority[uint8_t('|')] = 2;
    g_operator_priority[uint8_t('-')] = 3;
}

inline bool is_valid_trigger_char(char ch)
{
    return ::strchr("0123456789&|-()", ch) != NULL;
}

int infix2postfix(const std::string &query, std::vector<std::string> &result)
{
    char buffer[4096];

    result.clear();

    char ch;
    int len = 0;
    for (size_t i = 0; i < query.length(); ++i)
    {
        ch = query.at(i);
        if (::isspace(ch) == 0)
        {
            if (!is_valid_trigger_char(ch))
            {
                WARNING("invalid char[%c] in query[%s]", ch, query.c_str());
                return -1;
            }
            buffer[len++] = ch;
            if (len >= int(sizeof(buffer)))
            {
                WARNING("too long query[%s]", query.c_str());
                return -1;
            }
        }
    }
    buffer[len] = '\0';
    TRACE("clean query[%s]", buffer);

    int start = -1;
    int last_token = 0;
    std::stack<char> op_stack;
    for (int i = 0; i < len; )
    {
        ch = buffer[i];
        if (ch >= '0' && ch <= '9')
        {
            start = i++;
            while (i < len)
            {
                ch = buffer[i];
                if (ch >= '0' && ch <= '9')
                {
                    ++i;
                }
                break;
            }
            result.push_back(std::string(buffer + start, i - start));
            TRACE("get token[%s]", result[result.size() - 1].c_str());
            last_token = 1;
        }
        else if (ch == '(')
        {
            if (last_token == 1)
            {
                WARNING("invalid operator '('");
                return -1;
            }
            op_stack.push(ch);
            TRACE("meet operator '('");
            last_token = 0;
            ++i;
        }
        else if (ch == ')')
        {
            if (last_token != 1)
            {
                WARNING("invalid operator ')', has nothing in ()");
                return -1;
            }
            TRACE("meet operator ')'");
            bool matched = false;
            while (op_stack.size() > 0)
            {
                char op = op_stack.top();
                op_stack.pop();
                if (op == '(')
                {
                    matched = true;
                    break;
                }
                result.push_back(std::string(&op, 1));
                TRACE("push back operator[%c]", op);
            }
            if (!matched)
            {
                WARNING("unmatched operator ')'");
                return -1;
            }
            last_token = 1;
            ++i;
        }
        else
        {
            if (last_token != 1)
            {
                WARNING("operator '%c' must has left value", ch);
                return -1;
            }
            TRACE("meet operator[%c]", ch);
            while (op_stack.size() > 0)
            {
                char op = op_stack.top();
                if (op == '(')
                {
                    break;
                }
                if (g_operator_priority[uint8_t(op)] > g_operator_priority[uint8_t(ch)])
                {
                    break;
                }
                op_stack.pop();
                result.push_back(std::string(&op, 1));
                TRACE("push back operator[%c]", op);
            }
            op_stack.push(ch);
            last_token = 2;
            ++i;
        }
    }
    while (op_stack.size() > 0)
    {
        char op = op_stack.top();
        if (op == '(')
        {
            WARNING("unmatched operator '('");
            return -1;
        }
        op_stack.pop();
        result.push_back(std::string(&op, 1));
        TRACE("push back operator[%c]", op);
    }
    TRACE("parsed query[%s] ok", query.c_str());
    for (size_t i = 0; i < result.size(); ++i)
    {
        TRACE("parsed tokens[%03d]: %s", int(i), result[i].c_str());
    }
    return 0;
}
