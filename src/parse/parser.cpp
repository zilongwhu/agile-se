#include <ctype.h>
#include <string.h>
#include <stack>
#include "parse/parser.h"
#include "log_utils.h"

static int8_t g_operator_priority[256];

void init_operator_priority()
{
    g_operator_priority[uint8_t('*')] = 1; /* A*B 表示必须在A中，是否在B中无所谓 */
    g_operator_priority[uint8_t('&')] = 2; /* A&B 表示A与B */
    g_operator_priority[uint8_t('|')] = 3; /* A|B 表示A或B */
    g_operator_priority[uint8_t('-')] = 4; /* A-B 表示A差B */
}

inline bool is_valid_trigger_char(char ch)
{
    return ::strchr("0123456789*&|-()", ch) != NULL;
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
                P_WARNING("invalid char[%c] in query[%s]", ch, query.c_str());
                return -1;
            }
            buffer[len++] = ch;
            if (len >= int(sizeof(buffer)))
            {
                P_WARNING("too long query[%s]", query.c_str());
                return -1;
            }
        }
    }
    buffer[len] = '\0';
    P_TRACE("clean query[%s]", buffer);

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
                else break;
            }
            result.push_back(std::string(buffer + start, i - start));
            P_TRACE("get token[%s]", result[result.size() - 1].c_str());
            last_token = 1;
        }
        else if (ch == '(')
        {
            if (last_token == 1)
            {
                P_WARNING("invalid operator '('");
                return -1;
            }
            op_stack.push(ch);
            P_TRACE("meet operator '('");
            last_token = 0;
            ++i;
        }
        else if (ch == ')')
        {
            if (last_token != 1)
            {
                P_WARNING("invalid operator ')', has nothing in ()");
                return -1;
            }
            P_TRACE("meet operator ')'");
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
                P_TRACE("push back operator[%c]", op);
            }
            if (!matched)
            {
                P_WARNING("unmatched operator ')'");
                return -1;
            }
            last_token = 1;
            ++i;
        }
        else
        {
            if (last_token != 1)
            {
                P_WARNING("operator '%c' must has left value", ch);
                return -1;
            }
            P_TRACE("meet operator[%c]", ch);
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
                P_TRACE("push back operator[%c]", op);
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
            P_WARNING("unmatched operator '('");
            return -1;
        }
        op_stack.pop();
        result.push_back(std::string(&op, 1));
        P_TRACE("push back operator[%c]", op);
    }
    P_TRACE("parsed query[%s] ok", query.c_str());
    for (size_t i = 0; i < result.size(); ++i)
    {
        P_TRACE("parsed tokens[%03d]: %s", int(i), result[i].c_str());
    }
    return 0;
}
