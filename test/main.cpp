#include "index/index.h"
#include "index/const_index.h"

Index idx;
log_conf_t lc;

ConstIndex cidx;

int main(int argc, char *argv[])
{
    ::snprintf(lc._path_prefix, sizeof(lc._path_prefix), "./test");
    lc._max_log_length = 4096;
    init_log(&lc);

    init_nbslib();

    if (1)
    {
        idx.init("./conf", "index.conf");
        idx.join();
    }
    else
    {
    cidx.init("./conf", "index.conf");
    }
    return 0;
}
