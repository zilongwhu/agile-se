#include "index/index.h"

Index idx;
log_conf_t lc;

int main(int argc, char *argv[])
{
    ::snprintf(lc._path_prefix, sizeof(lc._path_prefix), "./test");
    lc._max_log_length = 4096;
    init_log(&lc);

    init_nbslib();

    idx.init("./conf", "index.conf");
    idx.join();
    return 0;
}
