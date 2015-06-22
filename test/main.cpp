#include "index/index.h"

int main(int argc, char *argv[])
{
    Index idx;
    idx.init("./conf", "index.conf");
    return 0;
}
