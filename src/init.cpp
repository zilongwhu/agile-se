#include "parse/parser.h"
#include "pool/delaypool.h"
#include "index/const_index.h"

void init_time_updater();

DECLARE_BASE_PROCESSOR(default_base_builder);

void init_nbslib()
{
    init_time_updater();
    init_operator_priority();

    REGISTER_BASE_PROCESSOR(default_base_builder);
}
