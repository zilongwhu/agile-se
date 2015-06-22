#include "parse/parser.h"
#include "pool/delaypool.h"
#include "index/index.h"
#include "index/const_index.h"

void init_time_updater();

namespace inc
{
    DECLARE_INC_PROCESSOR(das_processor);
}
DECLARE_BASE_PROCESSOR(default_base_builder);

void init_nbslib()
{
    init_time_updater();
    init_operator_priority();

    REGISTER_INC_PROCESSOR(inc::das_processor);
    REGISTER_BASE_PROCESSOR(default_base_builder);
}
