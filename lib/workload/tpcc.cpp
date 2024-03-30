#include <SQLiteCpp/SQLiteCpp.h>
#include <spectrum/workload/tpcc.hpp>

namespace spectrum {

TPCC::TPCC(size_t scale_factor, size_t num_warehouses):
    scale_factor{scale_factor},
    num_warehouses{num_warehouses}
{

}

} // namespace spectrum