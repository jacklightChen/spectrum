#pragma once
#include <variant>
#include <functional>
#include "./evm_transaction.hpp"

namespace spectrum {

class Workload {

    public:
    virtual Transaction Next() = 0;
    virtual void SetEVMType(EVMType ty) = 0;
    virtual ~Workload() = default;

};

} // namespace spectrum
