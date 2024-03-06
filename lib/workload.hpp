#pragma once
#include <variant>
#include <functional>
#include <mutex>
#include "./evm_transaction.hpp"

namespace spectrum {

class Workload {

    public:
    virtual Transaction Next() = 0;
    virtual void SetEVMType(EVMType ty) = 0;

};

} // namespace spectrum
