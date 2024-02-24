#include <variant>
#include <functional>
#include "./evm_transaction.hpp"

namespace spectrum {

class Workload {

    public:
    virtual Transaction Next() = 0;
    virtual void SetEVMType(EVMType ty);

};

class Smallbank: Workload {

    private:
    std::vector<uint8_t> code;
    
    public:
    Transaction Next();
    void SetEVMType(EVMType ty);

};

} // namespace spectrum
