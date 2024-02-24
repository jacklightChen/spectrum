#include <variant>
#include <functional>
#include "./evm_transaction.hpp"

namespace spectrum {

class Workload {

    public:
    virtual Transaction Next() = 0;
    virtual void SetEVMType(EVMType ty);

};

class Smallbank: virtual public Workload {

    private:
    std::vector<uint8_t> code;
    
    public:
    Transaction Next() override;
    void SetEVMType(EVMType ty) override;

};

class TPCC: virtual public Workload {

    private:
    std::vector<uint8_t> code;

    public:
    Transaction Next() override;
    void SetEVMType(EVMType ty) override;

};

} // namespace spectrum
