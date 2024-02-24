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

class Smallbank: virtual public Workload {

    private:
    std::basic_string<uint8_t> code;
    EVMType evm_type;
    std::mutex random_mu;
    uint32_t random_state;
    uint32_t Random();

    public:
    Smallbank();
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
