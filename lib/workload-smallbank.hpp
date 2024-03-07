#include "workload.hpp"

namespace spectrum {

class Smallbank: public Workload {

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

};