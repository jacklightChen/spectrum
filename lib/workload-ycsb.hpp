#include "workload.hpp"
#include "random.hpp"

namespace spectrum {

class YCSB: public Workload {

    private:
    std::basic_string<uint8_t>  code;
    EVMType                     evm_type;
    std::unique_ptr<Random>     rng;

    public:
    YCSB(size_t num_elements, double zipf_exponent);
    Transaction Next() override;
    void SetEVMType(EVMType ty) override;

};

};