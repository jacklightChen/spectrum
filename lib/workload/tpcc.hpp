#include <spectrum/workload/abstraction.hpp>

namespace spectrum {

class TPCC: public Workload {

    private:
    std::basic_string<uint8_t>  code;
    EVMType                     evm_type;
    std::unique_ptr<Random>     rng;
    void CreateTable();
    void Payment();
    void NewOrder();

    public:
    TPCC(size_t scale_factor, size_t num_warehouses);
    Transaction Next() override;
    void SetEVMType(EVMType ty) override;

};

} // namespace spectrum