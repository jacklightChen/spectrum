#include <spectrum/workload/abstraction.hpp>
#include <spectrum/common/random.hpp>

namespace spectrum {

class TPCC: public Workload {

    private:
    std::basic_string<uint8_t>  code;
    EVMType                     evm_type;
    std::unique_ptr<Random>     rng;
    size_t                      scale_factor;
    size_t                      num_warehouses;
    std::atomic<bool>           is_first_transaction;
    std::basic_string<uint8_t> CreateTable();
    std::basic_string<uint8_t> Payment();
    std::basic_string<uint8_t> NewOrder();

    public:
    TPCC(size_t scale_factor, size_t num_warehouses);
    Transaction Next() override;
    void SetEVMType(EVMType ty) override;

};

} // namespace spectrum