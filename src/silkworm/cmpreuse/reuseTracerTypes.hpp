#include "reuseTracer.hpp"
#include <cstdint>
#include <string>
namespace silkworm {
struct Variable {
    void *val;
    // Statement *originStatement;
    bool constant;
    bool guardedConstant;
    uint32_t id;
    std::string customNamePart;
    ReuseTracer *tracer;
    std::string cachedName;
    void MarkGuardedConst();
    void MarkConst();
    bool IsConst();
    std::string Name();
    evmc::bytes32 *Bytes32();
    bool Bool();
    std::string GetValAsString();
    evmc::address BAddress();
    int Int();
    uint64_t Uint64();
    int64_t Int64();
    Variable *LoadBalance(evmc::bytes32 *result);
    Variable *LoadNonce(uint64_t *result);
    Variable *LoadExit(bool *result);
    Variable *LoadEmpty(bool *result);
    Variable *LoadCodeHash();
};

std::string LimitStringLength(std::string vs, int lenLimit);

enum MVType { VTBytes32, VTAddress, VTHash };

std::string MVtypeToString(MVType m);

struct MultiTypedValue : Variable {
    evmc::bytes32 *Bytes32;
    evmc::address *Addr;
    evmc::bytes32 *Hash;
    MVType Typeid;
    evmc::bytes32 *GetBytes32();
    evmc::address GetAddress();
    evmc::bytes32 GetHash();
};

evmc::bytes32 *AddressToBytes32(evmc::address *addr);

} // namespace silkworm