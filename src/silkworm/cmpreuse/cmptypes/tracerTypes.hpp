#include "evmone/execution_state.hpp"
#include <cstdint>
#include <evmc/evmc.h>
#include <mutex>
#include <string>
#include <vector>

namespace silkworm {
using Bytes = std::basic_string<uint8_t>;
class PrecompiledContract {
  public:
    virtual uint64_t RequiredGas(Bytes &byte);
    virtual void Run(Bytes &byte, std::string &err);
};

class IReuseTracer {
  public:
    virtual uint Snapshot();
    virtual void RevertToSnapshot(uint snapShotID);
    virtual void InitTx(const evmc::address &from, const evmc::address &to,
                        evmc::bytes32 gasPrice, evmc::bytes32 value,
                        uint64_t gas, uint64_t nonce,
                        const silkworm::Bytes &data);
    virtual void TracePreCheck();
    virtual void TraceIncCallerNonce();
    virtual void TraceCanTransfer();
    virtual void TraceAssertCodeAddrExistence();
    virtual void TraceGuardIsCodeAddressPrecompiled();
    virtual void TraceAssertValueZeroness();
    virtual void TraceTransfer();
    virtual void TraceGuardCodeLen();
    virtual void TraceRefund(evmc::bytes32, uint64_t);
    virtual void TraceAssertStackValueZeroness();
    virtual void TraceAssertStackValueSelf(std::vector<int> &);
    virtual void TraceGasSStore();
    virtual void TraceGasSStoreEIP2200();
    virtual void TraceAssertExpByteLen();
    virtual void TraceGasCall();
    virtual void TraceGasCallCode();
    virtual void TraceGasDelegateOrStaticCall();
    virtual void TraceGasSelfdestruct();
    virtual void TraceOpByName(std::string, uint64_t, uint64_t);
    virtual void TraceCallReturn(std::string err, std::string reverted,
                                 bool nilOrEmptyReturnData);
    virtual void TraceCreateReturn(std::string err, bool reverted);
    virtual void TraceCreateAddressAndSetupCode();
    virtual void TraceAssertNoExistingContract();
    virtual void MarkUnimplementedOp();
    virtual void ClearReturnData();
    virtual bool NeedRT();
    virtual void TraceSetNonceForCreatedContract();
    virtual void TraceAssertNewCodeLen();
    virtual void TraceStoreNewCode();
    virtual void TraceAssertInputLen();
    virtual void TraceGasBigModExp();
    virtual void TraceRunPrecompiled(PrecompiledContract p);
    virtual void MarkNotExternalTransfer();
    virtual void MarkCompletedTrace(bool);
    virtual void ResizeMem(uint64_t size);
    virtual int GetStackSize();
    virtual int GetMemSize();
    virtual evmc::bytes32 GetTopStack();
    virtual void TraceLoadCodeAndGuardHash();
    virtual silkworm::Bytes GetReturnData();
    virtual void TraceCreateContractAccount();
};

class SimpleTryLock {
  public:
    SimpleTryLock() { locked = false; }
    bool TryLock();
    void Lock();
    void Unlock();

  private:
    std::mutex mtx;
    bool locked;
};

} // namespace silkworm