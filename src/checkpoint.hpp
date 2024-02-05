#pragma once
#include <evmc/evmc.hpp>

namespace spectrum {

class CheckpointVM: public evmc_vm {
  public:
    constexpr CheckpointVM(evmc_vm vm): evmc_vm(vm) {}
    size_t    MakeCheckpoint()          { return 0; }
    void      GotoCheckpoint(size_t id) { return;   }
};

}
