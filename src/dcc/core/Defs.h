#pragma once

#include <cstddef>
namespace dcc {

enum class ExecutorStatus {
  START,
  CLEANUP,
  C_PHASE,
  S_PHASE,
  Analysis,
  Execute,
  Aria_READ,
  Aria_COMMIT,
  AriaFB_READ,
  AriaFB_COMMIT,
  AriaFB_Fallback_Prepare,
  AriaFB_Fallback,
  Bohm_Analysis,
  Bohm_Insert,
  Bohm_Execute,
  Bohm_GC,
  Pwv_Analysis,
  Pwv_Execute,
  STOP,
  EXIT
};

enum class TransactionResult { COMMIT, READY_TO_COMMIT, ABORT, ABORT_NORETRY };

#define LIST_EXEC_TYPE \
  X(Exec_Undefined, 0) \
  X(Exec_Serial, 1)    \
  X(Exec_AriaFB, 2)    \
  X(Exec_Sparkle, 3)   \
  X(Exec_Spectrum, 4)

#define X(exec_type, type_val) exec_type = type_val,
enum class ExecType { LIST_EXEC_TYPE };
#undef X

#define TUPLE_LIST_SIZE 200

}  // namespace dcc
