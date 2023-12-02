#include "tracerTypes.hpp"

namespace silkworm {
void SimpleTryLock::Lock() {
    mtx.lock();
    locked = true;
}
void SimpleTryLock::Unlock() {
    locked = false;
    mtx.unlock();
}
bool SimpleTryLock::TryLock() { return locked; }
} // namespace silkworm