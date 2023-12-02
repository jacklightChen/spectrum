
#include <dcc/common/Time.h>

namespace dcc {
  std::chrono::steady_clock::time_point Time::startTime = std::chrono::steady_clock::now();
}