#include "./statistics.hpp"

namespace spectrum {

class Protocol {
    virtual void Start() = 0;
    virtual Statistics Stop() = 0;
    virtual Statistics Report() = 0;
};

} // namespace spectrum