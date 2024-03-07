#pragma once
#include "./statistics.hpp"

namespace spectrum {

class Protocol {
    public:
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

} // namespace spectrum