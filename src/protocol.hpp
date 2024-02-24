#include "./statistics.hpp"

namespace spectrum {

class Protocol {
    virtual void Start(size_t n_threads) = 0;
    virtual Statistics Stop() = 0;
};

}