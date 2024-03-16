#pragma once
#include <random>
#include <mutex>

namespace spectrum {

class Random {

    public:
    virtual size_t Next() = 0;
    virtual ~Random() = default;

};

class Unif : public Random {

    private:
    std::mutex      mu;
    std::mt19937    rng;
    std::uniform_int_distribution<size_t>   distribution;

    public:
    Unif(size_t num_elements);
    ~Unif() override = default;
    size_t Next() override;

};

class Zipf : public Random {

    private:
    std::mutex      mu;
    double          num_elements;
    double          exponent;
    double          h_integral_x1;
    double          h_integral_num_elements;
    double          s;
    std::mt19937    rng;

    public:
    Zipf(size_t num_elements, double exponent);
    ~Zipf() override = default;
    size_t Next() override;

};

} // namespace spectrum