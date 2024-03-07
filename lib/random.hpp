#include <random>

namespace spectrum {

class Zipf {

    private:
    double num_elements;
    double exponent;
    double h_integral_x1;
    double h_integral_num_elements;
    double s;

    public:
    Zipf(size_t num_elements, double exponent);
    size_t Next(std::mt19937& rng);

};

} // namespace spectrum