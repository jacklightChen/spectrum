#include <thread-util.hpp>
#include <thread>
#include <pthread.h>

void PinRoundRobin(unsigned rotate_id) {
    #include <pthread.h>
    unsigned core_id    = rotate_id % std::thread::hardware_concurrency();
    unsigned thread_id  = std::this_thread::get_id();
    cpu_set_t   cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET (core_id, &cpu_set);
    auto rc = pthread_setaffinity_np(
        thread_id.native_handle(),
        sizeof(cpu_set_t), &cpu_set
    );
    if (rc != 0) {
        throw std::runtime_error(fmt::format("cannot pin thread-{} to core-{}", thread_id, core_id));
    }
}