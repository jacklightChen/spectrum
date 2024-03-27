#include <fmt/core.h>
#define GLOG_PREFIX                                                                                \
    google::InstallPrefixFormatter([](std::ostream& s, const google::LogMessage& m, void* data) {  \
        auto color = [&m](){                                                                       \
            switch (m.severity()) {                                                                \
                case 0: return "\e[1;36m";                                                         \
                case 1: return "\e[1;33m";                                                         \
                case 2: return "\e[1;31m";                                                         \
                default: return "\e[0;30m";                                                        \
            }                                                                                      \
        }();                                                                                       \
        s << color << std::setfill(' ') << std::setw(8)  << std::setiosflags(std::ios::left)       \
                << google::GetLogSeverityName(m.severity()) << "\e[0;30m"                          \
          << std::setfill(' ') << std::setw(16) << m.thread_id() << ' '                            \
          << std::setfill(' ') << std::setw(30) << fmt::format("{}:{}", m.basename(), m.line());   \
    });                                                                                            
