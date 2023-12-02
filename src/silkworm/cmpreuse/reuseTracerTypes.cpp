#include "reuseTracerTypes.hpp"
#include <evmc/evmc.hpp>
#include <intx/intx.hpp>

namespace silkworm {
void Variable::MarkGuardedConst() {
    guardedConstant = true;
    cachedName = "";
}
void Variable::MarkConst() { constant = true; }
bool Variable::IsConst() { return constant || guardedConstant; }
std::string Variable::Name() {
    if (cachedName != "")
        return cachedName;
    std::string prefix = "";
    if (constant)
        prefix += "c";
    else if (guardedConstant)
        prefix += "g";
    else
        prefix += "v";
    std::string name = prefix + std::to_string(id);
    if (customNamePart != "")
        name.append("_" + customNamePart);
    cachedName = name;
    return name;
}
evmc::bytes32 *Variable::Bytes32() {
    return ((MultiTypedValue *)val)->GetBytes32();
}
bool Variable::Bool() { return (bool)val; }
std::string Variable::GetValAsString() {
    return *static_cast<std::string *>(val);
}
evmc::address Variable::BAddress() {
    return ((MultiTypedValue *)val)->GetAddress();
}
int Variable::Int() { return *(int *)val; }
uint64_t Variable::Uint64() { return *(uint64_t *)val; }
int64_t Variable::Int64() { return *(int64_t *)val; }

std::string LimitStringLength(std::string vs, int lenLimit) {
    std::string res = "";
    res.append(vs);
    if ((int)vs.size() > lenLimit) {
        int l = lenLimit / 2;
        int r = lenLimit - l;
        res = vs.substr(0, l) + " ... " + vs.substr(vs.size() - r - 1);
    }
    return res;
}

std::string MVtypeToString(MVType m) {
    switch (m) {
    case MVType::VTBytes32:
        return "Bytes32";
    case MVType::VTAddress:
        return "Address";
    default:
        std::cout << "Error: Unknown type." << std::endl;
        exit(-1);
    }
    return "";
}

evmc::bytes32 *AddressToBytes32(evmc::address *addr) {
    uint64_t tmp = 0;
    for (int i = 0; i < 8; i++)
        tmp = (tmp << 8) + static_cast<uint64_t>((*addr).bytes[12 + i]);
    evmc::bytes32 tmp_ = static_cast<evmc::bytes32>(tmp);
    return &tmp_;
}

evmc::bytes32 *MultiTypedValue::GetBytes32() {
    switch (Typeid) {
    case VTBytes32:
        if (Bytes32 == nullptr) {
            std::cout << "Error: Nil bytes32." << std::endl;
            exit(-1);
        }
        break;
    case VTAddress:
        if (Bytes32 == nullptr) {
            Bytes32 = AddressToBytes32(Addr);
        }
        break;
    default:
        std::cout << "Error: Get Bytes32 from " + MVtypeToString(Typeid) + "."
                  << std::endl;
        exit(-1);
    }
    return Bytes32;
}

evmc::address MultiTypedValue::GetAddress() {
    switch (Typeid) {
    case VTAddress:
        if (Addr == nullptr) {
            std::cout << "Error: Nil addr." << std::endl;
            exit(-1);
        }
        break;
    case VTBytes32:
        if (Addr == nullptr) {
            uint64_t tmp_ = 0;
            for (int i = 0; i < 8; i++) {
                tmp_ = (tmp_ << 8) +
                       static_cast<uint64_t>((*Bytes32).bytes[24 + i]);
            }
            evmc::address Addr_ = static_cast<evmc::address>(tmp_);
            Addr = &Addr_;
        }
        break;
    default:
        std::cout << "Error: Get Address from " + MVtypeToString(Typeid) + "."
                  << std::endl;
        exit(-1);
    }
    return *Addr;
}

} // namespace silkworm