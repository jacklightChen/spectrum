#include "./protocol-aria-fb.hpp"

namespace spectrum 
{

void Aria::ParallelEach(
    std::function<void(AriaTransaction&)>   map, 
    std::vector<AriaTransaction>&           batch
) {
    pool.submit_loop(
        size_t{0}, batch.size(), 
        [&](size_t i) { map(batch[i]); }
    ).wait();
}

void AriaExecutor::Execute(AriaTransaction& tx, AriaTable& table) {
    // read from the public table
    tx.UpdateGetStorageHandler([&](
        const evmc::address &addr,
        const evmc::bytes32 &key
    ) {
        // if some write is issued previously, 
        //  the read dependency will be barricated from journal. 
        auto tup = std::make_tuple(addr, key);
        if (tx.local_put.contains(tup)) {
            return tx.local_put[tup];
        }
        if (tx.local_get.contains(tup)) {
            return tx.local_get[tup];
        }
        auto value = evmc::bytes32{0};
        table.Put(tup, [&](auto& entry){
            value = entry.value;
        });
        tx.local_get[tup] = value;
        return value;
    });
    // write locally to local storage
    tx.UpdateSetStorageHandler([&](
        const evmc::address &addr, 
        const evmc::bytes32 &key,
        const evmc::bytes32 &value
    ) {
        auto tup = std::make_tuple(addr, key);
        tx.local_put[tup] = value;
        return evmc_storage_status::EVMC_STORAGE_MODIFIED;
    });
    // just execute the transaction
    tx.Execute();
}

void AriaExecutor::Reserve(AriaTransaction& tx) {
    // write entries to reservation table

    // for each written entry touched, 
    // transactions with larger id is aborted. 

}

void AriaExecutor::Commit(AriaTransaction& tx) {
    // commit changes when possible
}

} // namespace spectrum