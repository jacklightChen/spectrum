#pragma once
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/Spectrum/SpectrumTransaction.h>

namespace dcc {

template <class Database>
class Spectrum {
 public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using TransactionType = SpectrumTransaction;

  Spectrum(DatabaseType &db, ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {}

  void abort(TransactionType &txn) {
    // nothing needs to be done
  }

  bool commit(TransactionType &txn) { return true; }

 private:
  DatabaseType &db;
  ContextType &context;
  Partitioner &partitioner;
};
}  // namespace dcc