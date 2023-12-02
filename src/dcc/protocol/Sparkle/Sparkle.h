// We implement Spectrum here, original Sparkle is in the no-partial branch.
#pragma once
#include <dcc/core/Partitioner.h>
#include <dcc/protocol/Sparkle/SparkleTransaction.h>

namespace dcc {

template <class Database>
class Sparkle {
 public:
  using DatabaseType = Database;
  using ContextType = typename DatabaseType::ContextType;
  using TransactionType = SparkleTransaction;

  Sparkle(DatabaseType &db, ContextType &context, Partitioner &partitioner)
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