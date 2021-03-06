/**
 * Autogenerated by Thrift
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
#ifndef Cassandra_H
#define Cassandra_H

#include <TProcessor.h>
#include "cassandra_types.h"
#include "FacebookService.h"

namespace com { namespace facebook { namespace infrastructure { namespace service {

class CassandraIf : virtual public facebook::fb303::FacebookServiceIf {
 public:
  virtual ~CassandraIf() {}
  virtual void get_slice(std::vector<column_t> & _return, const std::string& tablename, const std::string& key, const std::string& columnFamily_column, const int32_t start, const int32_t count) = 0;
  virtual void get_column(column_t& _return, const std::string& tablename, const std::string& key, const std::string& columnFamily_column) = 0;
  virtual int32_t get_column_count(const std::string& tablename, const std::string& key, const std::string& columnFamily_column) = 0;
  virtual void insert(const std::string& tablename, const std::string& key, const std::string& columnFamily_column, const std::string& cellData, const int32_t timestamp) = 0;
  virtual void batch_insert(const batch_mutation_t& batchMutation) = 0;
  virtual bool batch_insert_blocking(const batch_mutation_t& batchMutation) = 0;
  virtual void remove(const std::string& tablename, const std::string& key, const std::string& columnFamily_column) = 0;
  virtual void get_slice_super(std::vector<superColumn_t> & _return, const std::string& tablename, const std::string& key, const std::string& columnFamily_superColumnName, const int32_t start, const int32_t count) = 0;
  virtual void get_superColumn(superColumn_t& _return, const std::string& tablename, const std::string& key, const std::string& columnFamily) = 0;
  virtual void batch_insert_superColumn(const batch_mutation_super_t& batchMutationSuper) = 0;
  virtual bool batch_insert_superColumn_blocking(const batch_mutation_super_t& batchMutationSuper) = 0;
};

class CassandraNull : virtual public CassandraIf , virtual public facebook::fb303::FacebookServiceNull {
 public:
  virtual ~CassandraNull() {}
  void get_slice(std::vector<column_t> & /* _return */, const std::string& /* tablename */, const std::string& /* key */, const std::string& /* columnFamily_column */, const int32_t /* start */, const int32_t /* count */) {
    return;
  }
  void get_column(column_t& /* _return */, const std::string& /* tablename */, const std::string& /* key */, const std::string& /* columnFamily_column */) {
    return;
  }
  int32_t get_column_count(const std::string& /* tablename */, const std::string& /* key */, const std::string& /* columnFamily_column */) {
    int32_t _return = 0;
    return _return;
  }
  void insert(const std::string& /* tablename */, const std::string& /* key */, const std::string& /* columnFamily_column */, const std::string& /* cellData */, const int32_t /* timestamp */) {
    return;
  }
  void batch_insert(const batch_mutation_t& /* batchMutation */) {
    return;
  }
  bool batch_insert_blocking(const batch_mutation_t& /* batchMutation */) {
    bool _return = false;
    return _return;
  }
  void remove(const std::string& /* tablename */, const std::string& /* key */, const std::string& /* columnFamily_column */) {
    return;
  }
  void get_slice_super(std::vector<superColumn_t> & /* _return */, const std::string& /* tablename */, const std::string& /* key */, const std::string& /* columnFamily_superColumnName */, const int32_t /* start */, const int32_t /* count */) {
    return;
  }
  void get_superColumn(superColumn_t& /* _return */, const std::string& /* tablename */, const std::string& /* key */, const std::string& /* columnFamily */) {
    return;
  }
  void batch_insert_superColumn(const batch_mutation_super_t& /* batchMutationSuper */) {
    return;
  }
  bool batch_insert_superColumn_blocking(const batch_mutation_super_t& /* batchMutationSuper */) {
    bool _return = false;
    return _return;
  }
};

class Cassandra_get_slice_args {
 public:

  Cassandra_get_slice_args() : tablename(""), key(""), columnFamily_column(""), start(-1), count(-1) {
  }

  virtual ~Cassandra_get_slice_args() throw() {}

  std::string tablename;
  std::string key;
  std::string columnFamily_column;
  int32_t start;
  int32_t count;

  struct __isset {
    __isset() : tablename(false), key(false), columnFamily_column(false), start(false), count(false) {}
    bool tablename;
    bool key;
    bool columnFamily_column;
    bool start;
    bool count;
  } __isset;

  bool operator == (const Cassandra_get_slice_args & rhs) const
  {
    if (!(tablename == rhs.tablename))
      return false;
    if (!(key == rhs.key))
      return false;
    if (!(columnFamily_column == rhs.columnFamily_column))
      return false;
    if (!(start == rhs.start))
      return false;
    if (!(count == rhs.count))
      return false;
    return true;
  }
  bool operator != (const Cassandra_get_slice_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_get_slice_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_slice_pargs {
 public:


  virtual ~Cassandra_get_slice_pargs() throw() {}

  const std::string* tablename;
  const std::string* key;
  const std::string* columnFamily_column;
  const int32_t* start;
  const int32_t* count;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_slice_result {
 public:

  Cassandra_get_slice_result() {
  }

  virtual ~Cassandra_get_slice_result() throw() {}

  std::vector<column_t>  success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const Cassandra_get_slice_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const Cassandra_get_slice_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_get_slice_result & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_slice_presult {
 public:


  virtual ~Cassandra_get_slice_presult() throw() {}

  std::vector<column_t> * success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);

};

class Cassandra_get_column_args {
 public:

  Cassandra_get_column_args() : tablename(""), key(""), columnFamily_column("") {
  }

  virtual ~Cassandra_get_column_args() throw() {}

  std::string tablename;
  std::string key;
  std::string columnFamily_column;

  struct __isset {
    __isset() : tablename(false), key(false), columnFamily_column(false) {}
    bool tablename;
    bool key;
    bool columnFamily_column;
  } __isset;

  bool operator == (const Cassandra_get_column_args & rhs) const
  {
    if (!(tablename == rhs.tablename))
      return false;
    if (!(key == rhs.key))
      return false;
    if (!(columnFamily_column == rhs.columnFamily_column))
      return false;
    return true;
  }
  bool operator != (const Cassandra_get_column_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_get_column_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_column_pargs {
 public:


  virtual ~Cassandra_get_column_pargs() throw() {}

  const std::string* tablename;
  const std::string* key;
  const std::string* columnFamily_column;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_column_result {
 public:

  Cassandra_get_column_result() {
  }

  virtual ~Cassandra_get_column_result() throw() {}

  column_t success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const Cassandra_get_column_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const Cassandra_get_column_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_get_column_result & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_column_presult {
 public:


  virtual ~Cassandra_get_column_presult() throw() {}

  column_t* success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);

};

class Cassandra_get_column_count_args {
 public:

  Cassandra_get_column_count_args() : tablename(""), key(""), columnFamily_column("") {
  }

  virtual ~Cassandra_get_column_count_args() throw() {}

  std::string tablename;
  std::string key;
  std::string columnFamily_column;

  struct __isset {
    __isset() : tablename(false), key(false), columnFamily_column(false) {}
    bool tablename;
    bool key;
    bool columnFamily_column;
  } __isset;

  bool operator == (const Cassandra_get_column_count_args & rhs) const
  {
    if (!(tablename == rhs.tablename))
      return false;
    if (!(key == rhs.key))
      return false;
    if (!(columnFamily_column == rhs.columnFamily_column))
      return false;
    return true;
  }
  bool operator != (const Cassandra_get_column_count_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_get_column_count_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_column_count_pargs {
 public:


  virtual ~Cassandra_get_column_count_pargs() throw() {}

  const std::string* tablename;
  const std::string* key;
  const std::string* columnFamily_column;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_column_count_result {
 public:

  Cassandra_get_column_count_result() : success(0) {
  }

  virtual ~Cassandra_get_column_count_result() throw() {}

  int32_t success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const Cassandra_get_column_count_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const Cassandra_get_column_count_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_get_column_count_result & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_column_count_presult {
 public:


  virtual ~Cassandra_get_column_count_presult() throw() {}

  int32_t* success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);

};

class Cassandra_insert_args {
 public:

  Cassandra_insert_args() : tablename(""), key(""), columnFamily_column(""), cellData(""), timestamp(0) {
  }

  virtual ~Cassandra_insert_args() throw() {}

  std::string tablename;
  std::string key;
  std::string columnFamily_column;
  std::string cellData;
  int32_t timestamp;

  struct __isset {
    __isset() : tablename(false), key(false), columnFamily_column(false), cellData(false), timestamp(false) {}
    bool tablename;
    bool key;
    bool columnFamily_column;
    bool cellData;
    bool timestamp;
  } __isset;

  bool operator == (const Cassandra_insert_args & rhs) const
  {
    if (!(tablename == rhs.tablename))
      return false;
    if (!(key == rhs.key))
      return false;
    if (!(columnFamily_column == rhs.columnFamily_column))
      return false;
    if (!(cellData == rhs.cellData))
      return false;
    if (!(timestamp == rhs.timestamp))
      return false;
    return true;
  }
  bool operator != (const Cassandra_insert_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_insert_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_insert_pargs {
 public:


  virtual ~Cassandra_insert_pargs() throw() {}

  const std::string* tablename;
  const std::string* key;
  const std::string* columnFamily_column;
  const std::string* cellData;
  const int32_t* timestamp;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_args {
 public:

  Cassandra_batch_insert_args() {
  }

  virtual ~Cassandra_batch_insert_args() throw() {}

  batch_mutation_t batchMutation;

  struct __isset {
    __isset() : batchMutation(false) {}
    bool batchMutation;
  } __isset;

  bool operator == (const Cassandra_batch_insert_args & rhs) const
  {
    if (!(batchMutation == rhs.batchMutation))
      return false;
    return true;
  }
  bool operator != (const Cassandra_batch_insert_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_batch_insert_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_pargs {
 public:


  virtual ~Cassandra_batch_insert_pargs() throw() {}

  const batch_mutation_t* batchMutation;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_blocking_args {
 public:

  Cassandra_batch_insert_blocking_args() {
  }

  virtual ~Cassandra_batch_insert_blocking_args() throw() {}

  batch_mutation_t batchMutation;

  struct __isset {
    __isset() : batchMutation(false) {}
    bool batchMutation;
  } __isset;

  bool operator == (const Cassandra_batch_insert_blocking_args & rhs) const
  {
    if (!(batchMutation == rhs.batchMutation))
      return false;
    return true;
  }
  bool operator != (const Cassandra_batch_insert_blocking_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_batch_insert_blocking_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_blocking_pargs {
 public:


  virtual ~Cassandra_batch_insert_blocking_pargs() throw() {}

  const batch_mutation_t* batchMutation;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_blocking_result {
 public:

  Cassandra_batch_insert_blocking_result() : success(0) {
  }

  virtual ~Cassandra_batch_insert_blocking_result() throw() {}

  bool success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const Cassandra_batch_insert_blocking_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const Cassandra_batch_insert_blocking_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_batch_insert_blocking_result & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_blocking_presult {
 public:


  virtual ~Cassandra_batch_insert_blocking_presult() throw() {}

  bool* success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);

};

class Cassandra_remove_args {
 public:

  Cassandra_remove_args() : tablename(""), key(""), columnFamily_column("") {
  }

  virtual ~Cassandra_remove_args() throw() {}

  std::string tablename;
  std::string key;
  std::string columnFamily_column;

  struct __isset {
    __isset() : tablename(false), key(false), columnFamily_column(false) {}
    bool tablename;
    bool key;
    bool columnFamily_column;
  } __isset;

  bool operator == (const Cassandra_remove_args & rhs) const
  {
    if (!(tablename == rhs.tablename))
      return false;
    if (!(key == rhs.key))
      return false;
    if (!(columnFamily_column == rhs.columnFamily_column))
      return false;
    return true;
  }
  bool operator != (const Cassandra_remove_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_remove_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_remove_pargs {
 public:


  virtual ~Cassandra_remove_pargs() throw() {}

  const std::string* tablename;
  const std::string* key;
  const std::string* columnFamily_column;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_slice_super_args {
 public:

  Cassandra_get_slice_super_args() : tablename(""), key(""), columnFamily_superColumnName(""), start(-1), count(-1) {
  }

  virtual ~Cassandra_get_slice_super_args() throw() {}

  std::string tablename;
  std::string key;
  std::string columnFamily_superColumnName;
  int32_t start;
  int32_t count;

  struct __isset {
    __isset() : tablename(false), key(false), columnFamily_superColumnName(false), start(false), count(false) {}
    bool tablename;
    bool key;
    bool columnFamily_superColumnName;
    bool start;
    bool count;
  } __isset;

  bool operator == (const Cassandra_get_slice_super_args & rhs) const
  {
    if (!(tablename == rhs.tablename))
      return false;
    if (!(key == rhs.key))
      return false;
    if (!(columnFamily_superColumnName == rhs.columnFamily_superColumnName))
      return false;
    if (!(start == rhs.start))
      return false;
    if (!(count == rhs.count))
      return false;
    return true;
  }
  bool operator != (const Cassandra_get_slice_super_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_get_slice_super_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_slice_super_pargs {
 public:


  virtual ~Cassandra_get_slice_super_pargs() throw() {}

  const std::string* tablename;
  const std::string* key;
  const std::string* columnFamily_superColumnName;
  const int32_t* start;
  const int32_t* count;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_slice_super_result {
 public:

  Cassandra_get_slice_super_result() {
  }

  virtual ~Cassandra_get_slice_super_result() throw() {}

  std::vector<superColumn_t>  success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const Cassandra_get_slice_super_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const Cassandra_get_slice_super_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_get_slice_super_result & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_slice_super_presult {
 public:


  virtual ~Cassandra_get_slice_super_presult() throw() {}

  std::vector<superColumn_t> * success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);

};

class Cassandra_get_superColumn_args {
 public:

  Cassandra_get_superColumn_args() : tablename(""), key(""), columnFamily("") {
  }

  virtual ~Cassandra_get_superColumn_args() throw() {}

  std::string tablename;
  std::string key;
  std::string columnFamily;

  struct __isset {
    __isset() : tablename(false), key(false), columnFamily(false) {}
    bool tablename;
    bool key;
    bool columnFamily;
  } __isset;

  bool operator == (const Cassandra_get_superColumn_args & rhs) const
  {
    if (!(tablename == rhs.tablename))
      return false;
    if (!(key == rhs.key))
      return false;
    if (!(columnFamily == rhs.columnFamily))
      return false;
    return true;
  }
  bool operator != (const Cassandra_get_superColumn_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_get_superColumn_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_superColumn_pargs {
 public:


  virtual ~Cassandra_get_superColumn_pargs() throw() {}

  const std::string* tablename;
  const std::string* key;
  const std::string* columnFamily;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_superColumn_result {
 public:

  Cassandra_get_superColumn_result() {
  }

  virtual ~Cassandra_get_superColumn_result() throw() {}

  superColumn_t success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const Cassandra_get_superColumn_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const Cassandra_get_superColumn_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_get_superColumn_result & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_get_superColumn_presult {
 public:


  virtual ~Cassandra_get_superColumn_presult() throw() {}

  superColumn_t* success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);

};

class Cassandra_batch_insert_superColumn_args {
 public:

  Cassandra_batch_insert_superColumn_args() {
  }

  virtual ~Cassandra_batch_insert_superColumn_args() throw() {}

  batch_mutation_super_t batchMutationSuper;

  struct __isset {
    __isset() : batchMutationSuper(false) {}
    bool batchMutationSuper;
  } __isset;

  bool operator == (const Cassandra_batch_insert_superColumn_args & rhs) const
  {
    if (!(batchMutationSuper == rhs.batchMutationSuper))
      return false;
    return true;
  }
  bool operator != (const Cassandra_batch_insert_superColumn_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_batch_insert_superColumn_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_superColumn_pargs {
 public:


  virtual ~Cassandra_batch_insert_superColumn_pargs() throw() {}

  const batch_mutation_super_t* batchMutationSuper;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_superColumn_blocking_args {
 public:

  Cassandra_batch_insert_superColumn_blocking_args() {
  }

  virtual ~Cassandra_batch_insert_superColumn_blocking_args() throw() {}

  batch_mutation_super_t batchMutationSuper;

  struct __isset {
    __isset() : batchMutationSuper(false) {}
    bool batchMutationSuper;
  } __isset;

  bool operator == (const Cassandra_batch_insert_superColumn_blocking_args & rhs) const
  {
    if (!(batchMutationSuper == rhs.batchMutationSuper))
      return false;
    return true;
  }
  bool operator != (const Cassandra_batch_insert_superColumn_blocking_args &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_batch_insert_superColumn_blocking_args & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_superColumn_blocking_pargs {
 public:


  virtual ~Cassandra_batch_insert_superColumn_blocking_pargs() throw() {}

  const batch_mutation_super_t* batchMutationSuper;

  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_superColumn_blocking_result {
 public:

  Cassandra_batch_insert_superColumn_blocking_result() : success(0) {
  }

  virtual ~Cassandra_batch_insert_superColumn_blocking_result() throw() {}

  bool success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  bool operator == (const Cassandra_batch_insert_superColumn_blocking_result & rhs) const
  {
    if (!(success == rhs.success))
      return false;
    return true;
  }
  bool operator != (const Cassandra_batch_insert_superColumn_blocking_result &rhs) const {
    return !(*this == rhs);
  }

  bool operator < (const Cassandra_batch_insert_superColumn_blocking_result & ) const;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);
  uint32_t write(facebook::thrift::protocol::TProtocol* oprot) const;

};

class Cassandra_batch_insert_superColumn_blocking_presult {
 public:


  virtual ~Cassandra_batch_insert_superColumn_blocking_presult() throw() {}

  bool* success;

  struct __isset {
    __isset() : success(false) {}
    bool success;
  } __isset;

  uint32_t read(facebook::thrift::protocol::TProtocol* iprot);

};

class CassandraClient : virtual public CassandraIf, public facebook::fb303::FacebookServiceClient {
 public:
  CassandraClient(boost::shared_ptr<facebook::thrift::protocol::TProtocol> prot) :
    facebook::fb303::FacebookServiceClient(prot, prot) {}
  CassandraClient(boost::shared_ptr<facebook::thrift::protocol::TProtocol> iprot, boost::shared_ptr<facebook::thrift::protocol::TProtocol> oprot) :
    facebook::fb303::FacebookServiceClient(iprot, oprot) {}
  boost::shared_ptr<facebook::thrift::protocol::TProtocol> getInputProtocol() {
    return piprot_;
  }
  boost::shared_ptr<facebook::thrift::protocol::TProtocol> getOutputProtocol() {
    return poprot_;
  }
  void get_slice(std::vector<column_t> & _return, const std::string& tablename, const std::string& key, const std::string& columnFamily_column, const int32_t start, const int32_t count);
  void send_get_slice(const std::string& tablename, const std::string& key, const std::string& columnFamily_column, const int32_t start, const int32_t count);
  void recv_get_slice(std::vector<column_t> & _return);
  void get_column(column_t& _return, const std::string& tablename, const std::string& key, const std::string& columnFamily_column);
  void send_get_column(const std::string& tablename, const std::string& key, const std::string& columnFamily_column);
  void recv_get_column(column_t& _return);
  int32_t get_column_count(const std::string& tablename, const std::string& key, const std::string& columnFamily_column);
  void send_get_column_count(const std::string& tablename, const std::string& key, const std::string& columnFamily_column);
  int32_t recv_get_column_count();
  void insert(const std::string& tablename, const std::string& key, const std::string& columnFamily_column, const std::string& cellData, const int32_t timestamp);
  void send_insert(const std::string& tablename, const std::string& key, const std::string& columnFamily_column, const std::string& cellData, const int32_t timestamp);
  void batch_insert(const batch_mutation_t& batchMutation);
  void send_batch_insert(const batch_mutation_t& batchMutation);
  bool batch_insert_blocking(const batch_mutation_t& batchMutation);
  void send_batch_insert_blocking(const batch_mutation_t& batchMutation);
  bool recv_batch_insert_blocking();
  void remove(const std::string& tablename, const std::string& key, const std::string& columnFamily_column);
  void send_remove(const std::string& tablename, const std::string& key, const std::string& columnFamily_column);
  void get_slice_super(std::vector<superColumn_t> & _return, const std::string& tablename, const std::string& key, const std::string& columnFamily_superColumnName, const int32_t start, const int32_t count);
  void send_get_slice_super(const std::string& tablename, const std::string& key, const std::string& columnFamily_superColumnName, const int32_t start, const int32_t count);
  void recv_get_slice_super(std::vector<superColumn_t> & _return);
  void get_superColumn(superColumn_t& _return, const std::string& tablename, const std::string& key, const std::string& columnFamily);
  void send_get_superColumn(const std::string& tablename, const std::string& key, const std::string& columnFamily);
  void recv_get_superColumn(superColumn_t& _return);
  void batch_insert_superColumn(const batch_mutation_super_t& batchMutationSuper);
  void send_batch_insert_superColumn(const batch_mutation_super_t& batchMutationSuper);
  bool batch_insert_superColumn_blocking(const batch_mutation_super_t& batchMutationSuper);
  void send_batch_insert_superColumn_blocking(const batch_mutation_super_t& batchMutationSuper);
  bool recv_batch_insert_superColumn_blocking();
};

class CassandraProcessor : virtual public facebook::thrift::TProcessor, public facebook::fb303::FacebookServiceProcessor {
 protected:
  boost::shared_ptr<CassandraIf> iface_;
  virtual bool process_fn(facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot, std::string& fname, int32_t seqid);
 private:
  std::map<std::string, void (CassandraProcessor::*)(int32_t, facebook::thrift::protocol::TProtocol*, facebook::thrift::protocol::TProtocol*)> processMap_;
  void process_get_slice(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
  void process_get_column(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
  void process_get_column_count(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
  void process_insert(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
  void process_batch_insert(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
  void process_batch_insert_blocking(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
  void process_remove(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
  void process_get_slice_super(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
  void process_get_superColumn(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
  void process_batch_insert_superColumn(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
  void process_batch_insert_superColumn_blocking(int32_t seqid, facebook::thrift::protocol::TProtocol* iprot, facebook::thrift::protocol::TProtocol* oprot);
 public:
  CassandraProcessor(boost::shared_ptr<CassandraIf> iface) :
    facebook::fb303::FacebookServiceProcessor(iface),
    iface_(iface) {
    processMap_["get_slice"] = &CassandraProcessor::process_get_slice;
    processMap_["get_column"] = &CassandraProcessor::process_get_column;
    processMap_["get_column_count"] = &CassandraProcessor::process_get_column_count;
    processMap_["insert"] = &CassandraProcessor::process_insert;
    processMap_["batch_insert"] = &CassandraProcessor::process_batch_insert;
    processMap_["batch_insert_blocking"] = &CassandraProcessor::process_batch_insert_blocking;
    processMap_["remove"] = &CassandraProcessor::process_remove;
    processMap_["get_slice_super"] = &CassandraProcessor::process_get_slice_super;
    processMap_["get_superColumn"] = &CassandraProcessor::process_get_superColumn;
    processMap_["batch_insert_superColumn"] = &CassandraProcessor::process_batch_insert_superColumn;
    processMap_["batch_insert_superColumn_blocking"] = &CassandraProcessor::process_batch_insert_superColumn_blocking;
  }

  virtual bool process(boost::shared_ptr<facebook::thrift::protocol::TProtocol> piprot, boost::shared_ptr<facebook::thrift::protocol::TProtocol> poprot);
  virtual ~CassandraProcessor() {}
};

class CassandraMultiface : virtual public CassandraIf, public facebook::fb303::FacebookServiceMultiface {
 public:
  CassandraMultiface(std::vector<boost::shared_ptr<CassandraIf> >& ifaces) : ifaces_(ifaces) {
    std::vector<boost::shared_ptr<CassandraIf> >::iterator iter;
    for (iter = ifaces.begin(); iter != ifaces.end(); ++iter) {
      facebook::fb303::FacebookServiceMultiface::add(*iter);
    }
  }
  virtual ~CassandraMultiface() {}
 protected:
  std::vector<boost::shared_ptr<CassandraIf> > ifaces_;
  CassandraMultiface() {}
  void add(boost::shared_ptr<CassandraIf> iface) {
    facebook::fb303::FacebookServiceMultiface::add(iface);
    ifaces_.push_back(iface);
  }
 public:
  void get_slice(std::vector<column_t> & _return, const std::string& tablename, const std::string& key, const std::string& columnFamily_column, const int32_t start, const int32_t count) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        ifaces_[i]->get_slice(_return, tablename, key, columnFamily_column, start, count);
        return;
      } else {
        ifaces_[i]->get_slice(_return, tablename, key, columnFamily_column, start, count);
      }
    }
  }

  void get_column(column_t& _return, const std::string& tablename, const std::string& key, const std::string& columnFamily_column) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        ifaces_[i]->get_column(_return, tablename, key, columnFamily_column);
        return;
      } else {
        ifaces_[i]->get_column(_return, tablename, key, columnFamily_column);
      }
    }
  }

  int32_t get_column_count(const std::string& tablename, const std::string& key, const std::string& columnFamily_column) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        return ifaces_[i]->get_column_count(tablename, key, columnFamily_column);
      } else {
        ifaces_[i]->get_column_count(tablename, key, columnFamily_column);
      }
    }
  }

  void insert(const std::string& tablename, const std::string& key, const std::string& columnFamily_column, const std::string& cellData, const int32_t timestamp) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->insert(tablename, key, columnFamily_column, cellData, timestamp);
    }
  }

  void batch_insert(const batch_mutation_t& batchMutation) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->batch_insert(batchMutation);
    }
  }

  bool batch_insert_blocking(const batch_mutation_t& batchMutation) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        return ifaces_[i]->batch_insert_blocking(batchMutation);
      } else {
        ifaces_[i]->batch_insert_blocking(batchMutation);
      }
    }
  }

  void remove(const std::string& tablename, const std::string& key, const std::string& columnFamily_column) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->remove(tablename, key, columnFamily_column);
    }
  }

  void get_slice_super(std::vector<superColumn_t> & _return, const std::string& tablename, const std::string& key, const std::string& columnFamily_superColumnName, const int32_t start, const int32_t count) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        ifaces_[i]->get_slice_super(_return, tablename, key, columnFamily_superColumnName, start, count);
        return;
      } else {
        ifaces_[i]->get_slice_super(_return, tablename, key, columnFamily_superColumnName, start, count);
      }
    }
  }

  void get_superColumn(superColumn_t& _return, const std::string& tablename, const std::string& key, const std::string& columnFamily) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        ifaces_[i]->get_superColumn(_return, tablename, key, columnFamily);
        return;
      } else {
        ifaces_[i]->get_superColumn(_return, tablename, key, columnFamily);
      }
    }
  }

  void batch_insert_superColumn(const batch_mutation_super_t& batchMutationSuper) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      ifaces_[i]->batch_insert_superColumn(batchMutationSuper);
    }
  }

  bool batch_insert_superColumn_blocking(const batch_mutation_super_t& batchMutationSuper) {
    uint32_t sz = ifaces_.size();
    for (uint32_t i = 0; i < sz; ++i) {
      if (i == sz - 1) {
        return ifaces_[i]->batch_insert_superColumn_blocking(batchMutationSuper);
      } else {
        ifaces_[i]->batch_insert_superColumn_blocking(batchMutationSuper);
      }
    }
  }

};

}}}} // namespace

#endif
