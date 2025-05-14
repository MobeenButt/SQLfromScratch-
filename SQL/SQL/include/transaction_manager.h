#ifndef TRANSACTION_MANAGER_H
#define TRANSACTION_MANAGER_H

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <memory>
#include <algorithm>  // For std::remove_if
#include "storage_manager.h"
#include "catalog_manager.h"
#include "index_manager.h"

// Transaction states
enum class TransactionState {
    ACTIVE,
    COMMITTED,
    ABORTED
};

// Lock types
enum class LockType {
    SHARED,
    EXCLUSIVE
};

// Lock modes
enum class LockMode {
    READ,
    WRITE
};

// Lock object
struct Lock {
    LockType type;
    std::string resource;  // Table or record identifier
    int transaction_id;
};

// Transaction object
class Transaction {
public:
    Transaction(int id) : transaction_id(id), state(TransactionState::ACTIVE) {}
    
    int getTransactionId() const { return transaction_id; }
    TransactionState getState() const { return state; }
    void setState(TransactionState new_state) { state = new_state; }
    
    void addLock(const Lock& lock) { locks.push_back(lock); }
    void removeLock(const std::string& resource) {
        locks.erase(
            std::remove_if(locks.begin(), locks.end(),
                [&](const Lock& l) { return l.resource == resource; }),
            locks.end()
        );
    }
    
    const std::vector<Lock>& getLocks() const { return locks; }
    
private:
    int transaction_id;
    TransactionState state;
    std::vector<Lock> locks;
};

class TransactionManager {
public:
    TransactionManager(StorageManager* storage_manager,
                      CatalogManager* catalog_manager,
                      IndexManager* index_manager);
    ~TransactionManager();

    // Transaction management
    int beginTransaction();
    bool commitTransaction(int transaction_id);
    bool abortTransaction(int transaction_id);
    
    // Lock management
    bool acquireLock(int transaction_id, const std::string& resource, LockMode mode);
    bool releaseLock(int transaction_id, const std::string& resource);
    bool releaseAllLocks(int transaction_id);
    
    // Data operations with transaction support
    bool insert(int transaction_id, const std::string& table_name, const Record& record);
    bool update(int transaction_id, const std::string& table_name, const Record& old_record, const Record& new_record);
    bool remove(int transaction_id, const std::string& table_name, const Record& record);
    std::vector<Record> select(int transaction_id, const std::string& table_name, const std::string& condition = "");

    // Set current database
    void setCurrentDatabase(const std::string& db_name) { current_db_name = db_name; }

private:
    StorageManager* storage_manager;
    CatalogManager* catalog_manager;
    IndexManager* index_manager;
    
    std::mutex transaction_mutex;
    std::mutex lock_mutex;
    
    int next_transaction_id;
    std::string current_db_name;
    std::unordered_map<int, std::unique_ptr<Transaction>> transactions;
    std::unordered_map<std::string, std::vector<Lock>> lock_table;
    
    bool checkDeadlock(int transaction_id);
    void detectAndResolveDeadlocks();
    bool hasLock(int transaction_id, const std::string& resource, LockMode mode);
    bool canAcquireLock(int transaction_id, const std::string& resource, LockMode mode);
    void waitForLock(int transaction_id, const std::string& resource, LockMode mode);
};

#endif // TRANSACTION_MANAGER_H 