#include "transaction_manager.h"
#include <algorithm>
#include <thread>
#include <chrono>
#include <queue>
#include <set>

TransactionManager::TransactionManager(StorageManager* sm, CatalogManager* cm, IndexManager* im)
    : storage_manager(sm), catalog_manager(cm), index_manager(im), next_transaction_id(1) {}

TransactionManager::~TransactionManager() {
    // Clean up any remaining transactions
    std::lock_guard<std::mutex> lock(transaction_mutex);
    transactions.clear();
}

int TransactionManager::beginTransaction() {
    std::lock_guard<std::mutex> lock(transaction_mutex);
    int transaction_id = next_transaction_id++;
    transactions[transaction_id] = std::make_unique<Transaction>(transaction_id);
    return transaction_id;
}

bool TransactionManager::commitTransaction(int transaction_id) {
    std::lock_guard<std::mutex> lock(transaction_mutex);
    auto it = transactions.find(transaction_id);
    if (it == transactions.end()) {
        return false;
    }

    // Release all locks held by the transaction
    releaseAllLocks(transaction_id);
    
    // Mark transaction as committed
    it->second->setState(TransactionState::COMMITTED);
    
    // Remove the transaction
    transactions.erase(it);
    return true;
}

bool TransactionManager::abortTransaction(int transaction_id) {
    std::lock_guard<std::mutex> lock(transaction_mutex);
    auto it = transactions.find(transaction_id);
    if (it == transactions.end()) {
        return false;
    }

    // Release all locks held by the transaction
    releaseAllLocks(transaction_id);
    
    // Mark transaction as aborted
    it->second->setState(TransactionState::ABORTED);
    
    // Remove the transaction
    transactions.erase(it);
    return true;
}

bool TransactionManager::acquireLock(int transaction_id, const std::string& resource, LockMode mode) {
    std::lock_guard<std::mutex> lock(lock_mutex);
    
    // Check if transaction exists and is active
    auto trans_it = transactions.find(transaction_id);
    if (trans_it == transactions.end() || 
        trans_it->second->getState() != TransactionState::ACTIVE) {
        return false;
    }

    // Check if transaction already has the lock
    if (hasLock(transaction_id, resource, mode)) {
        return true;
    }

    // Check if we can acquire the lock
    if (!canAcquireLock(transaction_id, resource, mode)) {
        // Wait for lock
        waitForLock(transaction_id, resource, mode);
    }

    // Create and add the lock
    Lock new_lock;
    new_lock.type = (mode == LockMode::READ) ? LockType::SHARED : LockType::EXCLUSIVE;
    new_lock.resource = resource;
    new_lock.transaction_id = transaction_id;

    lock_table[resource].push_back(new_lock);
    trans_it->second->addLock(new_lock);

    return true;
}

bool TransactionManager::releaseLock(int transaction_id, const std::string& resource) {
    std::lock_guard<std::mutex> lock(lock_mutex);
    
    auto it = transactions.find(transaction_id);
    if (it == transactions.end()) {
        return false;
    }

    // Remove lock from transaction
    it->second->removeLock(resource);

    // Remove lock from lock table
    auto& locks = lock_table[resource];
    locks.erase(
        std::remove_if(locks.begin(), locks.end(),
            [&](const Lock& l) { return l.transaction_id == transaction_id; }),
        locks.end()
    );

    if (locks.empty()) {
        lock_table.erase(resource);
    }

    return true;
}

bool TransactionManager::releaseAllLocks(int transaction_id) {
    std::lock_guard<std::mutex> lock(lock_mutex);
    
    auto it = transactions.find(transaction_id);
    if (it == transactions.end()) {
        return false;
    }

    // Get all locks held by the transaction
    const auto& locks = it->second->getLocks();
    
    // Release each lock
    for (const auto& lock : locks) {
        auto& resource_locks = lock_table[lock.resource];
        resource_locks.erase(
            std::remove_if(resource_locks.begin(), resource_locks.end(),
                [&](const Lock& l) { return l.transaction_id == transaction_id; }),
            resource_locks.end()
        );

        if (resource_locks.empty()) {
            lock_table.erase(lock.resource);
        }
    }

    return true;
}

bool TransactionManager::insert(int transaction_id, const std::string& table_name, const Record& record) {
    // Acquire write lock
    if (!acquireLock(transaction_id, table_name, LockMode::WRITE)) {
        return false;
    }

    // Perform insert
    bool success = storage_manager->insertRecord(current_db_name, table_name, record);

    // Release lock if insert failed
    if (!success) {
        releaseLock(transaction_id, table_name);
    }

    return success;
}

bool TransactionManager::update(int transaction_id, const std::string& table_name, 
                              const Record& old_record, const Record& new_record) {
    // Acquire write lock
    if (!acquireLock(transaction_id, table_name, LockMode::WRITE)) {
        return false;
    }

    // Perform update
    bool success = storage_manager->updateRecord(current_db_name, table_name, old_record, new_record);

    // Release lock if update failed
    if (!success) {
        releaseLock(transaction_id, table_name);
    }

    return success;
}

bool TransactionManager::remove(int transaction_id, const std::string& table_name, const Record& record) {
    // Acquire write lock
    if (!acquireLock(transaction_id, table_name, LockMode::WRITE)) {
        return false;
    }

    // Perform delete
    bool success = storage_manager->deleteRecord(current_db_name, table_name, record);

    // Release lock if delete failed
    if (!success) {
        releaseLock(transaction_id, table_name);
    }

    return success;
}

std::vector<Record> TransactionManager::select(int transaction_id, const std::string& table_name, 
                                             const std::string& condition) {
    // Acquire read lock
    if (!acquireLock(transaction_id, table_name, LockMode::READ)) {
        return std::vector<Record>();
    }

    // Perform select
    std::vector<Record> results = storage_manager->selectRecords(current_db_name, table_name, condition);

    // Release lock
    releaseLock(transaction_id, table_name);

    return results;
}

bool TransactionManager::hasLock(int transaction_id, const std::string& resource, LockMode mode) {
    auto it = lock_table.find(resource);
    if (it == lock_table.end()) {
        return false;
    }

    const auto& locks = it->second;
    return std::any_of(locks.begin(), locks.end(),
        [&](const Lock& lock) {
            return lock.transaction_id == transaction_id &&
                   ((mode == LockMode::READ && lock.type == LockType::SHARED) ||
                    (mode == LockMode::WRITE && lock.type == LockType::EXCLUSIVE));
        });
}

bool TransactionManager::canAcquireLock(int transaction_id, const std::string& resource, LockMode mode) {
    // Check if transaction exists
    if (transactions.find(transaction_id) == transactions.end()) {
        return false;
    }
    
    // Check if resource is already locked
    auto lock_it = lock_table.find(resource);
    if (lock_it == lock_table.end()) {
        return true;  // No locks on resource
    }
    
    // Check if transaction already holds the lock
    for (const auto& lock : lock_it->second) {
        if (lock.transaction_id == transaction_id) {
            return true;  // Transaction already holds the lock
        }
    }
    
    // Check lock compatibility
    if (mode == LockMode::READ) {
        // For read locks, check if there are no exclusive locks
        return std::none_of(lock_it->second.begin(), lock_it->second.end(),
            [](const Lock& lock) { return lock.type == LockType::EXCLUSIVE; });
    }
    
    // For write locks, check if there are no other locks
    return lock_it->second.empty();
}

void TransactionManager::waitForLock(int transaction_id, const std::string& resource, LockMode mode) {
    // Simple implementation: wait with timeout
    const int max_attempts = 10;
    const int wait_time_ms = 100;
    
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
        if (canAcquireLock(transaction_id, resource, mode)) {
            return;
        }
        
        // Check for deadlocks
        if (checkDeadlock(transaction_id)) {
            throw std::runtime_error("Deadlock detected");
        }
        
        std::this_thread::sleep_for(std::chrono::milliseconds(wait_time_ms));
    }
    
    throw std::runtime_error("Lock acquisition timeout");
}

bool TransactionManager::checkDeadlock(int transaction_id) {
    std::set<int> visited;
    std::queue<int> to_visit;
    to_visit.push(transaction_id);
    
    while (!to_visit.empty()) {
        int current = to_visit.front();
        to_visit.pop();
        
        if (visited.count(current) > 0) {
            return true;  // Cycle detected
        }
        
        visited.insert(current);
        
        // Find transactions waiting for locks held by current transaction
        for (const auto& [resource, locks] : lock_table) {
            for (const auto& lock : locks) {
                if (lock.transaction_id == current) {
                    // Find transactions waiting for this resource
                    for (const auto& waiting_lock : locks) {
                        if (waiting_lock.transaction_id != current) {
                            to_visit.push(waiting_lock.transaction_id);
                        }
                    }
                }
            }
        }
    }
    
    return false;
}

void TransactionManager::detectAndResolveDeadlocks() {
    std::lock_guard<std::mutex> lock(lock_mutex);
    
    // Check for deadlocks among all active transactions
    for (const auto& [id, transaction] : transactions) {
        if (transaction->getState() == TransactionState::ACTIVE && checkDeadlock(id)) {
            // Abort the youngest transaction in the deadlock cycle
            abortTransaction(id);
            break;
        }
    }
} 