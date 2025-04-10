#include "include/Transaction.h"
#include <iostream>

/**
 * Constructs a Transaction Manager with a log file for WAL.
 */
Transaction::Transaction(const std::string& log_file) 
    : log_file(log_file), in_transaction(false) {}

/**
 * Begins a new transaction.
 */
void Transaction::begin() {
    if (in_transaction) {
        std::cerr << "Warning: Transaction already in progress." << std::endl;
        return;
    }
    in_transaction = true;
    operations.clear();
    logOperation("BEGIN TRANSACTION");
}

/**
 * Commits the transaction, making changes permanent.
 */
void Transaction::commit() {
    if (!in_transaction) {
        std::cerr << "Warning: No active transaction to commit." << std::endl;
        return;
    }

    logOperation("COMMIT TRANSACTION");
    flushLog();  // Write all operations to disk
    in_transaction = false;
}

/**
 * Rolls back the transaction, undoing uncommitted operations.
 */
void Transaction::rollback() {
    if (!in_transaction) {
        std::cerr << "Warning: No active transaction to rollback." << std::endl;
        return;
    }

    logOperation("ROLLBACK TRANSACTION");
    operations.clear();  // Discard operations
    in_transaction = false;
}

/**
 * Logs an operation to the transaction log.
 */
void Transaction::logOperation(const std::string& operation) {
    if (!in_transaction) {
        std::cerr << "Warning: No active transaction. Ignoring operation: " << operation << std::endl;
        return;
    }

    std::string log_entry = getTimestamp() + " " + operation;
    operations.push_back(log_entry);
}

/**
 * Writes the log operations to disk for durability.
 */
void Transaction::flushLog() {
    std::ofstream logStream(log_file, std::ios::app);
    if (!logStream) {
        throw std::runtime_error("Error: Unable to open transaction log file.");
    }

    for (const auto& op : operations) {
        logStream << op << std::endl;
    }

    logStream.close();
    operations.clear();  // Clear after writing
}

/**
 * Gets the current timestamp for logging.
 */
std::string Transaction::getTimestamp() const {
    auto now = std::chrono::system_clock::now();
    auto time_t_now = std::chrono::system_clock::to_time_t(now);
    std::tm tm_now = *std::localtime(&time_t_now);

    std::ostringstream oss;
    oss << std::put_time(&tm_now, "[%Y-%m-%d %H:%M:%S]");
    return oss.str();
}
