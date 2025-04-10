#ifndef TRANSACTION_H
#define TRANSACTION_H

#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <chrono>
#include <iomanip>

/**
 * Manages database transactions with Write-Ahead Logging (WAL).
 */
class Transaction {
public:
    /**
     * Constructs a Transaction Manager with a log file for WAL.
     * @param log_file Path to the transaction log file.
     */
    explicit Transaction(const std::string& log_file);

    /**
     * Begins a new transaction.
     */
    void begin();

    /**
     * Commits the transaction, making changes permanent.
     */
    void commit();

    /**
     * Rolls back the transaction, undoing uncommitted operations.
     */
    void rollback();

    /**
     * Logs an operation to the transaction log.
     * @param operation The operation description.
     */
    void logOperation(const std::string& operation);

private:
    std::string log_file;
    std::vector<std::string> operations;
    bool in_transaction;

    /**
     * Writes the log operations to disk for durability.
     */
    void flushLog();
    
    /**
     * Gets the current timestamp for logging.
     * @return A formatted timestamp string.
     */
    std::string getTimestamp() const;
};

#endif // TRANSACTION_H
