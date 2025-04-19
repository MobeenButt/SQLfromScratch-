#include "include/DB_Manager.h"
#include "include/Transaction.h"
#include "Index.h"
#include <vector>
#include <filesystem>
namespace fs = std::filesystem;

DatabaseManager::DatabaseManager(const std::string &catalog_path)
    : catalog_path(catalog_path),
      index_manager("data/"),
      in_transaction(false),
      transaction_log_file("data/transactions.log")
{
    catalog.load(catalog_path);
    index_manager.loadAllIndexes();
}

DatabaseManager::~DatabaseManager()
{
    catalog.save(catalog_path);
    index_manager.saveAllIndexes();
}

bool DatabaseManager::createTable(
    const std::string &table_name,
    const std::vector<std::string> &column_names,
    const std::vector<FieldType> &column_types,
    const std::vector<int> &primary_key_indices,
    const std::vector<bool> &is_foreign_key,
    const std::vector<std::string> &references_table,
    const std::vector<std::string> &references_column,
    const std::string &primary_key)
{
    if (catalog.tableExists(table_name))
        return false;

    std::vector<Column> columns;
    for (size_t i = 0; i < column_names.size(); ++i)
    {
        columns.emplace_back(column_names[i], static_cast<ColumnType>(column_types[i]), 0,
                             (column_names[i] == primary_key), is_foreign_key[i],
                             references_table[i], references_column[i]);
    }

    Schema schema(table_name, columns, "data/" + table_name + ".db", "index/" + table_name + ".idx");
    if (!catalog.addTable(schema))
        return false;

    // Create primary key index if specified
    if (!primary_key.empty())
    {
        createIndex(table_name + "_pk_idx", table_name, primary_key, db::IndexType::BTREE);
    }

    return true;
}

bool DatabaseManager::createIndex(
    const std::string &index_name,
    const std::string &table_name,
    const std::string &column_name,
    db::IndexType type)
{
    if (!catalog.tableExists(table_name))
        return false;

    Schema schema = catalog.getSchema(table_name);
    bool column_exists = false;
    for (const auto &col : schema.columns)
    {
        if (col.col_name == column_name)
        {
            column_exists = true;
            break;
        }
    }
    if (!column_exists)
        return false;

    return index_manager.createIndex(index_name, table_name, column_name, type) != nullptr;
}

bool DatabaseManager::dropIndex(const std::string &index_name)
{
    index_manager.dropIndex(index_name);
    return true;
}

bool DatabaseManager::insertRecordInternal(const std::string &table_name, const Record &record)
{
    Schema schema = catalog.getSchema(table_name);
    std::ofstream file(schema.data_file_path, std::ios::app | std::ios::binary);
    if (!file)
        return false;

    int offset = file.tellp();
    char deleted_flag = 0;
    file.write(&deleted_flag, sizeof(char)); // write not-deleted marker
    saveRecord(file, record, schema);

    // Update all indexes for this table
    auto table_indexes = index_manager.getTableIndexes(table_name);
    for (auto &index : table_indexes)
    {
        const std::string &col_name = index->getColumnName();
        if (record.find(col_name) != record.end())
        {
            index->insert(record.at(col_name).toString(), offset);
        }
    }

    return true;
}

bool DatabaseManager::deleteRecordInternal(const std::string &table_name,
                                           const std::string &key_column,
                                           const FieldValue &key_value)
{
    if (!catalog.tableExists(table_name))
        return false;

    Schema schema = catalog.getSchema(table_name);

    // Try to use an index to find offsets
    auto indexes = index_manager.getColumnIndexes(table_name, key_column);
    std::vector<int> offsets;

    if (!indexes.empty())
    {
        offsets = indexes[0]->search(key_value.toString());
    }
    else
    {
        // Full scan fallback if no index
        std::ifstream read_file(schema.data_file_path, std::ios::binary);
        if (!read_file)
            return false;

        int offset = 0;
        while (read_file.peek() != EOF) {
            std::streampos pos = read_file.tellg();

            Record rec = loadRecord(read_file, schema);

            // Skip if it was marked deleted (handled inside loadRecord)
            if (rec.find(key_column) != rec.end() &&
                rec[key_column].toString() == key_value.toString()) {
                offsets.push_back(static_cast<int>(pos));
            }
        }

        read_file.close();
    }

    if (offsets.empty())
        return false;

    // Mark the record(s) as deleted
    std::fstream file(schema.data_file_path, std::ios::in | std::ios::out | std::ios::binary);
    if (!file)
        return false;

    for (int offset : offsets)
    {
        file.seekp(offset);
        char deleted_flag = 1;
        file.write(&deleted_flag, sizeof(char));
    }
    file.close();

    // Remove from all indexes
    for (auto &index : index_manager.getTableIndexes(table_name))
    {
        index->remove(key_value.toString(), offsets[0]); // may need to remove from multiple indexes
    }

    return true;
}

std::vector<Record> DatabaseManager::searchRecordsInternal(const std::string &table_name,
                                                           const std::string &key_column,
                                                           const FieldValue &key_value)
{
    std::vector<Record> results;
    Schema schema = catalog.getSchema(table_name);

    // Try to use an index first
    auto indexes = index_manager.getColumnIndexes(table_name, key_column);
    if (!indexes.empty())
    {
        std::vector<int> offsets = indexes[0]->search(key_value.toString());

        std::ifstream file(schema.data_file_path, std::ios::binary);
        if (!file)
            return results;

        for (int offset : offsets)
        {
            file.seekg(offset);
            Record record = loadRecord(file, schema);
            results.push_back(record);
        }
        return results;
    }

    // Fallback to full scan if no index exists
    std::ifstream file(schema.data_file_path, std::ios::binary);
    if (!file)
        return results;

    while (file.peek() != EOF)
    {
        int current_pos = file.tellg();
        Record record = loadRecord(file, schema);

        if (record.find(key_column) != record.end() &&
            record[key_column].toString() == key_value.toString())
        {
            results.push_back(record);
        }
    }

    return results;
}
void DatabaseManager::displayTable(const std::string &table_name)
{
    if (!catalog.tableExists(table_name))
    {
        std::cout << "Table '" << table_name << "' does not exist.\n";
        return;
    }

    Schema schema = catalog.getSchema(table_name);
    std::ifstream file(schema.data_file_path, std::ios::binary);
    if (!file)
    {
        std::cout << "Failed to open table file.\n";
        return;
    }

    std::cout << "\nTable: " << table_name << "\n";
    std::cout << "----------------------------------\n";
    for (const auto &col : schema.columns)
    {
        std::cout << col.col_name << "\t";
    }
    std::cout << "\n----------------------------------\n";

    while (file.peek() != EOF)
    {
        std::streampos pos = file.tellg();

        // Read the deleted flag
        char deleted_flag;
        file.read(&deleted_flag, sizeof(char));
        if (file.eof())
            break; // end of file reached after flag read

        if (deleted_flag == 1)
        {
            // Skip this deleted record
            for (const auto &col : schema.columns)
            {
                FieldType type = static_cast<FieldType>(col.col_type);
                if (type == FieldType::INT)
                    file.seekg(sizeof(int), std::ios::cur);
                else if (type == FieldType::FLOAT)
                    file.seekg(sizeof(float), std::ios::cur);
                else if (type == FieldType::BOOL)
                    file.seekg(sizeof(bool), std::ios::cur);
                else if (type == FieldType::STRING)
                {
                    size_t len;
                    file.read(reinterpret_cast<char *>(&len), sizeof(size_t));
                    file.seekg(len, std::ios::cur);
                }
            }
            continue;
        }
        else
        {
            // Move back so loadRecord can re-read flag
            file.seekg(pos);
        }

        Record record = loadRecord(file, schema);

        for (const auto &col : schema.columns)
        {
            const FieldValue &val = record.at(col.col_name);
            std::cout << val.toString() << "\t";
        }
        std::cout << "\n";
    }

    file.close();
    std::cout << "----------------------------------\n";
    std::cout << "\nPress any key to return to menu...\n";
    _getwch();
}

// [Implement other methods following the same pattern...]

void DatabaseManager::createDatabase(const std::string &dbName)
{
    std::string dbPath = "databases/" + dbName;
    if (!fs::exists(dbPath))
    {
        fs::create_directory(dbPath);
        catalog.createMetadata(dbName);
        std::cout << "Database '" << dbName << "' created successfully.\n";
    }
    else
    {
        std::cout << "Database already exists!\n";
    }
}

void DatabaseManager::deleteDatabase(const std::string &dbName)
{
    std::string dbPath = "databases/" + dbName;
    if (!fs::exists(dbPath))
    {
        std::cout << "Error: Database '" << dbName << "' does not exist.\n";
        return;
    }

    try
    {
        fs::remove_all(dbPath);
        std::cout << "Database '" << dbName << "' deleted successfully.\n";
    }
    catch (const fs::filesystem_error &e)
    {
        std::cout << "Error deleting database: " << e.what() << "\n";
    }
}

void DatabaseManager::saveRecord(std::ofstream &file, const Record &record, const Schema &schema)
{
    for (const auto &column : schema.columns)
    {
        const auto &value = record.at(column.col_name);
        switch (value.type)
        {
        case FieldType::INT:
            file.write(reinterpret_cast<const char *>(&value.intValue), sizeof(int));
            break;
        case FieldType::FLOAT:
            file.write(reinterpret_cast<const char *>(&value.floatValue), sizeof(float));
            break;
        case FieldType::STRING:
        {
            size_t len = value.stringValue.size();
            file.write(reinterpret_cast<const char *>(&len), sizeof(size_t));
            file.write(value.stringValue.c_str(), len);
            break;
        }
        case FieldType::BOOL:
            file.write(reinterpret_cast<const char *>(&value.boolValue), sizeof(bool));
            break;
        }
    }
}

Record DatabaseManager::loadRecord(std::ifstream &file, const Schema &schema)
{
    Record record;

    // Read deleted flag
    char deleted_flag;
    file.read(&deleted_flag, sizeof(char));

    if (file.eof() || file.fail())
    {
        std::cerr << "Error: Unexpected end of file while reading deleted flag.\n";
        return record;
    }

    for (const auto &column : schema.columns)
    {
        FieldValue value;
        FieldType colType = static_cast<FieldType>(column.col_type);

        switch (colType)
        {
        case FieldType::INT:
            file.read(reinterpret_cast<char *>(&value.intValue), sizeof(int));
            value.type = FieldType::INT;
            break;

        case FieldType::FLOAT:
            file.read(reinterpret_cast<char *>(&value.floatValue), sizeof(float));
            value.type = FieldType::FLOAT;
            break;

        case FieldType::STRING:
        {
            size_t len;
            file.read(reinterpret_cast<char *>(&len), sizeof(size_t));

            if (len > 10000)
            { // Arbitrary safety cap to prevent insane allocations
                std::cerr << "Error: String length is too large (" << len << "). Possibly corrupted data.\n";
                return record;
            }

            std::string buffer(len, '\0');
            file.read(&buffer[0], len);

            value.stringValue = buffer;
            value.type = FieldType::STRING;
            break;
        }

        case FieldType::BOOL:
            file.read(reinterpret_cast<char *>(&value.boolValue), sizeof(bool));
            value.type = FieldType::BOOL;
            break;
        }

        if (file.fail())
        {
            std::cerr << "Error reading data for column: " << column.col_name << "\n";
            return record;
        }

        record[column.col_name] = value;
    }

    return record;
}

void DatabaseManager::switchDatabase(const std::string &dbName)
{
    catalog_path = "databases/" + dbName + "/catalog.bin";
    catalog.load(catalog_path);
    // No need to manually manage indexes - index_manager handles this
    index_manager.loadAllIndexes(); // Use the existing index manager
}
// std::vector<std::string> DatabaseManager::listDatabases() {
//     std::vector<std::string> databases;
//     for (const auto& entry : fs::directory_iterator("databases")) {
//         if (entry.is_directory()) {
//             databases.push_back(entry.path().filename().string());
//         }
//     }
//     return databases;
// }

// Table Operations
bool DatabaseManager::dropTable(const std::string &table_name)
{
    if (!catalog.tableExists(table_name))
        return false;

    // Remove data file
    Schema schema = catalog.getSchema(table_name);
    fs::remove(schema.data_file_path);

    // Remove index file
    fs::remove(schema.index_file_path);

    // Drop all indexes for this table
    auto table_indexes = index_manager.getTableIndexes(table_name);
    for (auto &index : table_indexes)
    {
        index_manager.dropIndex(index->getName());
    }

    return catalog.removeTable(table_name);
}

// Record Operations
bool DatabaseManager::insertRecord(const std::string &table_name, const Record &record)
{
    if (in_transaction)
    {
        transaction_buffer.emplace_back(table_name, record);
        return true;
    }
    return insertRecordInternal(table_name, record);
}

bool DatabaseManager::deleteRecord(const std::string &table_name,
                                   const std::string &key_column,
                                   const FieldValue &key_value)
{
    if (in_transaction)
    {
        // Add to transaction log
        // Implementation depends on your transaction handling
        return true;
    }
    return deleteRecordInternal(table_name, key_column, key_value);
}

std::vector<Record> DatabaseManager::searchRecords(const std::string &table_name,
                                                   const std::string &key_column,
                                                   const FieldValue &key_value)
{
    return searchRecordsInternal(table_name, key_column, key_value);
}

// Table Information
std::vector<std::string> DatabaseManager::listTables() const
{
    return catalog.listTables();
}
std::vector<std::string> DatabaseManager::listTableColumns(const std::string& table_name) const
{
    return catalog.listTableColumns(table_name);
}



Schema DatabaseManager::getTableSchema(const std::string &table_name) const
{
    return catalog.getSchema(table_name);
}

// Database Operations
std::vector<std::string> DatabaseManager::listDatabases()
{
    std::vector<std::string> databases;
    std::string dbDir = "databases";

    if (fs::exists(dbDir) && fs::is_directory(dbDir))
    {
        for (const auto &entry : fs::directory_iterator(dbDir))
        {
            if (entry.is_directory())
            {
                databases.push_back(entry.path().filename().string());
            }
        }
    }

    return databases;
}
void DatabaseManager::displayCurrentDatabase(const std::string &dbName) const
{
    if (dbName.empty())
    {
        std::cout << "No database selected.\n";
    }
    else
    {
        std::cout << "Current database: " << dbName << "\n";
    }
}

// Transaction Support
void DatabaseManager::beginTransaction()
{
    in_transaction = true;
    transaction_buffer.clear();
}

void DatabaseManager::commitTransaction()
{
    if (!in_transaction)
        return;

    try
    {
        // Process all buffered operations
        for (const auto &op : transaction_buffer)
        {
            const auto &[table_name, record] = op;
            insertRecordInternal(table_name, record);
        }

        flushTransactionLog();
        in_transaction = false;
        transaction_buffer.clear();
    }
    catch (...)
    {
        rollbackTransaction();
        throw;
    }
}

void DatabaseManager::rollbackTransaction()
{
    in_transaction = false;
    transaction_buffer.clear();
    rollbackTransactionBuffer();
}

// Helper Methods
void DatabaseManager::flushTransactionLog()
{
    std::ofstream log(transaction_log_file, std::ios::app | std::ios::binary);
    if (!log)
    {
        std::cerr << "Error: Cannot open transaction log file." << std::endl;
        return;
    }

    // Write a commit marker with a timestamp
    std::time_t now = std::time(nullptr);
    log << "Transaction committed at: " << std::ctime(&now);
    log.flush();
}

void DatabaseManager::rollbackTransactionBuffer()
{
    // Log the rollback with timestamp
    std::ofstream log(transaction_log_file, std::ios::app | std::ios::binary);
    if (!log)
    {
        std::cerr << "Error: Cannot open transaction log file for rollback." << std::endl;
    }
    else
    {
        std::time_t now = std::time(nullptr);
        log << "Transaction rollback initiated at: " << std::ctime(&now);
        log.flush();
    }

    // Iterate over the buffered operations and undo any partial changes.
    // Here, you might want to reverse operations in transaction_buffer if they
    // had been applied prior to an error. For example, if records had been inserted,
    // you could remove them from their respective data files and update any indexes.
    //
    // For now, we simply log that the rollback operation would occur.
    std::cerr << "RollbackTransactionBuffer: Reverting " << transaction_buffer.size()
              << " buffered operations.\n";

    // TODO: Add code here to undo operations in transaction_buffer.
    // For example:
    // for (const auto& op : transaction_buffer) {
    //     const auto& table_name = op.first;
    //     const auto& record = op.second;
    //     // Remove record from table (this may require storing record offsets, etc.)
    // }
}