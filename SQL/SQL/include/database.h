#pragma once

#include <string>
#include <vector>
#include <utility>
#include <iomanip>
#include <memory>
#include <map>
#include "catalog_manager.h"
#include "storage_manager.h"
#include "index_manager.h"
#include "page.h"
#include "transaction_manager.h"
#include "record.h"

// Forward declarations
struct QueryClauses {
    std::string group_by_column;
    std::string having_condition;
    std::string order_by_column;
    bool order_asc = true;
};

QueryClauses parseQueryClauses(const std::string& query);

class Database {
public:
    Database(const std::string& name);
    ~Database();

    // Transaction management
    int beginTransaction();
    bool commitTransaction(int transaction_id);
    bool abortTransaction(int transaction_id);

    bool createTable(const std::string& table_name, const std::vector<ColumnInfo>& columns);
    bool dropTable(const std::string& table_name);
    bool insert(const std::string& table_name, const std::vector<std::string>& values);
    std::vector<Record> select(const std::string& table_name, const std::string& where_clause = "");
    bool selectWithCondition(const std::string& table_name, const std::string& column_name,
                            const std::string& op, const std::string& value,
                            std::vector<Record>& result);
    bool update(const std::string& table_name, const std::string& set_clause,
                const std::string& where_clause);
    bool remove(const std::string& table_name, const std::string& where_clause);
    int getColumnIndex(TableInfo* table, const std::string& col_name);

    // Select methods
    bool selectWithClauses(const std::string& table_name, const std::string& query);
    std::vector<Record> groupQuery(const std::string& table_name, const std::string& group_column,
                                  const std::string& agg_function, const std::string& where_clause,
                                  const std::string& having_clause);

    bool createIndex(const std::string& table_name, const std::string& column_name);
    bool dropIndex(const std::string& table_name, const std::string& column_name);
    bool dropDatabase(const std::string& db_name);  // database.h
    // Join types enum
    enum class JoinType {
        INNER,
        LEFT,
        RIGHT
    };

    // Join method
    std::vector<Record> join(const std::string& left_table, const std::string& right_table,
                            const std::string& left_column, const std::string& right_column,
                            JoinType join_type = JoinType::INNER);

private:
    std::string db_name;
    std::string data_path;
    std::unique_ptr<CatalogManager> catalog_manager;
    std::unique_ptr<StorageManager> storage_manager;
    std::unique_ptr<IndexManager> index_manager;
    std::unique_ptr<TransactionManager> transaction_manager;
    
    void reloadCatalog();
    bool cleanup();

    bool selectUsingIndex(const std::string& table_name, const std::string& index_file,
                         const std::string& op, const std::string& value,
                         std::vector<Record>& result);

    bool selectUsingTableScan(const std::string& table_name, int col_index,
                             const std::string& op, const std::string& value,
                             std::vector<Record>& result);

    bool compareValues(const std::string& record_value, const std::string& search_value,
                      const std::string& op);

    std::string getTablePath(const std::string& table_name);

    // Helper methods for join
    bool findJoinColumns(const TableInfo* left_table, const TableInfo* right_table,
                        const std::string& left_column, const std::string& right_column,
                        int& left_col_idx, int& right_col_idx);

    bool evaluateCondition(const Record& record, const std::string& condition);
}; 