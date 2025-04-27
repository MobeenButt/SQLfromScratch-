#ifndef DATABASE_H
#define DATABASE_H

#include <string>
#include <vector>
#include <utility>
#include <iomanip>
#include "catalog_manager.h"
#include "storage_manager.h"
#include "index_manager.h"

class Database {
public:
    Database(const std::string& db_name);
    ~Database();

    bool createTable(const std::string& table_name,
                    const std::vector<ColumnInfo>& columns);
    
    bool dropTable(const std::string& table_name);
    
    bool insert(const std::string& table_name,
               const std::vector<std::string>& values);

    // Select methods
    std::vector<Record> select(const std::string& table_name,
                             const std::string& where_clause = "");

    bool selectWithCondition(const std::string& table_name,
                           const std::string& column_name,
                           const std::string& op,
                           const std::string& value,
                           std::vector<Record>& result);

    bool createIndex(const std::string& table_name, 
                    const std::string& column_name);
    bool dropIndex(const std::string& table_name, 
                  const std::string& column_name);
    bool update(const std::string& table_name, 
               const std::string& set_clause,
               const std::string& where_clause);
    bool remove(const std::string& table_name, 
               const std::string& where_clause);

    // Join types enum
    enum class JoinType {
        INNER,
        LEFT,
        RIGHT
    };

    // Join method
    std::vector<Record> join(
        const std::string& left_table,
        const std::string& right_table,
        const std::string& left_column,
        const std::string& right_column,
        JoinType join_type = JoinType::INNER);

private:
    std::string db_name;
    std::string data_path;
    CatalogManager* catalog_manager;
    StorageManager* storage_manager;
    IndexManager* index_manager;
    
    void reloadCatalog();

    bool selectUsingIndex(const std::string& table_name,
                         const std::string& index_file,
                         const std::string& op,
                         const std::string& value,
                         std::vector<Record>& result);

    bool selectUsingTableScan(const std::string& table_name,
                            int col_index,
                            const std::string& op,
                            const std::string& value,
                            std::vector<Record>& result);

    bool compareValues(const std::string& record_value,
                      const std::string& search_value,
                      const std::string& op);

    std::string getTablePath(const std::string& table_name);

    // Helper methods for join
    bool findJoinColumns(
        const TableInfo* left_table,
        const TableInfo* right_table,
        const std::string& left_column,
        const std::string& right_column,
        int& left_col_idx,
        int& right_col_idx);
};

#endif // DATABASE_H 