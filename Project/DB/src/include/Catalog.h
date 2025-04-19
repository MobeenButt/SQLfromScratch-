#ifndef CATALOG_H
#define CATALOG_H

#include <vector>
#include <string>
#include <unordered_map>
#include <fstream>
#include <iostream>
#include <stdexcept>

/**
 * Enum class to define different data types for columns.
 */
enum class ColumnType { INT, FLOAT, STRING, CHAR, BOOL, DATE, ENUM };

/**
 * Represents a column in a table schema.
 */
struct Column {
    std::string col_name;
    ColumnType col_type;
    int col_length;
    bool is_primary;
    bool is_foreign;
    std::string ref_table;
    std::string ref_column;
    bool is_unique;
    bool not_null;

    // ✅ **Default Constructor**
    Column() : col_name(""), col_type(ColumnType::INT), col_length(0),
               is_primary(false), is_foreign(false), ref_table(""), ref_column("") {}

    // ✅ **Parameterized Constructor**
    Column(std::string col_name, ColumnType col_type, int col_length = 0,
           bool is_primary = false, bool is_foreign = false,
           std::string ref_table = "", std::string ref_column = "")
        : col_name(col_name), col_type(col_type), col_length(col_length),
          is_primary(is_primary), is_foreign(is_foreign),
          ref_table(ref_table), ref_column(ref_column) {}

    // ✅ **Serialization**
    void serialize(std::ostream& out) const;
    void deserialize(std::istream& in);
    
};

/**
 * Represents a table schema containing column definitions and file paths.
 */
struct Schema {
    std::string name;
    std::vector<Column> columns;
    std::string data_file_path;
    std::string index_file_path;

    Schema(std::string table_name, std::vector<Column> table_columns, 
                std::string data_path, std::string index_path)
        : name(std::move(table_name)), columns(std::move(table_columns)), 
          data_file_path(std::move(data_path)), index_file_path(std::move(index_path)) {}

    Schema() = default;
    // Serialization for efficient binary storage
    void serialize(std::ostream& out) const;
    void deserialize(std::ifstream& file);
};

/**
 * Manages the database schema catalog, handling table metadata storage and retrieval.
 */
class Catalog {
public:
    /**
     * Loads the catalog from a binary file.
     * @param path File path to load from.
     */
    bool load(const std::string& path);

    /**
     * Saves the catalog to a binary file.
     * @param path File path to save to.
     */
    bool save(const std::string& path) const;

    /**
     * Checks if a table exists in the catalog.
     * @param table_name Name of the table.
     * @return True if the table exists, otherwise false.
     */
    void createDatabase(const std::string& dbName);

    /**
     * Checks if a table exists in the catalog.
     * @param table_name Name of the table.
     * @return True if the table exists, otherwise false.
     */
    bool tableExists(const std::string& table_name) const;

    /**
     * Retrieves the schema of a table.
     * @param table_name Name of the table.
     * @return The Schema structure for the table.
     * @throws std::runtime_error if the table does not exist.
     */
    Schema getSchema(const std::string& table_name) const;

    /**
     * Registers a new table in the catalog.
     * @param schema The schema of the table.
     * @return True if the table was successfully added, false if it already exists.
     */
    bool addTable(const Schema& schema);

    /**
     * Removes a table from the catalog.
     * @param table_name Name of the table to remove.
     * @return True if removed successfully, false if the table doesn't exist.
     */
    bool removeTable(const std::string& table_name);

    /**
     * Lists all tables in the catalog.
     * @return A vector of table names.
     */
    std::vector<std::string> listTables() const;

    void createMetadata(const std::string& dbName);

    std::vector<std::string> listTableColumns(const std::string& table_name) const;

private:
    std::unordered_map<std::string, Schema> tables; // Faster lookup than vector
};

#endif // CATALOG_H
