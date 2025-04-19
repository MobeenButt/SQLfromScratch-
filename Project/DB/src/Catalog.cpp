#include "include/Catalog.h"
#include <filesystem>
namespace fs = std::filesystem;

// ✅ **Serialization**
void Column::serialize(std::ostream& out) const {
    size_t name_size = col_name.size();
    out.write(reinterpret_cast<const char*>(&name_size), sizeof(name_size));
    out.write(col_name.data(), name_size);
    
    out.write(reinterpret_cast<const char*>(&col_type), sizeof(col_type));
    out.write(reinterpret_cast<const char*>(&col_length), sizeof(col_length));
    out.write(reinterpret_cast<const char*>(&is_primary), sizeof(is_primary));
    out.write(reinterpret_cast<const char*>(&is_foreign), sizeof(is_foreign));

    size_t ref_table_size = ref_table.size();
    out.write(reinterpret_cast<const char*>(&ref_table_size), sizeof(ref_table_size));
    out.write(ref_table.data(), ref_table_size);

    size_t ref_column_size = ref_column.size();
    out.write(reinterpret_cast<const char*>(&ref_column_size), sizeof(ref_column_size));
    out.write(ref_column.data(), ref_column_size);
}

// ✅ **Deserialization**
void Column::deserialize(std::istream& in) {
    size_t name_size;
    in.read(reinterpret_cast<char*>(&name_size), sizeof(name_size));
    col_name.resize(name_size);
    in.read(&col_name[0], name_size);

    in.read(reinterpret_cast<char*>(&col_type), sizeof(col_type));
    in.read(reinterpret_cast<char*>(&col_length), sizeof(col_length));
    in.read(reinterpret_cast<char*>(&is_primary), sizeof(is_primary));
    in.read(reinterpret_cast<char*>(&is_foreign), sizeof(is_foreign));

    size_t ref_table_size;
    in.read(reinterpret_cast<char*>(&ref_table_size), sizeof(ref_table_size));
    ref_table.resize(ref_table_size);
    in.read(&ref_table[0], ref_table_size);

    size_t ref_column_size;
    in.read(reinterpret_cast<char*>(&ref_column_size), sizeof(ref_column_size));
    ref_column.resize(ref_column_size);
    in.read(&ref_column[0], ref_column_size);
}

/**
 * Serializes a Schema to a binary stream.
 */
void Schema::serialize(std::ostream& out) const {
    size_t name_len = name.size();
    out.write(reinterpret_cast<const char*>(&name_len), sizeof(name_len));
    out.write(name.c_str(), name_len);

    size_t num_columns = columns.size();
    out.write(reinterpret_cast<const char*>(&num_columns), sizeof(num_columns));
    for (const auto& col : columns) {
        col.serialize(out);
    }

    size_t data_path_len = data_file_path.size();
    out.write(reinterpret_cast<const char*>(&data_path_len), sizeof(data_path_len));
    out.write(data_file_path.c_str(), data_path_len);

    size_t index_path_len = index_file_path.size();
    out.write(reinterpret_cast<const char*>(&index_path_len), sizeof(index_path_len));
    out.write(index_file_path.c_str(), index_path_len);
}

/**
 * Deserializes a Schema from a binary stream.
 */
void Schema::deserialize(std::ifstream& file) {
    // Read name length
    size_t name_length;
    if (!file.read(reinterpret_cast<char*>(&name_length), sizeof(name_length))) {
        throw std::runtime_error("Failed to read name length");
    }
    
    // Validate name length
    const size_t MAX_NAME_LENGTH = 256;
    if (name_length == 0 || name_length > MAX_NAME_LENGTH) {
        throw std::runtime_error("Invalid name length");
    }
    
    // Read name
    std::vector<char> name_buffer(name_length);
    if (!file.read(name_buffer.data(), name_length)) {
        throw std::runtime_error("Failed to read name");
    }
    
    this->name = std::string(name_buffer.data(), name_length);
    
    // Read number of columns
    size_t num_columns;
    if (!file.read(reinterpret_cast<char*>(&num_columns), sizeof(num_columns))) {
        throw std::runtime_error("Failed to read column count");
    }
    
    // Validate column count
    const size_t MAX_COLUMNS = 100;
    if (num_columns > MAX_COLUMNS) {
        throw std::runtime_error("Too many columns");
    }
    
    // Continue with your existing deserialization logic
    // but add validation and error checks...
}

/**
 * Loads the catalog from a binary file.
 */
bool Catalog::load(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) return false;

    size_t num_tables;
    if (!file.read(reinterpret_cast<char*>(&num_tables), sizeof(num_tables))) {
        return false; // Error reading file
    }

    // Validate number of tables (adjust MAX_TABLES to a reasonable limit)
    const size_t MAX_TABLES = 1000;
    if (num_tables > MAX_TABLES || num_tables == 0) {
        return false; // Invalid number of tables
    }

    tables.clear();
    try {
        for (size_t i = 0; i < num_tables; ++i) {
            Schema schema;
            try {
                schema.deserialize(file);
                // Check if file is still good after deserialization
                if (!file.good() && !file.eof()) {
                    return false; // Error during deserialization
                }
                tables[schema.name] = std::move(schema);
            } catch (const std::exception& e) {
                // Catch any exceptions thrown during deserialization
                return false;
            }
        }
    } catch (const std::bad_alloc& e) {
        // Handle memory allocation failure
        tables.clear(); // Clean up partially loaded data
        return false;
    }
    return true;
}

bool Catalog::save(const std::string& path) const {
    std::ofstream file(path, std::ios::binary);
    if (!file) return false;  // Instead of throwing an error, return false

    size_t num_tables = tables.size();
    file.write(reinterpret_cast<const char*>(&num_tables), sizeof(num_tables));

    for (const auto& table : tables) {
        table.second.serialize(file);
    }
    return true;  // Return true on success
}


/**
 * Checks if a table exists in the catalog.
 */
bool Catalog::tableExists(const std::string& table_name) const {
    return tables.find(table_name) != tables.end();
}

/**
 * Retrieves the schema of a table.
 */
Schema Catalog::getSchema(const std::string& table_name) const {
    auto it = tables.find(table_name);
    if (it == tables.end()) {
        throw std::runtime_error("Error: Table '" + table_name + "' not found.");
    }
    return it->second;
}

/**
 * Registers a new table in the catalog.
 */
bool Catalog::addTable(const Schema& schema) {
    if (tableExists(schema.name)) {
        return false;
    }
    tables[schema.name] = schema;
    return true;
}

/**
 * Removes a table from the catalog.
 */
bool Catalog::removeTable(const std::string& table_name) {
    return tables.erase(table_name) > 0;
}

/**
 * Lists all tables in the catalog.
 */
std::vector<std::string> Catalog::listTables() const {
    std::vector<std::string> table_names;
    table_names.reserve(tables.size());
    for (const auto& table : tables) {
        table_names.push_back(table.first);
    }
    return table_names;
}

std::vector<std::string> Catalog::listTableColumns(const std::string& table_name) const {
    std::vector<std::string> column_names;

    auto it = tables.find(table_name);
    if (it == tables.end()) return column_names;  // return empty if table not found

    const Schema& schema = it->second;
    column_names.reserve(schema.columns.size());

    for (const auto& col : schema.columns) {
        column_names.push_back(col.col_name);
    }
    return column_names;
}


void Catalog::createMetadata(const std::string& dbName) {
    std::string metaFilePath = "databases/" + dbName + "/metadata.dat";
    
    if (!fs::exists(metaFilePath)) {
        std::ofstream metaFile(metaFilePath);
        if (metaFile) {
            metaFile << "{}";  // Initialize metadata as an empty JSON-like format
            metaFile.close();
            std::cout << "Metadata initialized for database: " << dbName << "\n";
        } else {
            std::cerr << "Error creating metadata file for " << dbName << "!\n";
        }
    } else {
        std::cout << "Metadata already exists for database: " << dbName << "\n";
    }
}
