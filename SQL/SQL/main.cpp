#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <fstream>
#include "database.h"
#include <regex>
#include <filesystem>

void printHelp()
{
    std::cout << "\n=== Simple DBMS v1.0 Help ===\n\n";
    std::cout << "Database Management Commands:\n";
    std::cout << "----------------------------\n";
    std::cout << "CREATE DATABASE <name>     - Create a new database\n";
    std::cout << "USE DATABASE <name>        - Switch to a database\n";
    std::cout << "DROP DATABASE <name>       - Delete a database\n\n";

    std::cout << "Transaction Commands:\n";
    std::cout << "--------------------\n";
    std::cout << "BEGIN TRANSACTION         - Start a new transaction\n";
    std::cout << "COMMIT                    - Commit the current transaction\n";
    std::cout << "ROLLBACK                  - Roll back the current transaction\n\n";

    std::cout << "Table Management Commands:\n";
    std::cout << "-------------------------\n";
    std::cout << "CREATE TABLE <name> (<column_definitions>)\n";
    std::cout << "  Example: CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(50), salary INT)\n\n";
    std::cout << "DROP TABLE <name>          - Delete a table\n\n";

    std::cout << "Data Manipulation Commands:\n";
    std::cout << "--------------------------\n";
    std::cout << "INSERT INTO <table> VALUES (value1, value2, ...)\n";
    std::cout << "  Example: INSERT INTO employees VALUES (1, 'John', 50000)\n\n";
    
    std::cout << "SELECT Queries:\n";
    std::cout << "--------------\n";
    std::cout << "1. Basic SELECT:\n";
    std::cout << "   SELECT * FROM <table> [WHERE condition]\n";
    std::cout << "   Example: SELECT * FROM employees WHERE salary > 50000\n\n";
    
    std::cout << "2. Aggregate Functions:\n";
    std::cout << "   SELECT COUNT(*) FROM <table>\n";
    std::cout << "   SELECT SUM(<column>) FROM <table>\n";
    std::cout << "   SELECT AVG(<column>) FROM <table>\n";
    std::cout << "   SELECT MIN(<column>) FROM <table>\n";
    std::cout << "   SELECT MAX(<column>) FROM <table>\n";
    std::cout << "   Example: SELECT AVG(salary) FROM employees\n\n";
    
    std::cout << "3. GROUP BY Queries:\n";
    std::cout << "   SELECT <column>, COUNT(*) FROM <table> GROUP BY <column>\n";
    std::cout << "   SELECT <column>, SUM(<column>) FROM <table> GROUP BY <column>\n";
    std::cout << "   Example: SELECT department, COUNT(*) FROM employees GROUP BY department\n\n";
    
    std::cout << "4. HAVING Clause:\n";
    std::cout << "   SELECT <column>, COUNT(*) FROM <table> GROUP BY <column> HAVING COUNT(*) > n\n";
    std::cout << "   Example: SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 5\n\n";
    
    std::cout << "5. ORDER BY:\n";
    std::cout << "   SELECT * FROM <table> ORDER BY <column> [ASC|DESC]\n";
    std::cout << "   Example: SELECT * FROM employees ORDER BY salary DESC\n\n";
    
    std::cout << "6. JOIN Queries:\n";
    std::cout << "   SELECT * FROM <table1> JOIN <table2> ON <table1.column> = <table2.column>\n";
    std::cout << "   Example: SELECT * FROM employees JOIN departments ON employees.dept_id = departments.id\n\n";

    std::cout << "Data Modification Commands:\n";
    std::cout << "-------------------------\n";
    std::cout << "UPDATE <table> SET column = value WHERE condition\n";
    std::cout << "  Example: UPDATE employees SET salary = 60000 WHERE id = 1\n\n";
    
    std::cout << "DELETE FROM <table> WHERE condition\n";
    std::cout << "  Example: DELETE FROM employees WHERE id = 1\n\n";

    std::cout << "Index Management:\n";
    std::cout << "----------------\n";
    std::cout << "CREATE INDEX ON <table>(<column>)\n";
    std::cout << "  Example: CREATE INDEX ON employees(salary)\n\n";

    std::cout << "System Commands:\n";
    std::cout << "---------------\n";
    std::cout << "HELP            - Show this help message\n";
    std::cout << "EXIT            - Exit the program\n\n";

    std::cout << "Example Complete Session:\n";
    std::cout << "------------------------\n";
    std::cout << "CREATE DATABASE company\n";
    std::cout << "USE DATABASE company\n";
    std::cout << "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(50), salary INT, department VARCHAR(30))\n";
    std::cout << "INSERT INTO employees VALUES (1, 'John', 50000, 'IT')\n";
    std::cout << "INSERT INTO employees VALUES (2, 'Alice', 60000, 'HR')\n";
    std::cout << "SELECT * FROM employees\n";
    std::cout << "SELECT department, AVG(salary) FROM employees GROUP BY department\n";
    std::cout << "UPDATE employees SET salary = 55000 WHERE id = 1\n";
    std::cout << "CREATE INDEX ON employees(salary)\n";
    std::cout << "SELECT * FROM employees WHERE salary > 55000\n";
    std::cout << "EXIT\n\n";

    std::cout << "Note: All commands are case-insensitive except for data values.\n";
    std::cout << "      String values should be enclosed in single quotes.\n";
    std::cout << "      Use semicolons to separate multiple commands.\n";
}

// Helper function to split string by delimiter
std::vector<std::string> split(const std::string &str, char delimiter)
{
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(str);
    while (std::getline(tokenStream, token, delimiter))
    {
        // Trim whitespace
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);
        if (!token.empty())
        {
            tokens.push_back(token);
        }
    }
    return tokens;
}

// Helper function to parse column definitions
std::vector<ColumnInfo> parseColumnDefs(const std::string &columnDefs)
{
    std::vector<ColumnInfo> columns;
    std::istringstream ss(columnDefs);
    std::string token;
    std::string current_def;

    while (std::getline(ss, token, ','))
    {
        // Trim whitespace from token
        token.erase(0, token.find_first_not_of(" \t"));
        token.erase(token.find_last_not_of(" \t") + 1);

        std::istringstream def_stream(token);
        ColumnInfo col;

        std::string name, type, constraint;
        def_stream >> name >> type;
        col.name = name;

        // Handle VARCHAR(n)
        if (type.substr(0, 7) == "VARCHAR(")
        {
            col.type = "VARCHAR";
            col.size = std::stoi(type.substr(7, type.length() - 8));
        }
        else
        {
            col.type = type;
            if (type == "INT")
                col.size = sizeof(int);
        }

        // Parse constraints
        std::string word;
        while (def_stream >> word)
        {
            if (word == "PRIMARY" && def_stream >> word && word == "KEY")
            {
                col.is_primary_key = true;
            }
            else if (word == "FOREIGN" && def_stream >> word && word == "KEY")
            {
                col.is_foreign_key = true;
                // Parse REFERENCES
                if (def_stream >> word && word == "REFERENCES")
                {
                    std::string ref_def;
                    def_stream >> ref_def;
                    // Parse table(column)
                    size_t open_paren = ref_def.find('(');
                    size_t close_paren = ref_def.find(')');
                    if (open_paren != std::string::npos && close_paren != std::string::npos)
                    {
                        col.references_table = ref_def.substr(0, open_paren);
                        col.references_column = ref_def.substr(open_paren + 1,
                                                               close_paren - open_paren - 1);
                    }
                }
            }
        }
        columns.push_back(col);
    }
    return columns;
}
bool Database::dropDatabase(const std::string& db_name) {  // database.cpp
    std::string db_path = "./data/" + db_name;
    try {
        if (std::filesystem::remove_all(db_path)) {
            std::cout << "Database dropped: " << db_name << std::endl;
            return true;
        }
        return false;
    } catch (...) {
        return false;
    }
}
bool handleCreateTable(Database &db, const std::string &command)
{
    std::regex create_pattern(
        R"(CREATE\s+TABLE\s+(\w+)\s*\(([\s\S]+)\))",
        std::regex_constants::icase);

    std::smatch matches;
    if (std::regex_search(command, matches, create_pattern))
    {
        std::string table_name = matches[1];
        std::string column_defs = matches[2];

        std::vector<ColumnInfo> columns = parseColumnDefs(column_defs);
        return db.createTable(table_name, columns);
    }
    return false;
}

void executeSelect(Database &db, const std::string &query)
{
    std::vector<std::string> tokens = split(query, ' ');
    if (tokens.size() < 4)
    {
        std::cout << "Invalid SELECT syntax\n";
        return;
    }

    // Find the FROM clause position
    size_t from_pos = query.find("FROM");
    if (from_pos == std::string::npos)
    {
        std::cout << "Invalid SELECT syntax: missing FROM clause\n";
        return;
    }

    // Extract table name and query clauses
    std::string query_clauses = query.substr(from_pos);
    std::vector<std::string> from_tokens = split(query_clauses, ' ');
    if (from_tokens.size() < 2)
    {
        std::cout << "Invalid SELECT syntax: missing table name\n";
        return;
    }

    std::string table_name = from_tokens[1];
    
    // Execute the query with all clauses
    if (!db.selectWithClauses(table_name, query_clauses))
    {
        std::cout << "Error executing SELECT query\n";
    }
}

int main()
{
    Database* current_db = nullptr;
    int current_transaction_id = -1;

    // Regex patterns for transaction commands
    std::regex begin_transaction_pattern(R"(BEGIN\s+TRANSACTION)", std::regex_constants::icase);
    std::regex commit_pattern(R"(COMMIT)", std::regex_constants::icase);
    std::regex rollback_pattern(R"(ROLLBACK)", std::regex_constants::icase);

    std::string input;
    std::string current_db_name;

    std::cout << "Simple DBMS v1.0\n";
    std::cout << "Type 'HELP' for commands\n";

    while (true)
    {
    std::cout << (current_db_name.empty() ? "dbms" : current_db_name) << "> " << std::flush;
        std::getline(std::cin, input);

        // Convert input to uppercase for case-insensitive commands
        std::string upperInput = input;
        std::transform(upperInput.begin(), upperInput.end(), upperInput.begin(), ::toupper);

        if (upperInput == "EXIT")
        {
            break;
        }

        if (upperInput == "HELP")
        {
            printHelp();
            continue;
        }

        try
        {
            std::istringstream iss(input);
            std::string command;
            iss >> command;
            std::transform(command.begin(), command.end(), command.begin(), ::toupper);

            if (command == "CREATE")
            {
                std::string type;
                iss >> type;
                std::transform(type.begin(), type.end(), type.begin(), ::toupper);

                if (type == "DATABASE")
                {
                    std::string db_name;
                    iss >> db_name;
                    if (db_name.empty())
                    {
                        std::cout << "Error: Database name required\n";
                        continue;
                    }

                    if (current_db)
                    {
                        delete current_db;
                    }
                    current_db = new Database(db_name);
                    current_db_name = db_name;
                    std::cout << "Database created: " << db_name << std::endl;
                }
                else if (type == "TABLE")
                {
                    if (!current_db)
                    {
                        std::cout << "Error: No database selected\n";
                        continue;
                    }

                    std::string table_name;
                    iss >> table_name;

                    // Read column definitions
                    std::string columnDefs;
                    std::getline(iss, columnDefs);

                    // Remove parentheses
                    size_t first = columnDefs.find('(');
                    size_t last = columnDefs.find_last_of(')');
                    if (first == std::string::npos || last == std::string::npos)
                    {
                        std::cout << "Error: Invalid column definitions format\n";
                        continue;
                    }

                    columnDefs = columnDefs.substr(first + 1, last - first - 1);
                    std::vector<ColumnInfo> columns = parseColumnDefs(columnDefs);

                    if (columns.empty())
                    {
                        std::cout << "Error: No columns defined\n";
                        continue;
                    }

                    if (current_db->createTable(table_name, columns))
                    {
                        std::cout << "Table created: " << table_name << std::endl;
                    }
                    else
                    {
                        std::cout << "Error creating table\n";
                    }
                }
                else if (type == "INDEX")
                {
                    if (!current_db)
                    {
                        std::cout << "Error: No database selected\n";
                        continue;
                    }

                    std::string on, table_name, column_name;
                    iss >> on;
                    if (on != "ON")
                    {
                        std::cout << "Error: Invalid syntax. Use 'CREATE INDEX ON table(column)'\n";
                        continue;
                    }

                    std::string tableAndColumn;
                    iss >> tableAndColumn;

                    size_t openParen = tableAndColumn.find('(');
                    size_t closeParen = tableAndColumn.find(')');

                    if (openParen == std::string::npos || closeParen == std::string::npos)
                    {
                        std::cout << "Error: Invalid syntax. Use 'CREATE INDEX ON table(column)'\n";
                        continue;
                    }

                    table_name = tableAndColumn.substr(0, openParen);
                    column_name = tableAndColumn.substr(openParen + 1, closeParen - openParen - 1);

                    if (current_db->createIndex(table_name, column_name))
                    {
                        std::cout << "Index created successfully on " << table_name << "(" << column_name << ")\n";

                        // Verify index file exists
                        std::string index_file = "./data/" + current_db_name + "/" + table_name + "_" + column_name + ".idx";
                        std::ifstream f(index_file.c_str());
                        if (f.good())
                        {
                            std::cout << "Index file verified at: " << index_file << std::endl;
                        }
                        else
                        {
                            std::cout << "Warning: Index file not found at: " << index_file << std::endl;
                        }
                    }
                    else
                    {
                        std::cout << "Error creating index\n";
                    }
                }
            }
            else if (command == "USE")
            {
                std::string type, db_name;
                iss >> type >> db_name;
                std::transform(type.begin(), type.end(), type.begin(), ::toupper);

                if (type != "DATABASE")
                {
                    std::cout << "Error: Invalid syntax. Use 'USE DATABASE <name>'\n";
                    continue;
                }

                if (current_db)
                {
                    delete current_db;
                }
                current_db = new Database(db_name);
                current_db_name = db_name;
                std::cout << "Using database: " << db_name << std::endl;
            }
            else if (command == "INSERT")
            {
                if (!current_db)
                {
                    std::cout << "Error: No database selected\n";
                    continue;
                }

                std::string into, table_name, values;
                iss >> into;
                if (into != "INTO")
                {
                    std::cout << "Error: Invalid syntax. Use 'INSERT INTO table VALUES (...)'\n";
                    continue;
                }

                iss >> table_name >> values;

                // Parse values
                size_t first = input.find('(');
                size_t last = input.find_last_of(')');
                if (first == std::string::npos || last == std::string::npos)
                {
                    std::cout << "Error: Invalid values format\n";
                    continue;
                }

                std::string valueStr = input.substr(first + 1, last - first - 1);
                std::vector<std::string> valueTokens = split(valueStr, ',');

                if (current_db->insert(table_name, valueTokens))
                {
                    std::cout << "Record inserted successfully\n";
                }
                else
                {
                    std::cout << "Error inserting record\n";
                }
            }
            else if (command == "SELECT")
            {
                if (!current_db)
                {
                    std::cout << "Error: No database selected\n";
                    continue;
                }

                std::string rest_of_query;
                std::getline(iss, rest_of_query);

                // Check if this is a join query
                if (rest_of_query.find("JOIN") != std::string::npos)
                {
                    // Parse join query (existing code)
                    std::smatch matches;
                    std::regex join_pattern(
                        R"(^\s*\*\s+FROM\s+(\w+)\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+))");

                    if (std::regex_search(rest_of_query, matches, join_pattern))
                    {
                        std::string left_table = matches[1];
                        std::string right_table = matches[2];
                        std::string left_table_col_ref = matches[3];
                        std::string left_column = matches[4];
                        std::string right_table_col_ref = matches[5];
                        std::string right_column = matches[6];

                        if (left_table != left_table_col_ref ||
                            right_table != right_table_col_ref)
                        {
                            std::cout << "Error: Mismatched table references in join\n";
                            continue;
                        }

                        current_db->join(left_table, right_table, left_column, right_column);
                    }
                    else
                    {
                        std::cout << "Error: Invalid join syntax\n";
                        continue;
                    }
                }
                else
                {
                    // New parser for aggregate functions and regular selects
                    std::regex simple_select(R"(^\s*\*\s+FROM\s+(\w+)\s*(WHERE\s+(.*))?)", std::regex_constants::icase);
                    std::regex agg_select(R"(^\s*(COUNT\(\*\)|SUM\((\w+)\)|AVG\((\w+)\)|MIN\((\w+)\)|MAX\((\w+)\))\s+FROM\s+(\w+)\s*(WHERE\s+(.*))?)", std::regex_constants::icase);
                    std::regex group_by_pattern(R"(^\s*(\w+)\s*,\s*(COUNT\(\*\)|SUM\((\w+)\)|AVG\((\w+)\)|MIN\((\w+)\)|MAX\((\w+)\))\s+FROM\s+(\w+)(\s+WHERE\s+(.*?))?(?:\s+GROUP\s+BY\s+(\w+))(?:\s+HAVING\s+(.*?))?\s*$)", std::regex_constants::icase);
                                        
                    std::smatch matches;

                    if (std::regex_search(rest_of_query, matches, simple_select))
                    {
                        // Existing simple SELECT handling
                        std::string table_name = matches[1];
                        std::string where_clause = matches[3];

                        if (where_clause.empty())
                        {
                            current_db->select(table_name);
                        }
                        else
                        {
                            std::vector<std::string> where_parts = split(where_clause, ' ');
                            if (where_parts.size() >= 3)
                            {
                                std::vector<Record> results;
                                current_db->selectWithCondition(table_name, where_parts[0], where_parts[1], where_parts[2], results);
                            }
                        }
                    }
                    else if (std::regex_search(rest_of_query, matches, agg_select))
                    {
                        // Extract components correctly
                        std::string agg_func = matches[1].str();
                        std::string table_name = matches[6].str();
                        std::string where_clause = matches[8].str();

                        std::vector<Record> results;
                        if (where_clause.empty())
                        {
                            current_db->selectWithCondition(table_name, agg_func, "", "", results);
                        }
                        else
                        {
                            std::vector<std::string> where_parts = split(where_clause, ' ');
                            if (where_parts.size() >= 3)
                            {
                                current_db->selectWithCondition(table_name, agg_func, where_parts[0], where_parts[2], results);
                            }
                        }
                    }
                    else if (std::regex_search(rest_of_query, matches, group_by_pattern))
                    {
                        std::string group_column = matches[10].str();  // GROUP BY column
    std::string agg_function = matches[2].str();   // Aggregate function
    std::string table_name = matches[7].str();     // Table name
    std::string where_clause = matches[9].str();   // WHERE clause (may be empty)
                        std::string having_clause = matches[12].str(); // HAVING clause (may be empty)
    
    // Debug output
    std::cout << "DEBUG - Parsed GROUP BY query:\n"
              << "Table: " << table_name << "\n"
              << "Group Column: " << group_column << "\n"
              << "Agg Function: " << agg_function << "\n"
              << "Where Clause: " << (where_clause.empty() ? "(none)" : where_clause) << "\n"
              << "Having Clause: " << (having_clause.empty() ? "(none)" : having_clause) << "\n";
    
              std::vector<Record> results = current_db->groupQuery(
                table_name, group_column, agg_function, where_clause, having_clause);

                        // Display results
                        std::cout << "\nQuery Results (" << results.size() << " groups):\n";
                        std::cout << "----------------------------------------\n";
                        std::cout << std::setw(15) << std::left << group_column << " | ";
                        std::cout << std::setw(15) << std::left << agg_function << "\n";
                        std::cout << "----------------------------------------\n";
                        for (const auto &record : results)
                        {
                            std::cout << std::setw(15) << std::left << record.values[0] << " | ";
                            std::cout << std::setw(15) << std::left << record.values[1] << "\n";
                        }
                        std::cout << "----------------------------------------\n";
                    }

                    else
                    {
                        std::cout << "Invalid SELECT syntax. Supported formats:\n"
                                  << "  SELECT * FROM table [WHERE cond]\n"
                                  << "  SELECT COUNT(*) FROM table\n"
                                  << "  SELECT SUM(column) FROM table\n"
                                  << "  SELECT * FROM table1 JOIN table2 ON...\n";
                    }
                }
            }
            else if (command == "UPDATE")
            {
                if (!current_db)
                {
                    std::cout << "Error: No database selected\n";
                    continue;
                }

                std::string table_name, set, set_clause, where, where_clause;
                iss >> table_name >> set;

                if (set != "SET")
                {
                    std::cout << "Error: Invalid syntax. Use 'UPDATE table SET column = value WHERE condition'\n";
                    continue;
                }

                // Get SET clause
                size_t wherePos = input.find(" WHERE ");
                if (wherePos == std::string::npos)
                {
                    std::cout << "Error: WHERE clause required\n";
                    continue;
                }

                set_clause = input.substr(input.find("SET ") + 4, wherePos - (input.find("SET ") + 4));
                where_clause = input.substr(wherePos + 7);

                if (current_db->update(table_name, set_clause, where_clause))
                {
                    std::cout << "Records updated successfully\n";
                }
                else
                {
                    std::cout << "Error updating records\n";
                }
            }
            else if (command == "DELETE")
            {
                if (!current_db)
                {
                    std::cout << "Error: No database selected\n";
                    continue;
                }

                std::string from, table_name, where, condition;
                iss >> from;

                if (from != "FROM")
                {
                    std::cout << "Error: Invalid syntax. Use 'DELETE FROM table WHERE condition'\n";
                    continue;
                }

                iss >> table_name >> where;
                if (where != "WHERE")
                {
                    std::cout << "Error: WHERE clause required\n";
                    continue;
                }

                std::getline(iss, condition);
                condition = condition.substr(condition.find_first_not_of(" \t"));

                if (current_db->remove(table_name, condition))
                {
                    std::cout << "Records deleted successfully\n";
                }
                else
                {
                    std::cout << "Error deleting records\n";
                }
            }
            else if (command == "DROP")
            {
                if (!current_db)
                {
                    std::cout << "Error: No database selected\n";
                    continue;
                }

                std::string type, table_name;
                iss >> type;
                std::transform(type.begin(), type.end(), type.begin(), ::toupper);

                if (type != "TABLE")
                {
                    std::cout << "Error: Invalid syntax. Use 'DROP TABLE <name>'\n";
                    continue;
                }

                iss >> table_name;
                if (current_db->dropTable(table_name))
                {
                    std::cout << "Table dropped: " << table_name << std::endl;
                }
                else
                {
                    std::cout << "Error dropping table\n";
                }
            }
            else if (command == "BEGIN") {
                std::string type;
                iss >> type;
                std::transform(type.begin(), type.end(), type.begin(), ::toupper);
                
                if (type == "TRANSACTION") {
                    if (!current_db) {
                        std::cout << "Error: No database selected\n";
                        continue;
                    }
                    int transaction_id = current_db->beginTransaction();
                    if (transaction_id != -1) {
                        current_transaction_id = transaction_id;
                        std::cout << "Transaction started with ID: " << transaction_id << std::endl;
                    } else {
                        std::cout << "Failed to start transaction\n";
                    }
                } else {
                    std::cout << "Unknown command. Type 'HELP' for available commands\n";
                }
            }
            else if (command == "COMMIT") {
                if (!current_db) {
                    std::cout << "Error: No database selected\n";
                    continue;
                }
                if (current_transaction_id == -1) {
                    std::cout << "No active transaction\n";
                } else {
                    if (current_db->commitTransaction(current_transaction_id)) {
                        std::cout << "Transaction committed successfully\n";
                        current_transaction_id = -1;
                    } else {
                        std::cout << "Failed to commit transaction\n";
                    }
                }
            }
            else if (command == "ROLLBACK") {
                if (!current_db) {
                    std::cout << "Error: No database selected\n";
                    continue;
                }
                if (current_transaction_id == -1) {
                    std::cout << "No active transaction\n";
                } else {
                    if (current_db->abortTransaction(current_transaction_id)) {
                        std::cout << "Transaction rolled back successfully\n";
                        current_transaction_id = -1;
                    } else {
                        std::cout << "Failed to roll back transaction\n";
                    }
                }
            }
            else if (command == "DROP")   // main
            {
                if (!current_db)
                {
                    std::cout << "Error: No database selected\n";
                    continue;
                }

                std::string type, name;
                iss >> type;
                std::transform(type.begin(), type.end(), type.begin(), ::toupper);

                if (type == "TABLE")
                {
                    iss >> name;
                    if (current_db->dropTable(name))
                    {
                        std::cout << "Table dropped: " << name << std::endl;
                    }
                    else
                    {
                        std::cout << "Error dropping table\n";
                    }
                }
                else if (type == "DATABASE")
                {
                    iss >> name;
                    if (name == current_db_name)
                    {
                        if (current_db->dropDatabase(name))
                        {
                            delete current_db;
                            current_db = nullptr;
                            current_db_name.clear();
                        }
                        else
                        {
                            std::cout << "Error dropping database\n";
                        }
                    }
                    else
                    {
                        std::cout << "Cannot drop database - not currently using it\n";
                    }
                }
                else
                {
                    std::cout << "Error: Invalid syntax. Use 'DROP TABLE <name>' or 'DROP DATABASE <name>'\n";
                }
            }
            else
            {
                std::cout << "Unknown command. Type 'HELP' for available commands\n";
            }
        }
        catch (const std::exception &e)
        {
            std::cout << "Error: " << e.what() << std::endl;
        }
    }

    if (current_db)
    {
        delete current_db;
    }

    return 0;
}