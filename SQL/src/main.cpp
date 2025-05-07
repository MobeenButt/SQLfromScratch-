#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>
#include <fstream>
#include "database.h"
#include <regex>

void printHelp()
{
    std::cout << "Available commands:\n"
              << "CREATE DATABASE <name>\n"
              << "USE DATABASE <name>\n"
              << "CREATE TABLE <name> (<column_name> <type>(size),...)\n"
              << "DROP TABLE <name>\n"
              << "INSERT INTO <table> VALUES (value1, value2, ...)\n"
              << "SELECT * FROM <table> [WHERE column_name = value]\n"
              << "SELECT <column>, COUNT(*) FROM <table> [GROUP BY column]\n"
              << "SELECT SUM(<column>) FROM <table>\n"
              << "UPDATE <table> SET column = value WHERE condition\n"
              << "DELETE FROM <table> WHERE condition\n"
              << "CREATE INDEX ON <table>(<column>)\n"
              << "SELECT * FROM <table1> \n"
              << "SELECT AVG(<column>) FROM <table>\n"
              << "SELECT MIN(<column>) FROM <table>\n"
              << "SELECT <column>, COUNT(*) FROM <table> GROUP BY <column>\n"
              << "SELECT <column>, SUM(<column>) FROM <table> GROUP BY <column>\n"
              << "SELECT <column>, AVG(<column>) FROM <table> GROUP BY <column>\n"
              << "SELECT <column>, COUNT(*) FROM <table> GROUP BY <column>\n"
              << "SELECT <column>, SUM(<column>) FROM <table> GROUP BY <column>\n"
              << "SELECT <column>, AVG(<column>) FROM <table> GROUP BY <column>\n"
              << "SELECT <column>, MIN(<column>) FROM <table> GROUP BY <column>\n"
              << "SELECT <column>, MAX(<column>) FROM <table> GROUP BY <column>\n"
              << "SELECT <col>, COUNT(*) FROM <table> GROUP BY <col> HAVING COUNT(*) > 5\n"
              << "SELECT <col>, SUM(<col>) FROM <table> GROUP BY <col> HAVING SUM(...) > 100\n"
              << "JOIN <table2> ON <table1.column> = <table2.column>\n"
              << "HELP\n"
              << "EXIT\n";
              
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

    std::string table_name = tokens[3];

    if (tokens.size() >= 8 && tokens[4] == "WHERE")
    {
        // SELECT with condition
        std::string column_name = tokens[5];
        std::string op = tokens[6];
        std::string value = tokens[7];

        std::vector<Record> results;
        if (db.selectWithCondition(table_name, column_name, op, value, results))
        {
            // Results are already displayed in the select method
        }
        else
        {
            std::cout << "Error executing SELECT query\n";
        }
    }
    else if (tokens.size() == 4)
    {
        // Simple SELECT *
        db.select(table_name); // Results are displayed inside the method
    }
    else
    {
        std::cout << "Invalid SELECT syntax\n";
    }
}

int main()
{
    std::string input;
    Database *current_db = nullptr;
    std::string current_db_name;

    std::cout << "Simple DBMS v1.0\n";
    std::cout << "Type 'HELP' for commands\n";

    while (true)
    {
        std::cout << (current_db_name.empty() ? "dbms" : current_db_name) << "> ";
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
                    std::regex group_by_pattern(R"(^\s*(\w+)\s*,\s*(COUNT\(\*\)|SUM\((\w+)\)|AVG\((\w+)\)|MIN\((\w+)\)|MAX\((\w+)\))\s+FROM\s+(\w+)(\s+WHERE\s+(.*?))?(\s+HAVING\s+(.*?))?\s+GROUP\s+BY\s+(\w+)\s*$)", std::regex_constants::icase);
                                        
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
                        std::string group_column = matches[12].str();  // GROUP BY column (index 12)
    std::string agg_function = matches[2].str();   // Aggregate function
    std::string table_name = matches[7].str();     // Table name
    std::string where_clause = matches[9].str();   // WHERE clause (may be empty)
    std::string having_clause = matches[11].str(); // HAVING clause (may be empty)
    
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