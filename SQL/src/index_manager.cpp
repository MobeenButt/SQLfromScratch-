#include "index_manager.h"
#include <algorithm>
#include <cstring>
#include <map>
#include <fstream>
#include <iostream>
#include "storage_manager.h"
#include <vector>
#include "database.h"
#include <filesystem>

// Forward declarations
class BPlusTree;
struct IndexNode;

// IndexNode definition
struct IndexNode {
    bool is_leaf;
    int next_leaf;
    std::vector<int> keys;
    std::vector<int> children;
    
    IndexNode() : is_leaf(true), next_leaf(-1) {}
};

// BPlusTree class definition
class BPlusTree {
private:
    StorageManager* storage_manager;
    std::string index_file;
    int order;
    int root_page_id;

    bool insertNonFull(int page_id, int key, int value);
    void splitNode(IndexNode& node, int page_id);
    int searchInNode(int page_id, int key);
    void serializeNode(const IndexNode& node, Page& page);
    void deserializeNode(const Page& page, IndexNode& node);

public:
    BPlusTree(StorageManager* sm, const std::string& filename, int tree_order);
    bool insert(int key, int value);
    int search(int key);
    bool exists(int key);
};

// IndexManager implementation
IndexManager::IndexManager(StorageManager* storage_manager) 
    : storage_manager(storage_manager) {
}

IndexManager::~IndexManager() {
}

bool IndexManager::createIndex(const std::string& db_name,
                             const std::string& table_name,
                             const std::string& column_name) {
    try {
        std::string index_file = "./data/" + db_name + "/" + 
                                table_name + "_" + column_name + ".idx";
        
        std::cout << "Creating index file: " << index_file << std::endl;
        
        // Create the directory if it doesn't exist
        std::filesystem::path dir_path = std::filesystem::path(index_file).parent_path();
        std::filesystem::create_directories(dir_path);
        
        // Create the file first using createTable (which handles file creation)
        if (!storage_manager->createTable(db_name, table_name + "_" + column_name + ".idx")) {
            std::cerr << "Failed to create index file" << std::endl;
            return false;
        }
        
        // Initialize empty root page
        Page empty_page;
        empty_page.setLeaf(true);
        empty_page.setNumKeys(0);
        empty_page.setFreeSpace(PAGE_SIZE_BYTES);
        empty_page.clear();
        
        // Write the empty root page
        if (!storage_manager->writePage(index_file, 0, empty_page)) {
            std::cerr << "Failed to write initial index page" << std::endl;
        return false;
    }
    
        std::cout << "Successfully created index file" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error creating index: " << e.what() << std::endl;
        return false;
    }
}

bool IndexManager::exists(const std::string& index_name, int key) {
    try {
        // Ensure we're using the full path with data directory
        std::string full_path = "./data/" + index_name;
        
        std::vector<IndexRecord> records;
        if (!readIndexRecords(full_path, records)) {
            return false;
        }
        
        // Binary search for the key
        auto it = std::lower_bound(records.begin(), records.end(), key,
            [](const IndexRecord& record, int k) { return record.key < k; });
            
        return (it != records.end() && it->key == key);
    } catch (const std::exception& e) {
        std::cerr << "Error checking if key exists: " << e.what() << std::endl;
        return false;
    }
}

bool IndexManager::dropIndex(const std::string& table_name,
                           const std::string& column_name) {
    std::string index_name = table_name + "_" + column_name + ".idx";
    return storage_manager->dropTable("", index_name);
}

void IndexManager::sortIndexRecords(std::vector<IndexRecord>& records) {
    std::sort(records.begin(), records.end(),
        [](const IndexRecord& a, const IndexRecord& b) { return a.key < b.key; });
}

bool IndexManager::writeIndexRecord(const std::string& index_name, const IndexRecord& record) {
    try {
        // Use the provided path as is since it should already be complete
        std::string full_path = index_name;
        
        std::vector<IndexRecord> records;
        if (!readIndexRecords(full_path, records)) {
            records.clear();
        }
        
        records.push_back(record);
        sortIndexRecords(records);
        
        Page page;
        size_t offset = 0;
        
        for (const auto& r : records) {
            page.writeData(offset, &r, sizeof(IndexRecord));
            offset += sizeof(IndexRecord);
        }
        
        page.setFreeSpace(PAGE_SIZE - offset);
        return storage_manager->writePage(full_path, 0, page);
    } catch (const std::exception& e) {
        std::cerr << "Error writing index record: " << e.what() << std::endl;
        return false;
    }
}

bool IndexManager::readIndexRecords(const std::string& index_name, std::vector<IndexRecord>& records) {
    try {
        // Use the provided path as is since it should already be complete
        std::string full_path = index_name;
        
        Page page;
        if (!storage_manager->readPage(full_path, 0, page)) {
            return false;
        }
        
        size_t record_count = (PAGE_SIZE - page.getFreeSpace()) / sizeof(IndexRecord);
        records.clear();
        
        for (size_t i = 0; i < record_count; i++) {
            IndexRecord record;
            page.readData(i * sizeof(IndexRecord), &record, sizeof(IndexRecord));
            records.push_back(record);
        }
        
    return true;
    } catch (const std::exception& e) {
        std::cerr << "Error reading index records: " << e.what() << std::endl;
        return false;
    }
}

bool IndexManager::insert(const std::string& index_file, int key, [[maybe_unused]] const Record& record) {
    try {
        // Ensure we're using the full path
        std::string full_path = "./data/" + index_file;
        std::cout << "Inserting into index file: " << full_path << std::endl;

        // Create index record
        IndexRecord index_record;
        index_record.key = key;
        index_record.page_id = 0;  // For now, we'll store all records in page 0
        
        // Read existing records
        std::vector<IndexRecord> records;
        if (!readIndexRecords(full_path, records)) {
            std::cout << "No existing records found in index" << std::endl;
            records.clear();
        }
        
        // Check for duplicate keys
        for (const auto& rec : records) {
            if (rec.key == key) {
                std::cerr << "Duplicate key found in index: " << key << std::endl;
                return false;
            }
        }
        
        // Add new record and sort
        records.push_back(index_record);
        sortIndexRecords(records);
        
        // Write back to file
        Page page;
        size_t offset = 0;
        
        for (const auto& rec : records) {
            page.writeData(offset, &rec, sizeof(IndexRecord));
            offset += sizeof(IndexRecord);
        }
        
        page.setFreeSpace(PAGE_SIZE_BYTES - offset);
        bool success = storage_manager->writePage(full_path, 0, page);
        
        if (success) {
            std::cout << "Successfully updated index file with key: " << key << std::endl;
        } else {
            std::cerr << "Failed to write to index file" << std::endl;
        }
        
        return success;
    } catch (const std::exception& e) {
        std::cerr << "Error inserting into index: " << e.what() << std::endl;
        return false;
    }
}

bool IndexManager::search(const std::string& index_file,
                        const std::string& op,
                        int value,
                        std::vector<int>& result) {
    try {
        std::vector<IndexRecord> records;
        result.clear();
        
        if (!readIndexRecords(index_file, records)) {
            std::cerr << "Failed to read index records from: " << index_file << std::endl;
            return false;
        }

        std::cout << "Found " << records.size() << " records in index" << std::endl;

        // Sort records for efficient searching
        sortIndexRecords(records);

        // Perform search based on operator
        if (op == "=") {
            searchEqual(records, value, result);
        } else if (op == ">") {
            searchGreaterThan(records, value, result, false);
        } else if (op == ">=") {
            searchGreaterThan(records, value, result, true);
        } else if (op == "<") {
            searchLessThan(records, value, result, false);
        } else if (op == "<=") {
            searchLessThan(records, value, result, true);
        } else if (op == "!=") {
            searchNotEqual(records, value, result);
        }

        std::cout << "Found " << result.size() << " matching records" << std::endl;
    return true;
    } catch (const std::exception& e) {
        std::cerr << "Error in index search: " << e.what() << std::endl;
        return false;
    }
}

void IndexManager::searchEqual(const std::vector<IndexRecord>& records,
                             int value,
                             std::vector<int>& result) {
    for (const auto& record : records) {
        if (record.key == value) {
            result.push_back(record.key);
            break;  // Since keys are unique for primary key indexes
        }
    }
}

void IndexManager::searchGreaterThan(const std::vector<IndexRecord>& records,
                                   int value,
                                   std::vector<int>& result,
                                   bool include_equal) {
    for (const auto& record : records) {
        if ((include_equal && record.key >= value) ||
            (!include_equal && record.key > value)) {
            result.push_back(record.key);
        }
    }
}

void IndexManager::searchLessThan(const std::vector<IndexRecord>& records,
                                int value,
                                std::vector<int>& result,
                                bool include_equal) {
    for (const auto& record : records) {
        if ((include_equal && record.key <= value) ||
            (!include_equal && record.key < value)) {
            result.push_back(record.key);
        }
    }
}

void IndexManager::searchNotEqual(const std::vector<IndexRecord>& records,
                                int value,
                                std::vector<int>& result) {
    for (const auto& record : records) {
        if (record.key != value) {
            result.push_back(record.key);
        }
    }
}

// BPlusTree implementation
BPlusTree::BPlusTree(StorageManager* sm, const std::string& filename, int tree_order) 
    : storage_manager(sm), index_file(filename), order(tree_order) {
    try {
        std::cout << "Initializing B+ tree for file: " << filename << std::endl;
        
        Page root_page;
        bool root_exists = storage_manager->readPage(index_file, 0, root_page);
        
        if (!root_exists) {
            std::cout << "Creating new root node" << std::endl;
    // Initialize empty root node
    IndexNode root;
    root.is_leaf = true;
    root.next_leaf = -1;
    
            // Initialize the page
            root_page.clear();
            root_page.setFreeSpace(PAGE_SIZE);
            
            serializeNode(root, root_page);
            if (!storage_manager->writePage(index_file, 0, root_page)) {
                throw std::runtime_error("Failed to write initial root page");
            }
        }
    root_page_id = 0;
        std::cout << "B+ tree initialized successfully" << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Error initializing B+ tree: " << e.what() << std::endl;
        throw;
    }
}

bool BPlusTree::exists(int key) {
    return search(key) != -1;
}

bool BPlusTree::insert(int key, int value) {
    try {
        std::cout << "Inserting key: " << key << " with value: " << value << std::endl;
        
        // Read root page
        Page root_page;
        if (!storage_manager->readPage(index_file, root_page_id, root_page)) {
            throw std::runtime_error("Failed to read root page during insert");
        }

    IndexNode root;
    deserializeNode(root_page, root);

        // If root is full, create new root
        if (static_cast<int>(root.keys.size()) == (2 * order - 1)) {
            std::cout << "Splitting root node" << std::endl;
            
            // Create new root
        IndexNode new_root;
        new_root.is_leaf = false;
        new_root.children.push_back(root_page_id);

            // Save old root
            Page old_root_page;
            serializeNode(root, old_root_page);
            if (!storage_manager->writePage(index_file, root_page_id + 1, old_root_page)) {
                throw std::runtime_error("Failed to write old root during split");
            }

            // Split old root
            splitNode(root, root_page_id + 1);

            // Save new root
            Page new_root_page;
            serializeNode(new_root, new_root_page);
            if (!storage_manager->writePage(index_file, root_page_id, new_root_page)) {
                throw std::runtime_error("Failed to write new root");
            }
    }

    return insertNonFull(root_page_id, key, value);
    } catch (const std::exception& e) {
        std::cerr << "Error in insert: " << e.what() << std::endl;
        return false;
    }
}

bool BPlusTree::insertNonFull(int page_id, int key, int value) {
    try {
    Page current_page;
        if (!storage_manager->readPage(index_file, page_id, current_page)) {
            throw std::runtime_error("Failed to read page during insert");
        }

    IndexNode node;
    deserializeNode(current_page, node);

        // If this is a leaf node
    if (node.is_leaf) {
            // Insert key and value
            auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
            size_t pos = it - node.keys.begin();
            
            node.keys.insert(it, key);
            node.children.insert(node.children.begin() + pos, value);

        serializeNode(node, current_page);
            return storage_manager->writePage(index_file, page_id, current_page);
        }

        // If this is not a leaf node
        int i = node.keys.size() - 1;
        while (i >= 0 && key < node.keys[i]) {
            i--;
        }
        i++;

        Page child_page;
        if (!storage_manager->readPage(index_file, node.children[i], child_page)) {
            throw std::runtime_error("Failed to read child page");
        }

        IndexNode child;
        deserializeNode(child_page, child);

        if (static_cast<int>(child.keys.size()) == (2 * order - 1)) {
            splitNode(child, node.children[i]);
            if (key > node.keys[i]) {
                i++;
            }
        }

        return insertNonFull(node.children[i], key, value);
    } catch (const std::exception& e) {
        std::cerr << "Error in insertNonFull: " << e.what() << std::endl;
        return false;
    }
}

void BPlusTree::splitNode(IndexNode& node, int page_id) {
    try {
    IndexNode new_node;
    new_node.is_leaf = node.is_leaf;

        int mid = order - 1;
    
    // Move half of the keys to new node
    new_node.keys.assign(node.keys.begin() + mid + 1, node.keys.end());
    node.keys.resize(mid);

    // Move half of the children to new node
    if (!node.is_leaf) {
        new_node.children.assign(node.children.begin() + mid + 1, 
                               node.children.end());
        node.children.resize(mid + 1);
    }

    // Handle leaf node connections
    if (node.is_leaf) {
        new_node.next_leaf = node.next_leaf;
        node.next_leaf = page_id + 1;
    }

    // Save both nodes
    Page page, new_page;
    serializeNode(node, page);
    serializeNode(new_node, new_page);
        
        if (!storage_manager->writePage(index_file, page_id, page)) {
            throw std::runtime_error("Failed to write split node");
        }
        if (!storage_manager->writePage(index_file, page_id + 1, new_page)) {
            throw std::runtime_error("Failed to write new split node");
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in splitNode: " << e.what() << std::endl;
        throw;
    }
}

int BPlusTree::search(int key) {
    try {
    return searchInNode(root_page_id, key);
    } catch (const std::exception& e) {
        std::cerr << "Error in search: " << e.what() << std::endl;
        return -1;
    }
}

int BPlusTree::searchInNode(int page_id, int key) {
    Page current_page;
    if (!storage_manager->readPage(index_file, page_id, current_page)) {
        throw std::runtime_error("Failed to read page during search");
    }

    IndexNode node;
    deserializeNode(current_page, node);

    if (node.is_leaf) {
        auto it = std::lower_bound(node.keys.begin(), node.keys.end(), key);
        if (it != node.keys.end() && *it == key) {
            return node.children[it - node.keys.begin()];
        }
        return -1;
    }

    int i = 0;
    while (i < static_cast<int>(node.keys.size()) && key > node.keys[i]) {
        i++;
    }

    return searchInNode(node.children[i], key);
}

void BPlusTree::serializeNode(const IndexNode& node, Page& page) {
    try {
        // Clear the page
        page.clear();
        
        // Write is_leaf flag
        page.writeData(0, &node.is_leaf, sizeof(bool));
        
        // Write next_leaf
        page.writeData(sizeof(bool), &node.next_leaf, sizeof(int));
        
        // Write number of keys
    int num_keys = node.keys.size();
        page.writeData(sizeof(bool) + sizeof(int), &num_keys, sizeof(int));
    
        // Write keys
        size_t offset = sizeof(bool) + 2 * sizeof(int);
    for (int key : node.keys) {
            page.writeData(offset, &key, sizeof(int));
            offset += sizeof(int);
    }
    
        // Write number of children
    int num_children = node.children.size();
        page.writeData(offset, &num_children, sizeof(int));
        offset += sizeof(int);
    
        // Write children
    for (int child : node.children) {
            page.writeData(offset, &child, sizeof(int));
            offset += sizeof(int);
        }
        
        page.setFreeSpace(PAGE_SIZE - offset);
    } catch (const std::exception& e) {
        std::cerr << "Error in serializeNode: " << e.what() << std::endl;
        throw;
    }
}

void BPlusTree::deserializeNode(const Page& page, IndexNode& node) {
    try {
        // Read is_leaf flag
        page.readData(0, &node.is_leaf, sizeof(bool));
    
        // Read next_leaf
        page.readData(sizeof(bool), &node.next_leaf, sizeof(int));
    
        // Read number of keys
        int num_keys;
        page.readData(sizeof(bool) + sizeof(int), &num_keys, sizeof(int));
    
        // Read keys
        size_t offset = sizeof(bool) + 2 * sizeof(int);
    node.keys.clear();
    for (int i = 0; i < num_keys; i++) {
            int key;
            page.readData(offset, &key, sizeof(int));
            node.keys.push_back(key);
            offset += sizeof(int);
        }
        
        // Read number of children
        int num_children;
        page.readData(offset, &num_children, sizeof(int));
        offset += sizeof(int);
        
        // Read children
    node.children.clear();
    for (int i = 0; i < num_children; i++) {
            int child;
            page.readData(offset, &child, sizeof(int));
            node.children.push_back(child);
            offset += sizeof(int);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in deserializeNode: " << e.what() << std::endl;
        throw;
    }
}