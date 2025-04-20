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

bool IndexManager::createIndex(const std::string& table_name, 
                             const std::string& column_name,
                             TableInfo* table_info) {
    try {
        // Get the database name from the table's data file path
        std::string data_file = table_info->data_file;
        size_t start = data_file.find("/data/") + 6;
        size_t end = data_file.find("/", start);
        std::string db_name = data_file.substr(start, end - start);
        
        std::string index_name = table_name + "_" + column_name + ".idx";
        std::string full_index_path = "./data/" + db_name + "/" + index_name;
        
        std::cout << "Creating index at path: " << full_index_path << std::endl;

        // Initialize index file with empty page
        Page empty_page = {};
        empty_page.free_space = PAGE_SIZE;
        
        if (!storage_manager->writePage(full_index_path, 0, empty_page)) {
            throw std::runtime_error("Failed to initialize index file");
        }

        // Calculate record size
        size_t record_size = 0;
        for (const auto& col : table_info->columns) {
            if (col.type == "INT") record_size += sizeof(int);
            else if (col.type == "VARCHAR") record_size += col.size;
        }

        // Find the index of the column we're indexing
        int column_index = -1;
        for (size_t i = 0; i < table_info->columns.size(); i++) {
            if (table_info->columns[i].name == column_name) {
                column_index = i;
                break;
            }
        }

        if (column_index == -1) {
            throw std::runtime_error("Column not found");
        }

        // Read existing records and create index entries
        std::vector<IndexRecord> records;
        Page data_page;
        int page_id = 0;

        while (storage_manager->readPage(table_info->data_file, page_id, data_page)) {
            if (data_page.free_space == PAGE_SIZE) {
                page_id++;
                continue;
            }

            size_t used_space = PAGE_SIZE - data_page.free_space;
            size_t num_records = used_space / record_size;
            size_t offset = 0;

            for (size_t i = 0; i < num_records; i++) {
                size_t col_offset = 0;
                for (size_t j = 0; j < table_info->columns.size(); j++) {
                    if (static_cast<int>(j) == column_index) {
                        int key_value;
                        memcpy(&key_value, data_page.data + offset + col_offset, sizeof(int));
                        
                        IndexRecord record;
                        record.key = key_value;
                        record.page_id = page_id;
                        records.push_back(record);
                        break;
                    }
                    if (table_info->columns[j].type == "INT") {
                        col_offset += sizeof(int);
                    } else {
                        col_offset += table_info->columns[j].size;
                    }
                }
                offset += record_size;
            }
            page_id++;
        }

        // Sort records by key
        std::sort(records.begin(), records.end(),
            [](const IndexRecord& a, const IndexRecord& b) { return a.key < b.key; });

        // Write sorted records to index file
        Page index_page = {};
        size_t index_offset = 0;

        for (const auto& record : records) {
            if (index_offset + sizeof(IndexRecord) > PAGE_SIZE) {
                // Current page is full, write it and start a new one
                index_page.free_space = PAGE_SIZE - index_offset;
                if (!storage_manager->writePage(full_index_path, 0, index_page)) {
                    throw std::runtime_error("Failed to write index page");
                }
                index_page = {};
                index_offset = 0;
            }

            memcpy(index_page.data + index_offset, &record, sizeof(IndexRecord));
            index_offset += sizeof(IndexRecord);
        }

        // Write the last page if it contains any records
        if (index_offset > 0) {
            index_page.free_space = PAGE_SIZE - index_offset;
            if (!storage_manager->writePage(full_index_path, 0, index_page)) {
                throw std::runtime_error("Failed to write final index page");
            }
        }

        std::cout << "Successfully built index with " << records.size() << " entries" << std::endl;
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error creating index: " << e.what() << std::endl;
        return false;
    }
}

bool IndexManager::insertEntry(const std::string& index_name, int key, int page_id) {
    try {
        IndexRecord record{key, page_id};
        
        // Read existing records
        std::vector<IndexRecord> records;
        if (!readIndexRecords(index_name, records)) {
            records.clear();
        }
        
        // Add new record
        records.push_back(record);
        
        // Sort records
        sortIndexRecords(records);
        
        // Write back all records
        Page page = {};
        size_t offset = 0;
        
        for (const auto& r : records) {
            memcpy(page.data + offset, &r, sizeof(IndexRecord));
            offset += sizeof(IndexRecord);
        }
        
        page.free_space = PAGE_SIZE - offset;
        return storage_manager->writePage(index_name, 0, page);
    } catch (const std::exception& e) {
        std::cerr << "Error inserting index entry: " << e.what() << std::endl;
        return false;
    }
}

int IndexManager::searchEntry(const std::string& index_name, int key) {
    try {
        std::vector<IndexRecord> records;
        if (!readIndexRecords(index_name, records)) {
            return -1;
        }
        
        // Binary search for the key
        auto it = std::lower_bound(records.begin(), records.end(), key,
            [](const IndexRecord& record, int k) { return record.key < k; });
            
        if (it != records.end() && it->key == key) {
            return it->page_id;
        }
        
        return -1;
    } catch (const std::exception& e) {
        std::cerr << "Error searching index: " << e.what() << std::endl;
        return -1;
    }
}

bool IndexManager::exists(const std::string& index_name, int key) {
    return searchEntry(index_name, key) != -1;
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
        std::vector<IndexRecord> records;
        if (!readIndexRecords(index_name, records)) {
            records.clear();
        }
        
        records.push_back(record);
        sortIndexRecords(records);
        
        Page page = {};
        size_t offset = 0;
        
        for (const auto& r : records) {
            memcpy(page.data + offset, &r, sizeof(IndexRecord));
            offset += sizeof(IndexRecord);
        }
        
        page.free_space = PAGE_SIZE - offset;
        return storage_manager->writePage(index_name, 0, page);
    } catch (const std::exception& e) {
        std::cerr << "Error writing index record: " << e.what() << std::endl;
        return false;
    }
}

bool IndexManager::readIndexRecords(const std::string& index_name, std::vector<IndexRecord>& records) {
    try {
        Page page;
        if (!storage_manager->readPage(index_name, 0, page)) {
            return false;
        }
        
        size_t record_count = (PAGE_SIZE - page.free_space) / sizeof(IndexRecord);
        records.clear();
        
        for (size_t i = 0; i < record_count; i++) {
            IndexRecord record;
            memcpy(&record, page.data + (i * sizeof(IndexRecord)), sizeof(IndexRecord));
            records.push_back(record);
        }
        
        return true;
    } catch (const std::exception& e) {
        std::cerr << "Error reading index records: " << e.what() << std::endl;
        return false;
    }
}

// BPlusTree implementation
BPlusTree::BPlusTree(StorageManager* sm, const std::string& filename, int tree_order) 
    : storage_manager(sm), index_file(filename), order(tree_order) {
    try {
        std::cout << "Initializing B+ tree for file: " << filename << std::endl;
        
        Page root_page = {};  // Value-initialize instead of memset
        bool root_exists = storage_manager->readPage(index_file, 0, root_page);
        
        if (!root_exists) {
            std::cout << "Creating new root node" << std::endl;
            // Initialize empty root node
            IndexNode root;
            root.is_leaf = true;
            root.next_leaf = -1;
            
            // Initialize the page
            root_page = {};  // Value-initialize instead of memset
            root_page.free_space = PAGE_SIZE;
            
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
        page = {};  // Value-initialize instead of memset
        
        // Write is_leaf flag
        memcpy(page.data, &node.is_leaf, sizeof(bool));
        
        // Write next_leaf
        memcpy(page.data + sizeof(bool), &node.next_leaf, sizeof(int));
        
        // Write number of keys
        int num_keys = node.keys.size();
        memcpy(page.data + sizeof(bool) + sizeof(int), &num_keys, sizeof(int));
        
        // Write keys
        size_t offset = sizeof(bool) + 2 * sizeof(int);
        for (int key : node.keys) {
            memcpy(page.data + offset, &key, sizeof(int));
            offset += sizeof(int);
        }
        
        // Write number of children
        int num_children = node.children.size();
        memcpy(page.data + offset, &num_children, sizeof(int));
        offset += sizeof(int);
        
        // Write children
        for (int child : node.children) {
            memcpy(page.data + offset, &child, sizeof(int));
            offset += sizeof(int);
        }
        
        // Update free space
        page.free_space = PAGE_SIZE - offset;
    } catch (const std::exception& e) {
        std::cerr << "Error in serializeNode: " << e.what() << std::endl;
        throw;
    }
}

void BPlusTree::deserializeNode(const Page& page, IndexNode& node) {
    try {
        // Read is_leaf flag
        memcpy(&node.is_leaf, page.data, sizeof(bool));
        
        // Read next_leaf
        memcpy(&node.next_leaf, page.data + sizeof(bool), sizeof(int));
        
        // Read number of keys
        int num_keys;
        memcpy(&num_keys, page.data + sizeof(bool) + sizeof(int), sizeof(int));
        
        // Read keys
        size_t offset = sizeof(bool) + 2 * sizeof(int);
        node.keys.clear();
        for (int i = 0; i < num_keys; i++) {
            int key;
            memcpy(&key, page.data + offset, sizeof(int));
            node.keys.push_back(key);
            offset += sizeof(int);
        }
        
        // Read number of children
        int num_children;
        memcpy(&num_children, page.data + offset, sizeof(int));
        offset += sizeof(int);
        
        // Read children
        node.children.clear();
        for (int i = 0; i < num_children; i++) {
            int child;
            memcpy(&child, page.data + offset, sizeof(int));
            node.children.push_back(child);
            offset += sizeof(int);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in deserializeNode: " << e.what() << std::endl;
        throw;
    }
}