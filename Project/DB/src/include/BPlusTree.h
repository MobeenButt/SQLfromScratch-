#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <iostream>
#include <vector>
#include <fstream>
#include <cstring>
#include <stdexcept>

#define FANOUT 4 // Maximum number of keys a node can hold

struct BPlusNode {
    bool is_leaf;
    int key_count;
    int keys[FANOUT - 1];
    int pointers[FANOUT];
    int parent;
    int next_leaf;

    BPlusNode(bool leaf = false) : is_leaf(leaf), key_count(0), parent(-1), next_leaf(-1) {
        std::memset(keys, 0, sizeof(keys));
        std::memset(pointers, -1, sizeof(pointers));
    }
};

class BPlusTree {
private:
    std::fstream file;
    int root_offset;

    BPlusNode read_node(int offset);
    void write_node(int offset, const BPlusNode& node);
    int find_leaf(int key);
    void split_node(BPlusNode& node, int offset);
    void insert_into_parent(int parent_offset, int new_key, int left_offset, int right_offset);
    void merge_nodes(int left_offset, int right_offset, int parent_offset);
    void borrow_or_merge(int node_offset);

public:
    BPlusTree(const std::string& index_file);
    ~BPlusTree();
    void insert(int key, int data_offset);
    std::vector<int> search(int key);
    bool remove(int key);
    std::vector<int> range_search(int start_key, int end_key);

};

#endif
