#include "include/BPlusTree.h"

/**
 * @brief BPlusTree Constructor
 */
BPlusTree::BPlusTree(const std::string& index_file) {
    file.open(index_file, std::ios::in | std::ios::out | std::ios::binary);

    if (!file) {
        file.open(index_file, std::ios::out | std::ios::binary | std::ios::trunc);
        if (!file) throw std::runtime_error("Failed to create index file.");
        file.close();
        file.open(index_file, std::ios::in | std::ios::out | std::ios::binary);

        // Initialize root node
        BPlusNode root(true);
        root_offset = sizeof(int);
        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&root_offset), sizeof(int));
        write_node(root_offset, root);
    } else {
        file.seekg(0);
        file.read(reinterpret_cast<char*>(&root_offset), sizeof(int));
    }
}

/**
 * @brief BPlusTree Destructor
 */
BPlusTree::~BPlusTree() {
    if (file.is_open()) {
        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&root_offset), sizeof(int));
        file.flush();
        file.close();
    }
}

/**
 * @brief Reads a node from disk
 */
BPlusNode BPlusTree::read_node(int offset) {
    BPlusNode node;
    file.seekg(offset);
    file.read(reinterpret_cast<char*>(&node), sizeof(BPlusNode));
    if (!file) throw std::runtime_error("Failed to read node from file.");
    return node;
}

/**
 * @brief Writes a node to disk
 */
void BPlusTree::write_node(int offset, const BPlusNode& node) {
    file.seekp(offset);
    file.write(reinterpret_cast<const char*>(&node), sizeof(BPlusNode));
    file.flush();
    if (!file) throw std::runtime_error("Failed to write node to file.");
}

/**
 * @brief Finds the leaf node containing a key.
 */
int BPlusTree::find_leaf(int key) {
    if (root_offset == -1) return -1;
    int offset = root_offset;

    while (true) {
        BPlusNode node = read_node(offset);
        if (node.is_leaf) return offset;

        int i = 0;
        while (i < node.key_count && key >= node.keys[i]) i++;
        offset = node.pointers[i];
    }
}

/**
 * @brief Inserts a key into the B+ Tree.
 */
void BPlusTree::insert(int key, int data_offset) {
    int leaf_offset = find_leaf(key);
    if (leaf_offset == -1) {
        BPlusNode root(true);
        root.keys[0] = key;
        root.pointers[1] = data_offset;
        root.key_count = 1;
        root_offset = sizeof(int);
        write_node(root_offset, root);
        return;
    }

    BPlusNode leaf = read_node(leaf_offset);
    int i = leaf.key_count - 1;

    while (i >= 0 && leaf.keys[i] > key) {
        leaf.keys[i + 1] = leaf.keys[i];
        leaf.pointers[i + 2] = leaf.pointers[i + 1];
        i--;
    }
    leaf.keys[i + 1] = key;
    leaf.pointers[i + 2] = data_offset;
    leaf.key_count++;

    if (leaf.key_count < FANOUT - 1) {
        write_node(leaf_offset, leaf);
    } else {
        split_node(leaf, leaf_offset);
    }
}

/**
 * @brief Splits a node when full.
 */
void BPlusTree::split_node(BPlusNode& node, int offset) {
    int mid = node.key_count / 2;
    BPlusNode new_node(node.is_leaf);
    
    for (int i = mid, j = 0; i < node.key_count; i++, j++) {
        new_node.keys[j] = node.keys[i];
        new_node.pointers[j] = node.pointers[i];
    }
    new_node.pointers[new_node.key_count] = node.pointers[node.key_count];

    new_node.key_count = node.key_count - mid;
    node.key_count = mid;

    file.seekp(0, std::ios::end);
    int new_offset = file.tellp();

    if (node.is_leaf) {
        new_node.next_leaf = node.next_leaf;
        node.next_leaf = new_offset;
    }

    write_node(offset, node);
    write_node(new_offset, new_node);

    insert_into_parent(node.parent, new_node.keys[0], offset, new_offset);
}

/**
 * @brief Inserts key into parent node after split.
 */
void BPlusTree::insert_into_parent(int parent_offset, int new_key, int left_offset, int right_offset) {
    if (parent_offset == -1) {
        BPlusNode new_root(false);
        new_root.keys[0] = new_key;
        new_root.pointers[0] = left_offset;
        new_root.pointers[1] = right_offset;
        new_root.key_count = 1;

        file.seekp(0, std::ios::end);
        int new_root_offset = file.tellp();
        write_node(new_root_offset, new_root);

        root_offset = new_root_offset;
        file.seekp(0);
        file.write(reinterpret_cast<const char*>(&root_offset), sizeof(int));
        file.flush();
        return;
    }

    BPlusNode parent = read_node(parent_offset);
    int i = parent.key_count - 1;

    while (i >= 0 && parent.keys[i] > new_key) {
        parent.keys[i + 1] = parent.keys[i];
        parent.pointers[i + 2] = parent.pointers[i + 1];
        i--;
    }

    parent.keys[i + 1] = new_key;
    parent.pointers[i + 2] = right_offset;
    parent.key_count++;

    if (parent.key_count < FANOUT - 1) {
        write_node(parent_offset, parent);
    } else {
        split_node(parent, parent_offset);
    }
}
