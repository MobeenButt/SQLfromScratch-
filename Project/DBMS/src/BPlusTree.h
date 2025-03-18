<<<<<<< Updated upstream
<<<<<<< Updated upstream
#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <vector>
#include <map>
#include <string>
#include <fstream>
#include "json.hpp" // Include JSON for storage

using json = nlohmann::json;

const int ORDER = 4; // B+ Tree order (can be adjusted)

class BPlusTree {
private:
    struct Node {
        bool isLeaf;
        std::vector<int> keys;
        std::vector<std::string> values;  // Only used for leaf nodes
        std::vector<Node*> children;
        Node* nextLeaf;  // For fast range queries
        Node* parent; // Added parent pointer

        Node(bool leaf);
        json serialize();
        static Node* deserialize(const json& j);
    };

    Node* root;

    void insertInternal(int key, Node* leftChild, Node* rightChild);
    void splitLeaf(Node* leaf);
    void splitInternal(Node* node);
    void saveToFile();
    void loadFromFile();
    void printTree(Node* node, int level);
    void display();
    void mergeLeaf(Node* leaf);
    void mergeInternal(Node* node);


public:
    BPlusTree();  // Constructor
    ~BPlusTree(); // Destructor
    void insert(int key, std::string value);
    void remove(int key);
    std::string search(int key);
};

=======
=======
>>>>>>> Stashed changes
// #ifndef BPLUSTREE_H
// #define BPLUSTREE_H

// #include <iostream>
// #include <vector>
// #include <fstream>

// class BPlusTreeNode {
// public:
//     bool isLeaf;
//     std::vector<int> keys;
//     std::vector<BPlusTreeNode*> children;
//     std::vector<std::string> values;
//     BPlusTreeNode* nextLeaf;

//     BPlusTreeNode(bool leaf);
// };

// class BPlusTree {
// private:
//     BPlusTreeNode* root;
//     int maxKeys;

//     void insertInternal(int key, BPlusTreeNode* parent, BPlusTreeNode* child);
//     void splitLeafNode(BPlusTreeNode* leaf, int key, const std::string& value);
//     void splitInternalNode(BPlusTreeNode* node);
//     BPlusTreeNode* findParent(BPlusTreeNode* root, BPlusTreeNode* child);

// public:
//     BPlusTree(int order);
//     void insert(int key, const std::string& value);
//     std::string search(int key);
//     void remove(int key);
//     void display();
//     void deleteTree(BPlusTreeNode* node);

//     // Disk I/O
//     // void saveToDisk();
//     // void loadFromDisk();
//     void saveToDisk(const std::string& filename);
//     void loadFromDisk(const std::string& filename);
    
// void saveNodeToDisk(std::ofstream& file, BPlusTreeNode* node);
// BPlusTreeNode* loadNodeFromDisk(std::ifstream& file);

// };

// #endif

#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <iostream>
#include <vector>
#include <fstream>

class BPlusTreeNode {
public:
    bool isLeaf;
    std::vector<int> keys;
    std::vector<BPlusTreeNode*> children;
    std::vector<std::string> values;
    BPlusTreeNode* nextLeaf;

    BPlusTreeNode(bool leaf);
};

class BPlusTree {
private:
    BPlusTreeNode* root;
    int maxKeys;

    void insertInternal(int key, BPlusTreeNode* parent, BPlusTreeNode* child);
    void splitLeafNode(BPlusTreeNode* leaf, int key, const std::string& value);
    void splitInternalNode(BPlusTreeNode* node);
    BPlusTreeNode* findParent(BPlusTreeNode* root, BPlusTreeNode* child);
    void saveNodeToDisk(std::ofstream& file, BPlusTreeNode* node);
    BPlusTreeNode* loadNodeFromDisk(std::ifstream& file);
    void deleteTree(BPlusTreeNode* node);

public:
    BPlusTree():BPlusTree(4){}
    BPlusTree(int order);
    ~BPlusTree();  // Destructor
    void insert(int key, const std::string& value);
    std::string search(int key);
    void remove(int key);
    void display();

    // Disk I/O
    void saveToDisk(const std::string& filename);
    void loadFromDisk(const std::string& filename);
};
<<<<<<< Updated upstream
>>>>>>> Stashed changes
=======
>>>>>>> Stashed changes
#endif
