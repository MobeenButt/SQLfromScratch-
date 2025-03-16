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

#endif
