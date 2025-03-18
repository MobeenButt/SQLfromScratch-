// BPlusTree.h
#ifndef BPLUSTREE_H
#define BPLUSTREE_H

#include <map>
#include <string>

class BPlusTree {
private:
   map<int,string> tree;

public:
    void insert(int key,string value);
   string search(int key);
    void remove(int key);
};

#endif
