#pragma once
#include <vector>
#include <string>
#include <cstring>

const size_t PAGE_SIZE_BYTES = 4096;  // 4KB page size

class Page {
public:
    Page() {
        clear();
    }

    void clear() {
        is_leaf = false;
        num_keys = 0;
        free_space = PAGE_SIZE_BYTES;
        memset(data, 0, PAGE_SIZE_BYTES);
    }

    // Getters
    bool isLeaf() const { return is_leaf; }
    int getNumKeys() const { return num_keys; }
    size_t getFreeSpace() const { return free_space; }
    const char* getData() const { return data; }
    char* getData() { return data; }
    size_t getNumRecords() const { return num_keys; }  // For now, use num_keys as num_records

    // Setters
    void setLeaf(bool leaf) { is_leaf = leaf; }
    void setNumKeys(int keys) { num_keys = keys; }
    void setFreeSpace(size_t space) { free_space = space; }

    // Data manipulation methods
    bool writeData(size_t offset, const void* src, size_t size) {
        if (offset + size > PAGE_SIZE_BYTES) return false;
        memcpy(data + offset, src, size);
        return true;
    }

    bool readData(size_t offset, void* dest, size_t size) const {
        if (offset + size > PAGE_SIZE_BYTES) return false;
        memcpy(dest, data + offset, size);
        return true;
    }

    bool moveData(size_t dest_offset, size_t src_offset, size_t size) {
        if (dest_offset + size > PAGE_SIZE_BYTES || 
            src_offset + size > PAGE_SIZE_BYTES) return false;
        memmove(data + dest_offset, data + src_offset, size);
        return true;
    }

private:
    bool is_leaf;
    int num_keys;
    size_t free_space;
    char data[PAGE_SIZE_BYTES];
}; 