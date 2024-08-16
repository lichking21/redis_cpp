#pragma once

#include <iostream>
#include <cstdint>

class Heap_Item {
public:
    uint64_t val = 0;
    size_t* ref = nullptr;
};

void heap_upd(Heap_Item* a, size_t pos, size_t len);