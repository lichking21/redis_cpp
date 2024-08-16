#pragma once

#include <iostream>
#include <cstddef>
#include <cstdint>

struct Hnode {

    Hnode* next = nullptr;
    uint64_t hcode = 0;
};

struct Htab {

    Hnode** tab = nullptr;
    size_t mask = 0;
    size_t size = 0;
};

struct Hmap {

    Htab t1;
    Htab t2;
    size_t resizing_pos = 0;
};

Hnode* map_find(Hmap* map, Hnode* key, bool(*eq)(Hnode*, Hnode*));
void map_insert(Hmap* map, Hnode* node);
Hnode* map_pop(Hmap* map, Hnode* key, bool(*eq)(Hnode*, Hnode*));
size_t map_size(Hmap* map);
void map_destroy(Hmap* map);