#include <iostream>
#include <cassert>
#include "hashtable.h"

static void h_init(Htab* tab, size_t n){

    assert(n > 0 && ((n - 1) & n) == 0);
    tab->tab = (Hnode**)calloc(sizeof(Hnode*), n);
    tab->mask = n - 1;
    tab->size = 0;
}

static void h_insert(Htab* tab, Hnode* node){

    size_t pos = node->hcode & tab->mask;
    Hnode* next = tab->tab[pos];
    node->next = next;
    tab->tab[pos] = node;
    tab->size++;
}

static Hnode** h_find(Htab* tab, Hnode* key, bool (*eq)(Hnode*, Hnode*)){

    if (!tab->tab)
        return nullptr;
    
    size_t pos = key->hcode & tab->mask;
    Hnode** from = &tab->tab[pos];
    for (Hnode* cur; (cur = *from) != nullptr; from = &cur->next){

        if (cur->hcode == key->hcode && eq(cur, key))
            return from;
    }

    return nullptr;
}

static Hnode* h_detach(Htab* tab, Hnode** from){

    Hnode* node = *from;
    *from = node->next;
    tab->size--;
    return node;
}

const size_t resizing_work = 128;

static void map_help_resize(Hmap* map){

    size_t nwork = 0;
    while(nwork < resizing_work && map->t2.size > 0){

        Hnode** from = &map->t2.tab[map->resizing_pos];
        if (!*from){

            map->resizing_pos++;
            continue;
        }

        h_insert(&map->t1, h_detach(&map->t2, from));
        nwork++;
    }

    if (map->t2.size == 0 && map->t2.tab){

        free(map->t2.tab);
        map->t2 = Htab{};
    }
}

static void map_start_resize(Hmap* map){

    assert(map->t2.tab == nullptr);

    map->t2 = map->t1;
    h_init(&map->t1, (map->t1.mask + 1) * 2);
    map->resizing_pos = 0;
}

Hnode* map_find(Hmap* map, Hnode* key, bool(*eq)(Hnode*, Hnode*)){

    map_help_resize(map);
    Hnode** from = h_find(&map->t1, key, eq);
    from = from ? from : h_find(&map->t2, key, eq);
    return from ? *from : nullptr;
}

const size_t max_load_factor = 8;

void map_insert(Hmap* map, Hnode* node){

    if (!map->t1.tab)
        h_init(&map->t1, 4);

    h_insert(&map->t1, node);

    if (!map->t1.tab){

        size_t load_factor = map->t1.size / (map->t1.mask + 1);
        if (load_factor >= max_load_factor)
            map_start_resize(map);
    }

    map_help_resize(map);
}

Hnode* map_pop(Hmap* map, Hnode* key, bool (eq)(Hnode*, Hnode*)){

    map_help_resize(map);
    if (Hnode** from = h_find(&map->t1, key, eq))
        return h_detach(&map->t1, from);
    if (Hnode** from = h_find(&map->t2, key, eq))
        return h_detach(&map->t2, from);

    return nullptr;
}

size_t map_size(Hmap* map){
    return map->t1.size + map->t2.size;
}

void map_destroy(Hmap* map){

    free(map->t1.tab);
    free(map->t2.tab);
    *map = Hmap{};
}