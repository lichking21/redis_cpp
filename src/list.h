#pragma once

#include <iostream>

class Dlist {
public:
    Dlist* prev = nullptr;
    Dlist* next = nullptr;
};

inline void dlist_init(Dlist* node){
    node->prev = node->next = node;
}

inline bool dlist_empty(Dlist* node){
    return node->next == node;
}

inline void dlist_detach(Dlist* node){

    Dlist* prev = node->prev;
    Dlist* next = node->next;

    prev->next = next;
    next->prev = prev;
}

inline void dlist_insert(Dlist* target, Dlist* newbie){

    Dlist* prev = target->prev;
    prev->next = newbie;
    newbie->prev = prev;
    newbie->next = target;
    target->prev = newbie;
}