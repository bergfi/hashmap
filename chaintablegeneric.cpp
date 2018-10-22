#include "chaintable.h"

__thread slab<HashTableEntry>* HashTableEntry::_slab;
