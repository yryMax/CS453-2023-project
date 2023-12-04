/**
 * @file   tm.c
 * @author [...]
 *
 * @section LICENSE
 *
 * [...]
 *
 * @section DESCRIPTION
 *
 * Implementation of your own transaction manager.
 * You can completely rewrite this file (and create more files) as you wish.
 * Only the interface (i.e. exported symbols and semantic) must be preserved.
**/

// Requested features
#define _GNU_SOURCE
#define _POSIX_C_SOURCE   200809L
#ifdef __STDC_NO_ATOMICS__
    #error Current C11 compiler does not support atomic operations
#endif

// External headers

// Internal headers
#include <tm.h>
#include "macros.h"


struct Word {
    void* w[2];
    //which copy is valid from the previous epoch
    int valid;
    // length of each word
    size_t align;
    //whether the word has been written in the current epoch
    bool written;
    //whether they are 2 transactions has accessed the word in the current epoch
    bool accessed_by_two;
    //which transaction has accessed the word in the current epoch
    tx_t accessed_by;
};
static Word* const invalid_word = NULL;

void word_clean(struct Word *word, size_t align) {
    if (word->w[0]) memset(word->w[0], 0, align);
    if (word->w[1]) memset(word->w[1], 0, align);
    word->valid = 0;
    word->align = align;
    word->written = false;
    word->accessed_by_two = false;
    word->accessed_by = NULL;
}

Word* word_init(size_t align) {
    Word *word = (Word *)malloc(sizeof(Word));
    if (word == NULL) return invalid_word;
    if(posix_memalign(&(word->w[0]), align, align) != 0) {
        free(word);
        return invalid_word;
    }
    if(posix_memalign(&(word->w[1]), align, align) != 0) {
        free(word->w[0]);
        free(word);
        return invalid_word;
    }
    if(word->w[0] == NULL || word->w[1] == NULL) {
        free(word->w[0]);
        free(word->w[1]);
        free(word);
        return invalid_word;
    }
    word_clean(word, align);
    return word;
}

void word_free(Word *word) {
    if (word == NULL) return;
    free(word->w[0]);
    free(word->w[1]);
    free(word);
}


struct segment_node {
    segment_node* prev;
    segment_node* next;
    //pointer to the word list pointer
    Word** word_list;
    int word_length;
};
static segment_node* const invalid_segment_node = NULL;
typedef struct segment_node* segment_list;

segment_node* segment_node_init(size_t size, size_t align) {
    segment_node *node = (segment_node *)malloc(sizeof(segment_node));
    if (node == NULL) return invalid_segment_node;
    node->word_length = size / align;
    node->p = (Word **)malloc(sizeof(Word *) * node->word_length);
    if (node->p == NULL) {
        free(node);
        return invalid_segment_node;
    }
    for (int i = 0; i < node->word_length; i++) {
        node->p[i] = word_init(align);
        if (node->p[i] == NULL) {
            for (int j = 0; j < i; j++) {
                word_free(node->p[j]);
            }
            free(node->p);
            free(node);
            return invalid_segment_node;
        }
    }
    node->prev = NULL;
    node->next = NULL;
    return node;
}

void segment_node_free(segment_node *node) {
    if (node == NULL) return;
    for (int i = 0; i < node->word_length; i++) {
        word_free(node->p[i]);
    }
    free(node->p);
    free(node);
}

struct tx_t {
    //whether it's read only
    bool is_ro;
    // shared memory region associated with the transaction
    shared_t shared;
};

struct region {
    void* start;
    segment_list allocs;
    size_t size;
    size_t align;
};
/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t unused(size), size_t unused(align)) {
    // TODO: tm_create(size_t, size_t)
    return invalid_shared;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t unused(shared)) {
    // TODO: tm_destroy(shared_t)
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t unused(shared)) {
    // TODO: tm_start(shared_t)
    return NULL;
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t unused(shared)) {
    // TODO: tm_size(shared_t)
    return 0;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t unused(shared)) {
    // TODO: tm_align(shared_t)
    return 0;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t unused(shared), bool unused(is_ro)) {
    // TODO: tm_begin(shared_t)
    return invalid_tx;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t unused(shared), tx_t unused(tx)) {
    // TODO: tm_end(shared_t, tx_t)
    return false;
}

/** [thread-safe] Read operation in the given transaction, source in the shared region and target in a private region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in the shared region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in a private region)
 * @return Whether the whole transaction can continue
**/
bool tm_read(shared_t unused(shared), tx_t unused(tx), void const* unused(source), size_t unused(size), void* unused(target)) {
    // TODO: tm_read(shared_t, tx_t, void const*, size_t, void*)
    return false;
}

/** [thread-safe] Write operation in the given transaction, source in a private region and target in the shared region.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param source Source start address (in a private region)
 * @param size   Length to copy (in bytes), must be a positive multiple of the alignment
 * @param target Target start address (in the shared region)
 * @return Whether the whole transaction can continue
**/
bool tm_write(shared_t unused(shared), tx_t unused(tx), void const* unused(source), size_t unused(size), void* unused(target)) {
    // TODO: tm_write(shared_t, tx_t, void const*, size_t, void*)
    return false;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t unused(shared), tx_t unused(tx), size_t unused(size), void** unused(target)) {
    // TODO: tm_alloc(shared_t, tx_t, size_t, void**)
    return abort_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) {
    // TODO: tm_free(shared_t, tx_t, void*)
    return false;
}
