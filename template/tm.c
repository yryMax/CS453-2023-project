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
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdatomic.h>

struct Word {
    // length of each word
    size_t align;
    //whether the word has been written in the current epoch
    atomic_bool written;
    //whether they are 2 transactions has accessed the word in the current epoch
    atomic_bool accessed_by_two;
    //which transaction has accessed the word in the current epoch
    atomic_int accessed_by;
    //if the written word is aborted
    atomic_bool aborted;
};
static struct Word* const invalid_word = NULL;

void word_clean(struct Word *word, void* wordp, size_t align) {
    if(word->written){
        if(!word->aborted)memcpy(wordp, wordp + align, align);
    }
    word->align = align;
    word->written = false;
    word->accessed_by_two = false;
    word->accessed_by = -1;
    word->aborted = false;
}

struct Word* word_init(size_t align) {
    struct Word *word = (struct Word *)malloc(sizeof(struct Word));
    if (word == NULL) return invalid_word;
    memset(word, 0, sizeof(struct Word));
    word->align = align;
    word->written = false;
    word->accessed_by_two = false;
    word->accessed_by = -1;
    word->aborted = false;
    return word;
}

void word_free(struct Word *word) {
    if (word == NULL) return;
    free(word);
}

//atomic global cnt
atomic_int cnt = 1;
struct segment_node {
    struct segment_node* prev;
    struct segment_node* next;
    struct Word** p;
    void* Wordp;
    int64_t node_id;
    int word_length;
    bool free_flag;
};


static struct segment_node* const invalid_segment_node = NULL;
typedef struct segment_node* segment_list;

struct segment_node* segment_node_init(size_t size, size_t align) {
    struct segment_node *node = (struct segment_node *)malloc(sizeof(struct segment_node));
    if (node == NULL) return invalid_segment_node;
    memset(node, 0, sizeof(struct segment_node));
    node->word_length = size / align;
    node->p = (struct Word**)malloc(sizeof(struct Word*) * node->word_length);
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
    node->node_id = atomic_fetch_add(&cnt, 1);
    if(posix_memalign(&(node->Wordp), align, size*2) != 0) {
        for (int i = 0; i < node->word_length; i++) {
            word_free(node->p[i]);
        }
        free(node->p);
        free(node);
        return invalid_segment_node;
    }
    memset(node->Wordp, 0, size*2);
    node->prev = NULL;
    node->next = NULL;
    node->free_flag = false;
    return node;
}

void segment_node_free(struct segment_node *node) {
    if (node == NULL) return;
    for (int i = 0; i < node->word_length; i++) {
        word_free(node->p[i]);
    }
    free(node->Wordp);
    free(node->p);
    free(node);
}


typedef struct Batcher {
    atomic_int counter;
    atomic_int remaining;
    atomic_int blocked;
    pthread_mutex_t lock;
    pthread_cond_t cond;
}Batcher;

typedef struct region {
    segment_list start;
    segment_list allocs;
    size_t size;
    size_t align;
    Batcher* batcher;
}region;

void Batcher_init(Batcher *batcher) {
    atomic_store(&batcher->counter, 0);
    atomic_store(&batcher->remaining, 0);
    atomic_store(&batcher->blocked, 0);
    pthread_mutex_init(&batcher->lock, NULL);
    pthread_cond_init(&batcher->cond, NULL);
}

int get_epoch(Batcher *batcher) {
    return atomic_load(&batcher->counter);
}

void enter(Batcher *batcher) {
    pthread_mutex_lock(&batcher->lock);
    if (atomic_load(&batcher->remaining) == 0) {
        atomic_store(&batcher->remaining, 1);
    } else {
        atomic_fetch_add(&batcher->blocked, 1);
        pthread_cond_wait(&batcher->cond, &batcher->lock);
    }
    pthread_mutex_unlock(&batcher->lock);
}

void leave(Batcher *batcher, region *region) {
    pthread_mutex_lock(&batcher->lock);

    atomic_fetch_sub(&batcher->remaining, 1);
    if (atomic_load(&batcher->remaining) == 0) {
        atomic_fetch_add(&batcher->counter, 1);

        atomic_store(&batcher->remaining, atomic_load(&batcher->blocked));
        atomic_store(&batcher->blocked, 0);
        region_word_init(region);
        pthread_cond_broadcast(&batcher->cond);
    }
    pthread_mutex_unlock(&batcher->lock);
}

void batcher_destroy(Batcher *batcher) {
    pthread_mutex_destroy(&batcher->lock);
    pthread_cond_destroy(&batcher->cond);
    free(batcher);
}



void region_word_init(region* region){
    // printf("cleaning!!!!!!!!!!!!!!!!\n");
    for (int i = 0; i < region->start->word_length; i++) {
        word_clean(region->start->p[i],  region->start->Wordp + i * 2 * region->align, region->align);
    }
    segment_list nodes = region->allocs;
    while(nodes != NULL) {
        if(nodes->free_flag) {
            segment_list nxt = nodes->next;
            if(nodes->prev) nodes->prev->next = nodes->next;
            else region->allocs = nodes->next;
            if(nodes->next) nodes->next->prev = nodes->prev;
            segment_node_free(nodes);
            nodes = nxt;
            continue;
        }
        for (int i = 0; i < nodes->word_length; i++) {
            word_clean(nodes->p[i],  nodes->Wordp + i * 2 * region->align,region->align);
        }
        nodes = nodes->next;
    }

}
void word_rollback(struct Word* word, int tid) {
    //atomic check
    if(atomic_load(&word->written) && atomic_load(&word->accessed_by) == tid) {
        // printf("Dirty Word!!!!!!!!!!!!!!!!\n");
        word->aborted = true;
    }
}

void region_word_rollback(region* region, int tid){
    // printf("cleaning!!!!!!!!!!!!!!!!\n");
    for (int i = 0; i < region->start->word_length; i++) {
        word_rollback(region->start->p[i], tid);
    }
    segment_list nodes = region->allocs;
    while(nodes != NULL) {
        for (int i = 0; i < nodes->word_length; i++) {
            word_rollback(nodes->p[i],  tid);
        }
        nodes = nodes->next;
    }
}

atomic_int tid = 0;
typedef struct Transaction {
    //whether it's read only
    bool is_ro;
    // shared memory region associated with the transaction
    region* shared;
    // if_abort
    bool aborted;
    // id
    int id;
}Transaction;


bool read_word(struct Word* word, Transaction* transaction, char* target, void* wordp) {
    if(transaction->aborted) {
        return false;
    }
    //printf("word readale copy contains: %d\n", *(int*)word->w[0]);
    if(transaction->is_ro){
        memcpy(target,wordp,word->align);
    } else {
        bool wordWritten = atomic_load(&word->written);
        if(wordWritten){

            if(atomic_load(&word->accessed_by) == transaction->id){
                memcpy(target,wordp + word->align,word->align);
            }
            else{
                transaction->aborted = true;
                //rollback
                region_word_rollback(transaction->shared, transaction->id);
                leave(transaction->shared->batcher, transaction->shared);
                return false;
            }
        } else {
            memcpy(target,wordp,word->align);
            if(atomic_load(&word->accessed_by) == transaction->id){
                return true;
            } else if(atomic_load(&word->accessed_by) == -1){
                atomic_store(&word->accessed_by, transaction->id);
            } else {
                atomic_store(&word->accessed_by_two, true);
            }
        }
    }
    return true;
}

bool write_word(struct Word* word, Transaction* transaction, char* source, void* wordp) {
    if(transaction->aborted) {
        return false;
    }
    bool wordWritten = atomic_load(&word->written);
    if(wordWritten){
        if(atomic_load(&word->accessed_by) == transaction->id){
            memcpy(wordp + word->align,source,word->align);
        }
        else{
            transaction->aborted = true;
            region_word_rollback(transaction->shared, transaction->id);
            leave(transaction->shared->batcher, transaction->shared);
            return false;
        }
    } else {
        if(atomic_load(&word->accessed_by_two) || (atomic_load(&word->accessed_by) != transaction->id && atomic_load(&word->accessed_by) != -1)){
            transaction->aborted = true;
            region_word_rollback(transaction->shared, transaction->id);
            leave(transaction->shared->batcher, transaction->shared);
            return false;
        } else {
            memcpy(wordp + word->align,source,word->align);
            atomic_store(&word->written, true);
            atomic_store(&word->accessed_by, transaction->id);
        }
    }
    return true;
}


/** Create (i.e. allocate + init) a new shared memory region, with one first non-free-able allocated segment of the requested size and alignment.
 * @param size  Size of the first shared segment of memory to allocate (in bytes), must be a positive multiple of the alignment
 * @param align Alignment (in bytes, must be a power of 2) that the shared memory region must support
 * @return Opaque shared memory region handle, 'invalid_shared' on failure
**/
shared_t tm_create(size_t unused(size), size_t unused(align)) {
    //printf("size: %d, align: %d\n",size,align);
    struct region* region = (struct region*) malloc(sizeof(struct region));
    memset(region, 0, sizeof(struct region));
    if(region == NULL) {
        return invalid_shared;
    }
    region->start = segment_node_init(size, align);
    if(region->start == NULL) {
        free(region);
        return invalid_shared;
    }
    region->allocs = NULL;
    region->size = size;
    region->align = align;
    region->batcher = (Batcher*) malloc(sizeof(Batcher));
    memset(region->batcher,0, sizeof(Batcher));
    if(region->batcher == NULL) {
        segment_node_free(region->start);
        free(region);
        return invalid_shared;
    }
    Batcher_init(region->batcher);
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t unused(shared)) {
    //printf("tm_destroy");
    struct region* region = (struct region*) shared;
    segment_node_free(region->start);
    while(region->allocs != NULL) {
        segment_list nxt = region->allocs->next;
        segment_node_free(region->allocs);
        region->allocs = nxt;
    }
    batcher_destroy(region->batcher);
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t unused(shared)) {
    return (void*)(1LL<<48);
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t unused(shared)) {
    return ((struct region*) shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t unused(shared)) {
    return ((struct region*) shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t unused(shared), bool unused(is_ro)) {
    //printf("tm_begin\n");
    Transaction* transaction = (struct Transaction*) malloc(sizeof(Transaction));
    memset(transaction,0,sizeof (transaction));
    if(transaction == NULL) {
        return invalid_tx;
    }
    transaction->is_ro = is_ro;
    transaction->shared = (struct region*) shared;
    transaction->aborted = false;
    transaction->id = atomic_fetch_add(&tid, 1);
    enter(transaction->shared->batcher);
    return transaction;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t unused(shared), tx_t unused(tx)) {
    Transaction* transaction = (struct Transaction*) tx;
    region* region = (struct region*) shared;
    bool res = transaction->aborted;
    // printf("transaction %d %s\n", transaction->id, transaction->aborted ? "aborted" : "committed");
    leave(region->batcher, transaction->shared);
    free(transaction);
    return !res;
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

    // printf("tm_read target: %p source: %d size: %d\n", target,source, size);
    target = (char*) target;
    Transaction* transaction = (struct Transaction*) tx;
    if(transaction->aborted) {
        return false;
    }
    int64_t segment_index = (int64_t) source >> (48);
    int64_t offset_size = (int64_t) source & 0xffffffffffff;
    region* region = (struct region*) shared;
    int64_t start_index = offset_size / region->align;
    int64_t duration = size / region->align;
    struct segment_node* node = NULL;
    //printf("reading segment_index: %d start_index: %d\n", segment_index, start_index);
    if(segment_index == 1) {
        node = region->start;
    } else {
        node = region->allocs;
        while(node!=NULL && node->node_id != segment_index) {
            node = node->next;
        }
    }
    for(int i = start_index; i < start_index + duration; i++) {
        //  printf("tm_read targetn: %p\n", target);
        if(!read_word(node->p[i], transaction, target + (i - start_index) * region->align, node->Wordp + i * 2 * region->align)) {
            transaction->aborted = true;
            return false;
        }
    }
    //printf("tm_read target: %p source: %d target_value: %d\n", target,source,*(uint64_t *)target);
    return true;
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

    Transaction* transaction = (struct Transaction*) tx;
    if(transaction->aborted) {
        return false;
    }
    source = (char const*) source;
    int64_t segment_index = (int64_t) target >> (48);
    int64_t offset_size = (int64_t) target & 0xffffffffffff;
    region* region = (struct region*) shared;
    int64_t start_index = offset_size / region->align;
    int64_t duration = size / region->align;
    struct segment_node* node = NULL;
    //   printf("writing segment_index: %d start_index: %d\n", segment_index, start_index);
    if(segment_index == 1) {
        node = region->start;
    } else {

        node = region->allocs;
        while(node!=NULL && node->node_id != segment_index) {
            node = node->next;
        }
        //printf("im here writing\n");
    }
    for(int i = start_index; i < start_index + duration; i++) {
        if(!write_word(node->p[i], transaction, source + (i - start_index) * region->align, node->Wordp + i * 2 * region->align)) {
            //printf("aborted");
            transaction->aborted = true;
            return false;
        }
    }
    //printf("tm_write source: %p target: %d source value: %ld\n", source,target,*(uint64_t *)source);
    return true;
}

/** [thread-safe] Memory allocation in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param size   Allocation requested size (in bytes), must be a positive multiple of the alignment
 * @param target Pointer in private memory receiving the address of the first byte of the newly allocated, aligned segment
 * @return Whether the whole transaction can continue (success/nomem), or not (abort_alloc)
**/
alloc_t tm_alloc(shared_t unused(shared), tx_t unused(tx), size_t unused(size), void** unused(target)) {
    // printf("tm_alloc\n");
    Transaction* transaction = (struct Transaction*) tx;
    if(transaction->aborted) {
        return abort_alloc;
    }
    region* region = (struct region*) shared;
    struct segment_node* node = segment_node_init(size, region->align);
    if (node == NULL) {
        return nomem_alloc;
    }
    if(region->allocs == NULL) {
        region->allocs = node;
    }
    else {
        node->next = region->allocs;
        region->allocs->prev = node;
        region->allocs = node;

    }
    *target = node->node_id << (48);
    return success_alloc;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) {
    // printf("tm_free\n");
    Transaction* transaction = (struct Transaction*) tx;
    if(transaction->aborted) {
        return false;
    }
    int64_t segment_index = (int64_t) target >> (48);
    struct segment_node* node = ((struct region*) shared)->allocs;
    while(node!=NULL && node->node_id != segment_index) {
        node = node->next;
    }
    node->free_flag = true;
    return true;
}
