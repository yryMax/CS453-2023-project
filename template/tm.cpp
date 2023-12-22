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
#include <tm.hpp>
#include "macros.h"
#include <thread>
#include <pthread.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <map>
#include <iostream>
#include <set>
#include <mutex>
using namespace std;


struct Word {
    // length of each word
    size_t align;
    //whether the word has been written in the current epoch
    atomic<int> written;
    //whether they are 2 transactions has accessed the word in the current epoch
    atomic<bool> accessed_by_two;
    //which transaction has accessed the word in the current epoch
    atomic<int> accessed_by;
    //if the written word is aborted
    atomic<bool> aborted;
    long long addr;

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
atomic<int> cnt = 1;
struct segment_node {
    struct Word** p;
    void* Wordp;
    long long node_id;
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
    int counter;
    int remaining;
    int blocked;
    set<long long>modified_word;
    pthread_mutex_t lock;
    pthread_cond_t cond;
}Batcher;

mutex modified_word_mutex;
void insert_to_modified(Batcher *batcher, long long word_addr) {
    modified_word_mutex.lock();
//cout<<"insert_to_modified: "<<word_addr<<endl;
    batcher->modified_word.insert(word_addr);
    modified_word_mutex.unlock();
}


typedef struct region {
    map<long long, struct segment_node*> block;
    size_t size;
    size_t align;
    Batcher* batcher;
}region;

void region_word_init(region* region);
void region_word_next(region* region);
void Batcher_init(Batcher *batcher) {
    batcher->counter = 0;
    batcher->remaining = 0;
    batcher->blocked = 0;
    batcher->modified_word = set<long long>();
    pthread_mutex_init(&batcher->lock, NULL);
    pthread_cond_init(&batcher->cond, NULL);
}

int get_epoch(Batcher *batcher) {
    return batcher->counter;
}

void enter(Batcher *batcher) {
    pthread_mutex_lock(&batcher->lock);
    if (batcher->remaining == 0) {
        batcher->remaining = 1;
    } else {
        batcher->blocked++;
        pthread_cond_wait(&batcher->cond, &batcher->lock);
    }
    pthread_mutex_unlock(&batcher->lock);
}

void leave(Batcher *batcher, region *region) {
    pthread_mutex_lock(&batcher->lock);

    batcher->remaining--;
    if (batcher->remaining == 0) {
        batcher->counter++;

        batcher->remaining = batcher->blocked;
        batcher->blocked = 0;
        region_word_next(region);
        batcher->modified_word.clear();
        pthread_cond_broadcast(&batcher->cond);
    }
    pthread_mutex_unlock(&batcher->lock);
}

void batcher_destroy(Batcher *batcher) {
    pthread_mutex_destroy(&batcher->lock);
    pthread_cond_destroy(&batcher->cond);
    //free modified_word
    batcher->modified_word.clear();
    free(batcher);
}



void region_word_init(region* region){
    for(auto it = region->block.begin(); it != region->block.end();) {

        struct segment_node* node = it->second;
        if (node->free_flag) {
            //cout<<"region_word_init"<<endl;
            it = region->block.erase(it);
            segment_node_free(node);
            continue;
        }
        for (int i = 0; i < node->word_length; i++) {
            word_clean(node->p[i], node->Wordp + i * 2 * region->align, region->align);
            long long addr = (node->node_id << (48)) + i;
            node->p[i]->addr = addr;
        }
        ++it;
    }

}



void region_word_next(region* region){
    for(auto it = region->batcher->modified_word.begin(); it != region->batcher->modified_word.end(); it++) {
        long long segment_index = *it >> (48);
        long long start_index = *it & 0xffffffffffff;
        struct segment_node* node = region->block[segment_index];
        if(node == NULL) {
            continue;
        }
        word_clean(node->p[start_index], node->Wordp + start_index * 2 * region->align, region->align);
    }

    for(auto it = region->block.begin(); it != region->block.end();) {
        struct segment_node* node = it->second;
        if (node->free_flag) {
            it = region->block.erase(it);
            segment_node_free(node);
            continue;
        }
        ++it;
    }

}



void word_rollback(struct Word* word, int tid) {
    //atomic check
    if(atomic_load(&word->written) && atomic_load(&word->accessed_by) == tid) {
        word->aborted = true;
    }
}

void region_word_rollback(region* region, int tid){
    //travel all the modified word
    for(auto it = region->batcher->modified_word.begin(); it != region->batcher->modified_word.end(); it++) {
        long long segment_index = *it >> (48);
        long long start_index = *it & 0xffffffffffff;
        struct segment_node* node = region->block[segment_index];
        if(node == NULL) {
            continue;
        }
        word_rollback(node->p[start_index], tid);
    }
}

atomic<int> tid = 0;
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


bool read_word(struct Word* word, Transaction* transaction, char* target, void* wordp, Batcher* batcher) {
    if(transaction->aborted) {
        return false;
    }
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

bool write_word(struct Word* word, Transaction* transaction, char* source, void* wordp, Batcher* batcher) {
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
shared_t tm_create(size_t unused(size), size_t unused(align)) noexcept {
    //printf("size: %d, align: %d\n",size,align);
    struct region* region = (struct region*) malloc(sizeof(struct region));
    memset(region, 0, sizeof(struct region));
    if(region == NULL) {
        return invalid_shared;
    }
    struct segment_node* st = segment_node_init(size, align);
    if(st == NULL) {
        free(region);
        return invalid_shared;
    }

    region->block = map<long long, struct segment_node*>();
    region->block[st->node_id] = st;
    region->size = size;
    region->align = align;
    region->batcher = (Batcher*) malloc(sizeof(Batcher));
    memset(region->batcher,0, sizeof(Batcher));
    if(region->batcher == NULL) {
        segment_node_free(st);
        free(region);
        return invalid_shared;
    }
    Batcher_init(region->batcher);
    return region;
}

/** Destroy (i.e. clean-up + free) a given shared memory region.
 * @param shared Shared memory region to destroy, with no running transaction
**/
void tm_destroy(shared_t unused(shared)) noexcept {
    //printf("tm_destroy");
    struct region* region = (struct region*) shared;
    for(auto it = region->block.begin(); it != region->block.end(); it++) {
        struct segment_node *nodes = it->second;
        for (int i = 0; i < nodes->word_length; i++) {
            word_free(nodes->p[i]);
        }
    }
    batcher_destroy(region->batcher);
    free(region);
}

/** [thread-safe] Return the start address of the first allocated segment in the shared memory region.
 * @param shared Shared memory region to query
 * @return Start address of the first allocated segment
**/
void* tm_start(shared_t unused(shared)) noexcept {
    return (void*)(1LL<<48);
}

/** [thread-safe] Return the size (in bytes) of the first allocated segment of the shared memory region.
 * @param shared Shared memory region to query
 * @return First allocated segment size
**/
size_t tm_size(shared_t unused(shared)) noexcept {
    return ((struct region*) shared)->size;
}

/** [thread-safe] Return the alignment (in bytes) of the memory accesses on the given shared memory region.
 * @param shared Shared memory region to query
 * @return Alignment used globally
**/
size_t tm_align(shared_t unused(shared)) noexcept {
    return ((struct region*) shared)->align;
}

/** [thread-safe] Begin a new transaction on the given shared memory region.
 * @param shared Shared memory region to start a transaction on
 * @param is_ro  Whether the transaction is read-only
 * @return Opaque transaction ID, 'invalid_tx' on failure
**/
tx_t tm_begin(shared_t unused(shared), bool unused(is_ro)) noexcept {
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
    return (tx_t) transaction;
}

/** [thread-safe] End the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to end
 * @return Whether the whole transaction committed
**/
bool tm_end(shared_t unused(shared), tx_t unused(tx)) noexcept {
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
bool tm_read(shared_t unused(shared), tx_t unused(tx), void const* unused(source), size_t unused(size), void* unused(target)) noexcept {
   //cout<<"tm_read"<<endl;
    target = (char*) target;
    Transaction* transaction = (struct Transaction*) tx;
    if(transaction->aborted) {
        return false;
    }
    long long segment_index = (long long) source >> (48);
    long long offset_size = (long long) source & 0xffffffffffff;
    region* region = (struct region*) shared;
    long long start_index = offset_size / region->align;
    long long duration = size / region->align;
    struct segment_node* node = region->block[segment_index];
    for(int i = start_index; i < start_index + duration; i++) {
        long long addr = (node->node_id << (48)) + i;
        insert_to_modified(region->batcher, addr);
        if(!read_word(node->p[i], transaction, (char *)(target + (i - start_index) * region->align), node->Wordp + i * 2 * region->align, region->batcher)) {
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
bool tm_write(shared_t unused(shared), tx_t unused(tx), void const* unused(source), size_t unused(size), void* unused(target)) noexcept {

    //cout<<"tm_write"<<endl;
    Transaction* transaction = (struct Transaction*) tx;
    if(transaction->aborted) {
        return false;
    }
    source = (char const*) source;
    long long segment_index = (long long) target >> (48);
    long long offset_size = (long long) target & 0xffffffffffff;
    region* region = (struct region*) shared;
    long long start_index = offset_size / region->align;
    long long duration = size / region->align;
    struct segment_node* node = region->block[segment_index];
    for(int i = start_index; i < start_index + duration; i++) {
        long long addr = (node->node_id << (48)) + i;
        insert_to_modified(region->batcher, addr);
        if(!write_word(node->p[i], transaction, (char*)(source + (i - start_index) * region->align), node->Wordp + i * 2 * region->align, region->batcher)) {
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
Alloc tm_alloc(shared_t unused(shared), tx_t unused(tx), size_t unused(size), void** unused(target)) noexcept {
    //cout<<"tm_alloc"<<endl;
    Transaction* transaction = (struct Transaction*) tx;
    if(transaction->aborted) {
        return Alloc::abort;
    }
    region* region = (struct region*) shared;
    struct segment_node* node = segment_node_init(size, region->align);
    if (node == NULL) {
        return Alloc::nomem;
    }
    region->block[node->node_id] = node;
    *target = (void* )(node->node_id << (48));
    return Alloc::success;
}

/** [thread-safe] Memory freeing in the given transaction.
 * @param shared Shared memory region associated with the transaction
 * @param tx     Transaction to use
 * @param target Address of the first byte of the previously allocated segment to deallocate
 * @return Whether the whole transaction can continue
**/
bool tm_free(shared_t unused(shared), tx_t unused(tx), void* unused(target)) noexcept {
   // cout<<"tm_free"<<endl;
    Transaction* transaction = (struct Transaction*) tx;
    if(transaction->aborted) {
        return false;
    }
    long long segment_index = (long long) target >> (48);
    struct segment_node* node = ((struct region*) shared)->block[segment_index];
    node->free_flag = true;
    return true;
}
