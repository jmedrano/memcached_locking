/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "memcached.h"
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/signal.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>

/* Forward Declarations */
static void item_link_q(item *it, int l);
static void item_unlink_q(item *it, int l);
static int do_item_unlink(item *it, int l);

/*
 * We only reposition items in the LRU queue if they haven't been repositioned
 * in this many seconds. That saves us from churning on frequently-accessed
 * items.
 */
#define ITEM_UPDATE_INTERVAL 60

#define LRU_COUNT 32
#define LRU_MASK ((LRU_COUNT) - 1)

#define REFCNT_COUNT 256
#define REFCNT_LOCK_MASK ((REFCNT_COUNT) - 1)

#define LARGEST_ID 255
typedef struct {
    unsigned int evicted;
    rel_time_t evicted_time;
    unsigned int outofmemory;
    unsigned int tailrepairs;
} itemstats_t;

static pthread_mutex_t lru_locks[LARGEST_ID][LRU_COUNT];
static item *heads[LARGEST_ID][LRU_COUNT];
static item *tails[LARGEST_ID][LRU_COUNT];
static itemstats_t itemstats[LARGEST_ID][LRU_COUNT];
static unsigned int sizes[LARGEST_ID][LRU_COUNT];

void item_init(void) {
    int i, l;
    memset(itemstats, 0, sizeof(itemstats_t) * LARGEST_ID);
    for(i = 0; i < LARGEST_ID; i++) {
        for(l = 0; l < LRU_COUNT; l++) {
            heads[i][l] = NULL;
            tails[i][l] = NULL;
            sizes[i][l] = 0;
            pthread_mutex_init(&lru_locks[i][l], NULL);
        }
    }
}

void item_stats_reset(void) {
    int i, l;
    for (i=0; i<LARGEST_ID; i++) {
        for (l=0; l<LRU_COUNT; l++) {
            pthread_mutex_lock(&lru_locks[i][l]);
            memset(&itemstats[i][l], 0, sizeof(itemstats_t));
            pthread_mutex_unlock(&lru_locks[i][l]);
        }
    }
}


/* Get the next CAS id for a new item. */
uint64_t get_cas_id(void) {
    static uint64_t cas_id = 0;
    return ++cas_id;
}

/* Enable this for reference-count debugging. */
#if 0
# define DEBUG_REFCNT(it,op) \
                fprintf(stderr, "item %x refcnt(%c) %d %c%c%c\n", \
                        it, op, it->refcount, \
                        (it->it_flags & ITEM_LINKED) ? 'L' : ' ', \
                        (it->it_flags & ITEM_SLABBED) ? 'S' : ' ')
#else
# define DEBUG_REFCNT(it,op) while(0)
#endif

/**
 * Generates the variable-sized part of the header for an object.
 *
 * key     - The key
 * nkey    - The length of the key
 * flags   - key flags
 * nbytes  - Number of bytes to hold value and addition CRLF terminator
 * suffix  - Buffer for the "VALUE" line suffix (flags, size).
 * nsuffix - The length of the suffix is stored here.
 *
 * Returns the total size of the header.
 */
static size_t item_make_header(const uint8_t nkey, const int flags, const int nbytes,
                     char *suffix, uint8_t *nsuffix) {
    /* suffix is defined at 40 chars elsewhere.. */
    *nsuffix = (uint8_t) snprintf(suffix, 40, " %d %d\r\n", flags, nbytes - 2);
    return sizeof(item) + nkey + *nsuffix + nbytes;
}

/*@null@*/
item *item_alloc(char *key, const size_t nkey, const int flags, const rel_time_t exptime, const int nbytes) {
    uint8_t nsuffix;
    item *it = NULL;
    char suffix[40];
    size_t ntotal = item_make_header(nkey + 1, flags, nbytes, suffix, &nsuffix);
    if (settings.use_cas) {
        ntotal += sizeof(uint64_t);
    }

    unsigned int id = slabs_clsid(ntotal);
    if (id == 0)
        return 0;

    /* do a quick check if we have any expired items in the tail.. */
    int tries = 50;
    item *search;

    int l = hash(key, nkey, 0) & LRU_MASK;
    pthread_mutex_lock(&lru_locks[id][l]);
    for (search = tails[id][l];
         tries > 0 && search != NULL;
         tries--, search=search->prev) {
        if (search->exptime != 0 && search->exptime < current_time) {
            // don't need lock. this check is tentative
            if (atomic_read(&search->refcount) == 1) {
                /* I don't want to actually free the object, just steal
                 * the item to avoid to grab the slab mutex twice ;-)
                 */
                if (do_item_unlink(search, l)) {
                    /* Initialize the item block: */
                    it = search;
                    it->slabs_clsid = 0;
                    it->refcount = 1;
                    break;
                }
                // another thread got a reference. it'll release the item afterwards
            }
        }
    }
    pthread_mutex_unlock(&lru_locks[id][l]);

    if (it == NULL && (it = slabs_alloc(ntotal, id)) == NULL) {
        /*
        ** Could not find an expired item at the tail, and memory allocation
        ** failed. Try to evict some items!
        */
        tries = 50;

        /* If requested to not push old items out of cache when memory runs out,
         * we're out of luck at this point...
         */

        if (settings.evict_to_free == 0) {
            pthread_mutex_lock(&lru_locks[id][l]);
            itemstats[id][l].outofmemory++;
            pthread_mutex_unlock(&lru_locks[id][l]);
            return NULL;
        }

        /*
         * try to get one off the right LRU
         * don't necessariuly unlink the tail because it may be locked: refcount>1
         * search up from tail an item with refcount==0 and unlink it; give up after 50
         * tries
         */

        pthread_mutex_lock(&lru_locks[id][l]);
        if (tails[id][l] == 0) {
            itemstats[id][l].outofmemory++;
            pthread_mutex_unlock(&lru_locks[id][l]);
            return NULL;
        }

        for (search = tails[id][l]; tries > 0 && search != NULL; tries--, search=search->prev) {
            if (atomic_read(&search->refcount) == 1) {
                /* I don't want to actually free the object, just steal
                 * the item to avoid to grab the slab mutex twice ;-)
                 */
                if (do_item_unlink(search, l)) {
                    /* Initialize the item block: */
                    it = search;
                    it->slabs_clsid = 0;
                    it->refcount = 1;
                    atomic_inc(&stats.evictions);
                    break;
                }
                // another thread got a reference. it'll release the item afterwards
            }
        }
        pthread_mutex_unlock(&lru_locks[id][l]);
        if (it == 0) {
            itemstats[id][l].outofmemory++;
            /* Last ditch effort. There is a very rare bug which causes
             * refcount leaks. We've fixed most of them, but it still happens,
             * and it may happen in the future.
             * We can reasonably assume no item can stay locked for more than
             * three hours, so if we find one in the tail which is that old,
             * free it anyway.
             */
            tries = 50;
            pthread_mutex_lock(&lru_locks[id][l]);
            for (search = tails[id][l]; tries > 0 && search != NULL; tries--, search=search->prev) {
                if (atomic_read(&search->refcount) > 1 && search->time + TAIL_REPAIR_TIME < current_time) {
                    search->refcount = 1;
                    itemstats[id][l].tailrepairs++;
                    do_item_unlink(search, l);
                    it = search;
                    break;
                }
            }
            pthread_mutex_unlock(&lru_locks[id][l]);
            if (it == 0) {
                return NULL;
            }
        }
    }

    assert(it->slabs_clsid == 0);

    it->slabs_clsid = id;

    assert(it != heads[it->slabs_clsid][l]);

    it->next = it->prev = it->h_next = 0;
    it->refcount = 1;     /* the caller will have a reference */
    DEBUG_REFCNT(it, '*');
    it->it_flags = settings.use_cas ? ITEM_CAS : 0;
    it->nkey = nkey;
    it->nbytes = nbytes;
    memcpy(ITEM_key(it), key, nkey);
    it->exptime = exptime;
    memcpy(ITEM_suffix(it), suffix, (size_t)nsuffix);
    it->nsuffix = nsuffix;
    return it;
}

void item_free(item *it) {
    size_t ntotal = ITEM_ntotal(it);
    unsigned int clsid;
    assert((it->it_flags & ITEM_LINKED) == 0);
    // Better not to access lru lists here
    //assert(it != heads[it->slabs_clsid][l]);
    //assert(it != tails[it->slabs_clsid][l]);
    assert(it->refcount == 0);

    /* so slab size changer can tell later if item is already free or not */
    clsid = it->slabs_clsid;
    it->slabs_clsid = 0;
    it->it_flags |= ITEM_SLABBED;
    DEBUG_REFCNT(it, 'F');
    slabs_free(it, ntotal, clsid);
}

/**
 * Returns true if an item will fit in the cache (its size does not exceed
 * the maximum for a cache entry.)
 */
bool item_size_ok(const size_t nkey, const int flags, const int nbytes) {
    char prefix[40];
    uint8_t nsuffix;

    return slabs_clsid(item_make_header(nkey + 1, flags, nbytes,
                                        prefix, &nsuffix)) != 0;
}

/* Always called witch lock held */
static void item_link_q(item *it, int l) { /* item is the new head */
    item **head, **tail;
    /* always true, warns: assert(it->slabs_clsid <= LARGEST_ID); */
    assert((it->it_flags & ITEM_SLABBED) == 0);

    head = &heads[it->slabs_clsid][l];
    tail = &tails[it->slabs_clsid][l];
    assert(it != *head);
    assert((*head && *tail) || (*head == 0 && *tail == 0));
    it->prev = 0;
    it->next = *head;
    if (it->next) it->next->prev = it;
    *head = it;
    if (*tail == 0) *tail = it;
    sizes[it->slabs_clsid][l]++;
    return;
}

/* Always called witch lock held */
static void item_unlink_q(item *it, int l) {
    item **head, **tail;
    /* always true, warns: assert(it->slabs_clsid <= LARGEST_ID); */
    head = &heads[it->slabs_clsid][l];
    tail = &tails[it->slabs_clsid][l];

    if (*head == it) {
        assert(it->prev == 0);
        *head = it->next;
    }
    if (*tail == it) {
        assert(it->next == 0);
        *tail = it->prev;
    }
    assert(it->next != it);
    assert(it->prev != it);

    if (it->next) it->next->prev = it->prev;
    if (it->prev) it->prev->next = it->next;
    it->next = 0;
    it->prev = 0;
    sizes[it->slabs_clsid][l]--;
    return;
}

int item_link(item *it) {
    int l = hash(ITEM_key(it), it->nkey, 0) & LRU_MASK;
    pthread_mutex_lock(&lru_locks[it->slabs_clsid][l]);
    MEMCACHED_ITEM_LINK(ITEM_key(it), it->nkey, it->nbytes);
    assert((it->it_flags & (ITEM_LINKED|ITEM_SLABBED)) == 0);
    assert(it->nbytes < (1024 * 1024));  /* 1MB max size */
    it->it_flags |= ITEM_LINKED;
    it->time = current_time;
    //assoc_insert(it);

    atomic_inc(&it->refcount);

    (void)atomic_add(&stats.curr_bytes, ITEM_ntotal(it));
    atomic_inc(&stats.curr_items);
    atomic_inc(&stats.total_items);

    /* Allocate a new CAS ID on link. */
    ITEM_set_cas(it, (settings.use_cas) ? get_cas_id() : 0);

    item_link_q(it, l);
    pthread_mutex_unlock(&lru_locks[it->slabs_clsid][l]);

    assoc_insert(it);

    return 1;
}

static int do_item_unlink(item *it, int l) {
    int ret;
    ret = atomic_sub(&it->refcount, 1);
    MEMCACHED_ITEM_UNLINK(ITEM_key(it), it->nkey, it->nbytes);
    if ((it->it_flags & ITEM_LINKED) != 0) {
        it->it_flags &= ~ITEM_LINKED;
        (void)atomic_sub(&stats.curr_bytes, ITEM_ntotal(it));
        atomic_dec(&stats.curr_items);
        assoc_delete(it);
        item_unlink_q(it, l);
    }


    return ret == 0;
}

void item_unlink(item *it) {
    int id = it->slabs_clsid;
    int l = hash(ITEM_key(it), it->nkey, 0) & LRU_MASK;
    pthread_mutex_lock(&lru_locks[id][l]);
    do_item_unlink(it, l);
    pthread_mutex_unlock(&lru_locks[id][l]);
    item_remove(it);
}

void item_remove(item *it) {
    MEMCACHED_ITEM_REMOVE(ITEM_key(it), it->nkey, it->nbytes);
    assert((it->it_flags & ITEM_SLABBED) == 0);
    DEBUG_REFCNT(it, '-');
    if (atomic_dec_and_test(&it->refcount)) {
        item_free(it);
        return;
    }
}

void item_update(item *it) {
    MEMCACHED_ITEM_UPDATE(ITEM_key(it), it->nkey, it->nbytes);
    if (it->time < current_time - ITEM_UPDATE_INTERVAL) {
        int l = hash(ITEM_key(it), it->nkey, 0) & LRU_MASK;
        pthread_mutex_lock(&lru_locks[it->slabs_clsid][l]);
        assert((it->it_flags & ITEM_SLABBED) == 0);

        if ((it->it_flags & ITEM_LINKED) != 0) {
            item_unlink_q(it, l);
            it->time = current_time;
            item_link_q(it, l);
        }
        pthread_mutex_unlock(&lru_locks[it->slabs_clsid][l]);
    }
}

int item_replace(item *old_it, item *new_it, int force) {
    int replaced = 0;
    int l = hash(ITEM_key(old_it), old_it->nkey, 0) & LRU_MASK;
    MEMCACHED_ITEM_REPLACE(ITEM_key(old_it), old_it->nkey, old_it->nbytes,
                           ITEM_key(new_it), new_it->nkey, new_it->nbytes);

    int bytes_delta = ITEM_ntotal(new_it) - ITEM_ntotal(old_it);

    assert((old_it->it_flags & ITEM_LINKED) != 0);
    assert(old_it->nkey == new_it->nkey);
    assert(!memcmp(ITEM_key(old_it), ITEM_key(new_it), old_it->nkey));

    new_it->it_flags |= ITEM_LINKED;
    new_it->time = current_time;

    /* Allocate a new CAS ID. */
    ITEM_set_cas(new_it, (settings.use_cas) ? get_cas_id() : 0);

    atomic_inc(&new_it->refcount);

    pthread_mutex_lock(&lru_locks[new_it->slabs_clsid][l]);
    item_link_q(new_it, l);
    pthread_mutex_unlock(&lru_locks[new_it->slabs_clsid][l]);

    replaced = assoc_update(old_it, new_it, force);

    pthread_mutex_lock(&lru_locks[old_it->slabs_clsid][l]);
    old_it->it_flags &= ~ITEM_LINKED;
    item_unlink_q(old_it, l);
    pthread_mutex_unlock(&lru_locks[old_it->slabs_clsid][l]);

    item_remove(old_it);

    if (!replaced)
        bytes_delta = ITEM_ntotal(new_it);
    (void)atomic_add(&stats.curr_bytes, bytes_delta);


    return replaced;
}

/*@null@*/
char *item_cachedump(const unsigned int slabs_clsid, const unsigned int limit, unsigned int *bytes) {
    unsigned int memlimit = 2 * 1024 * 1024;   /* 2MB max response size */
    char *buffer;
    unsigned int bufcurr;
    item *it;
    unsigned int len;
    unsigned int shown = 0;
    char key_temp[KEY_MAX_LENGTH + 1];
    char temp[512];
    int l;

    if (slabs_clsid > LARGEST_ID) return NULL;

    buffer = malloc((size_t)memlimit);
    if (buffer == 0) return NULL;
    bufcurr = 0;

    for (l=0; l<LRU_COUNT; l++) {
        pthread_mutex_lock(&lru_locks[slabs_clsid][l]);
        it = heads[slabs_clsid][l];
        while (it != NULL && (limit == 0 || shown < limit)) {
            assert(it->nkey <= KEY_MAX_LENGTH);
            /* Copy the key since it may not be null-terminated in the struct */
            strncpy(key_temp, ITEM_key(it), it->nkey);
            key_temp[it->nkey] = 0x00; /* terminate */
            len = snprintf(temp, sizeof(temp), "ITEM %s [%d b; %lu s]\r\n",
                           key_temp, it->nbytes - 2,
                           (unsigned long)it->exptime + process_started);
            if (bufcurr + len + 6 > memlimit)  /* 6 is END\r\n\0 */
                break;
            memcpy(buffer + bufcurr, temp, len);
            bufcurr += len;
            shown++;
            it = it->next;
        }
    }

    memcpy(buffer + bufcurr, "END\r\n", 6);
    bufcurr += 5;

    *bytes = bufcurr;
    return buffer;
}

void item_stats(ADD_STAT add_stats, void *c) {
    int i;
    int l;
    for (i = 0; i < LARGEST_ID; i++) {
        for (l=0; l < LRU_COUNT; l++) {
            pthread_mutex_lock(&lru_locks[i][l]);
            if (tails[i][l] != NULL) {
                const char *fmt = "items:%d:%s";
                char key_str[STAT_KEY_LEN];
                char val_str[STAT_VAL_LEN];
                int klen = 0, vlen = 0;

                APPEND_NUM_FMT_STAT(fmt, i, "number", "%u", sizes[i][l]);
                APPEND_NUM_FMT_STAT(fmt, i, "age", "%u", tails[i][l]->time);
                APPEND_NUM_FMT_STAT(fmt, i, "evicted",
                                    "%u", itemstats[i][l].evicted);
                APPEND_NUM_FMT_STAT(fmt, i, "evicted_time",
                                    "%u", itemstats[i][l].evicted_time);
                APPEND_NUM_FMT_STAT(fmt, i, "outofmemory",
                                    "%u", itemstats[i][l].outofmemory);
                APPEND_NUM_FMT_STAT(fmt, i, "tailrepairs",
                                    "%u", itemstats[i][l].tailrepairs);;
            }
            pthread_mutex_unlock(&lru_locks[i][l]);
        }
    }

    /* getting here means both ascii and binary terminators fit */
    add_stats(NULL, 0, NULL, 0, c);
}

/** dumps out a list of objects of each size, with granularity of 32 bytes */
/*@null@*/
void item_stats_sizes(ADD_STAT add_stats, void *c) {

    /* max 1MB object, divided into 32 bytes size buckets */
    const int num_buckets = 32768;
    unsigned int *histogram = calloc(num_buckets, sizeof(int));

    if (histogram != NULL) {
        int i, l;

        /* build the histogram */
        for (i = 0; i < LARGEST_ID; i++) {
            for (l = 0; l < LRU_COUNT; l++) {
                pthread_mutex_lock(&lru_locks[i][l]);
                item *iter = heads[i][l];
                while (iter) {
                    int ntotal = ITEM_ntotal(iter);
                    int bucket = ntotal / 32;
                    if ((ntotal % 32) != 0) bucket++;
                    if (bucket < num_buckets) histogram[bucket]++;
                    iter = iter->next;
                }
                pthread_mutex_unlock(&lru_locks[i][l]);
            }
        }

        /* write the buffer */
        for (i = 0; i < num_buckets; i++) {
            if (histogram[i] != 0) {
                char key[8];
                int klen = 0;
                klen = snprintf(key, sizeof(key), "%d", i * 32);
                assert(klen < sizeof(key));
                APPEND_STAT(key, "%u", histogram[i]);
            }
        }
        free(histogram);
    }
    add_stats(NULL, 0, NULL, 0, c);
}

int item_ref(item* it) {
    int ret;
    DEBUG_REFCNT(it, '+');
    ret = atomic_inc_not_zero(&it->refcount);
    return ret;
}


/** wrapper around assoc_find which does the lazy expiration logic */
item *item_get(const char *key, const size_t nkey) {
    // this increases refcount
    item *it = assoc_find(key, nkey);
    int was_found = 0;

    if (settings.verbose > 2) {
        if (it == NULL) {
            fprintf(stderr, "> NOT FOUND %s", key);
        } else {
            fprintf(stderr, "> FOUND KEY %s", ITEM_key(it));
            was_found++;
        }
    }

    if (it != NULL && settings.oldest_live != 0 && settings.oldest_live <= current_time &&
        it->time <= settings.oldest_live) {
        item_unlink(it);
        item_remove(it);           /* return reference */
        it = NULL;
    }

    if (it == NULL && was_found) {
        fprintf(stderr, " -nuked by flush");
        was_found--;
    }

    if (it != NULL && it->exptime != 0 && it->exptime <= current_time) {
        item_unlink(it);
        item_remove(it);           /* return reference */
        it = NULL;
    }

    if (it == NULL && was_found) {
        fprintf(stderr, " -nuked by expire");
        was_found--;
    }

    if (settings.verbose > 2)
        fprintf(stderr, "\n");

    return it;
}

/** returns an item whether or not it's expired. */
item *item_get_nocheck(const char *key, const size_t nkey) {
    item *it = assoc_find(key, nkey);
    return it;
}

/* expires items that are more recent than the oldest_live setting. */
void item_flush_expired(void) {
    int i, l;
    item *iter, *next;
    if (settings.oldest_live == 0)
        return;
    for (i = 0; i < LARGEST_ID; i++) {
        for (l = 0; l < LRU_COUNT; l++) {
            /* The LRU is sorted in decreasing time order, and an item's timestamp
             * is never newer than its last access time, so we only need to walk
             * back until we hit an item older than the oldest_live time.
             * The oldest_live checking will auto-expire the remaining items.
             */
            pthread_mutex_lock(&lru_locks[i][l]);
            for (iter = heads[i][l]; iter != NULL; iter = next) {
                if (iter->time >= settings.oldest_live) {
                    next = iter->next;
                    if ((iter->it_flags & ITEM_SLABBED) == 0) {
                        if (do_item_unlink(iter, l)) {
                            item_free(iter);
                        }
                    }
                } else {
                    /* We've hit the first old item. Continue to the next queue. */
                    break;
                }
            }
            pthread_mutex_unlock(&lru_locks[i][l]);
        }
    }
}
