/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Hash table
 *
 * The hash function used here is by Bob Jenkins, 1996:
 *    <http://burtleburtle.net/bob/hash/doobs.html>
 *       "By Bob Jenkins, 1996.  bob_jenkins@burtleburtle.net.
 *       You may use this code any way you wish, private, educational,
 *       or commercial.  It's free."
 *
 * The rest of the file is licensed under the BSD license.  See LICENSE.
 */

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
#include <assert.h>
#include <pthread.h>

#define REGIONS_POWER 8
#define REGIONS_COUNT ((ub4)1<<(REGIONS_POWER))
#define REGIONS_MASK (REGIONS_COUNT - 1)

static pthread_cond_t maintenance_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t maintenance_lock;


typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;   /* unsigned 1-byte quantities */

/* how many powers of 2's worth of buckets we use */
static unsigned int hashpower[REGIONS_COUNT];

#define hashsize(n) ((ub4)1<<(n))
#define hashmask(n) ((hashsize(n) << REGIONS_POWER) -1)
#define hash_region(hv) ((hv) & REGIONS_MASK)
#define hash_bucket(hv, pwr) (((hv) & hashmask(pwr)) >> REGIONS_POWER)

static pthread_rwlock_t assoc_locks[REGIONS_COUNT];

/* Main hash table. This is where we look except during expansion. */
static item** primary_hashtable[REGIONS_COUNT];

/*
 * Previous hash table. During expansion, we look here for keys that haven't
 * been moved over to the primary yet.
 */
static item** old_hashtable[REGIONS_COUNT];

/* Number of items in the hash table. */
static unsigned int hash_items[REGIONS_COUNT];

/* Flag: Are we in the middle of expanding now? */
static bool expanding[REGIONS_COUNT];

/*
 * During expansion we migrate values with bucket granularity; this is how
 * far we've gotten so far. Ranges from 0 .. hashsize(hashpower - 1) - 1.
 */
static unsigned int expand_bucket[REGIONS_COUNT];

void assoc_init(void) {
    int r;
    for(r=0; r<REGIONS_COUNT; r++) {
        hashpower[r] = 8;
        hashpower[r] = 16;
        primary_hashtable[r] = calloc(hashsize(hashpower[r]), sizeof(void *));
        if (! primary_hashtable[r]) {
            fprintf(stderr, "Failed to init hashtable.\n");
            exit(EXIT_FAILURE);
        }
        old_hashtable[r] = 0;
        expanding[r] = 0;
        expand_bucket[r] = 0;
        hash_items[r] = 0;
        pthread_rwlock_init(&assoc_locks[r], NULL);
    }
    pthread_mutex_init(&maintenance_lock, NULL);
}

item *assoc_find(const char *key, const size_t nkey) {
    uint32_t hv = hash(key, nkey, 0);
    int r = hash_region(hv);
    item *it;
    unsigned int oldbucket;

    pthread_rwlock_rdlock(&assoc_locks[r]);
    if (expanding[r] &&
        (oldbucket = (hash_bucket(hv, hashpower[r]-1))) >= expand_bucket[r])
    {
        it = old_hashtable[r][oldbucket];
    } else {
        it = primary_hashtable[r][hash_bucket(hv, hashpower[r])];
    }

    item *ret = NULL;
    int depth = 0;
    while (it) {
        if ((nkey == it->nkey) && (memcmp(key, ITEM_key(it), nkey) == 0)) {
            ret = it;
            break;
        }
        it = it->h_next;
        ++depth;
    }
    if (ret && !item_ref(ret)) {
        ret = NULL;
    }
    pthread_rwlock_unlock(&assoc_locks[r]);
    MEMCACHED_ASSOC_FIND(key, nkey, depth);
    return ret;
}

/* returns the address of the item pointer before the key.  if *item == 0,
   the item wasn't found */

static item** do_hashitem_before (const char *key, const size_t nkey, uint32_t hv) {
    int r = hash_region(hv);
    item **pos;
    unsigned int oldbucket;

    if (expanding[r] &&
        (oldbucket = (hash_bucket(hv, hashpower[r]-1))) >= expand_bucket[r])
    {
        pos = &old_hashtable[r][oldbucket];
    } else {
        pos = &primary_hashtable[r][hash_bucket(hv, hashpower[r])];
    }

    while (*pos && ((nkey != (*pos)->nkey) || memcmp(key, ITEM_key(*pos), nkey))) {
        pos = &(*pos)->h_next;
    }
    return pos;
}

/* grows the hashtable to the next power of 2. */
static int assoc_expand(int r) {
    old_hashtable[r] = primary_hashtable[r];

    primary_hashtable[r] = calloc(hashsize(hashpower[r] + 1), sizeof(void *));
    if (primary_hashtable[r]) {
        if (settings.verbose > 1)
            fprintf(stderr, "Hash table expansion starting\n");
        hashpower[r]++;
        expanding[r] = true;
        expand_bucket[r] = 0;
        return 1;
    } else {
        primary_hashtable[r] = old_hashtable[r];
        /* Bad news, but we can keep running. */
    }
    return 0;
}

static int do_assoc_insert(item *it, uint32_t hv) {
    int r = hash_region(hv);
    unsigned int oldbucket;
    int expanded = 0;

    hv = hash(ITEM_key(it), it->nkey, 0);
    if (expanding[r] &&
        (oldbucket = (hash_bucket(hv, hashpower[r]-1))) >= expand_bucket[r])
    {
        it->h_next = old_hashtable[r][oldbucket];
        old_hashtable[r][oldbucket] = it;
    } else {
        it->h_next = primary_hashtable[r][hash_bucket(hv, hashpower[r])];
        primary_hashtable[r][hash_bucket(hv, hashpower[r])] = it;
    }

    hash_items[r]++;
    if (! expanding[r] && hash_items[r] > (hashsize(hashpower[r]) * 2) / 3) {
        expanded = assoc_expand(r);
    }

    MEMCACHED_ASSOC_INSERT(ITEM_key(it), it->nkey, hash_items[r]);
    return expanded;
}

/* Note: this isn't an assoc_update.  The key must not already exist to call this */
int assoc_insert(item *it) {
    uint32_t hv = hash(ITEM_key(it), it->nkey, 0);
    int r = hash_region(hv);
    int expanded = 0;

    // this would leak a reference
    //assert(assoc_find(ITEM_key(it), it->nkey) == 0);  /* shouldn't have duplicately named things defined */

    pthread_rwlock_wrlock(&assoc_locks[r]);
    expanded = do_assoc_insert(it, hv);
    pthread_rwlock_unlock(&assoc_locks[r]);

    if (expanded) {
        pthread_mutex_lock(&maintenance_lock);
        pthread_cond_signal(&maintenance_cond);
        pthread_mutex_unlock(&maintenance_lock);
    }

    return 1;
}

void assoc_delete(item* it) {
    uint32_t hv = hash(ITEM_key(it), it->nkey, 0);
    int r = hash_region(hv);
    item **pos;
    unsigned int oldbucket;

    pthread_rwlock_wrlock(&assoc_locks[r]);

    if (expanding[r] &&
        (oldbucket = (hash_bucket(hv, hashpower[r]-1))) >= expand_bucket[r])
    {
        pos = &old_hashtable[r][oldbucket];
    } else {
        pos = &primary_hashtable[r][hash_bucket(hv, hashpower[r])];
    }

    while (*pos && *pos != it) {
        pos = &(*pos)->h_next;
    }

    if (*pos == it) {
        item *nxt = it->h_next;
        hash_items[r]--;
        MEMCACHED_ASSOC_DELETE(ITEM_key(it), it->nkey, hash_items);
        *pos = nxt;
        it->h_next = 0;   /* probably pointless, but whatever. */
    }
    pthread_rwlock_unlock(&assoc_locks[r]);
}

int assoc_update(item *old_it, item *new_it, int force) {
    uint32_t hv = hash(ITEM_key(new_it), new_it->nkey, 0);
    int r = hash_region(hv);
    int expanded = 0;
    int replaced = 0;
    item **before;

    pthread_rwlock_wrlock(&assoc_locks[r]);

    before = do_hashitem_before(ITEM_key(old_it), old_it->nkey, hv);
    if (*before && (force || *before == old_it)) {
        new_it->h_next = (*before)->h_next;
        replaced = 1;
        (*before)->h_next = 0;   /* probably pointless, but whatever. */
        *before = new_it;
    } else if (force) {
        expanded = do_assoc_insert(new_it, hv);
    }

    pthread_rwlock_unlock(&assoc_locks[r]);

    if (expanded) {
        pthread_mutex_lock(&maintenance_lock);
        pthread_cond_signal(&maintenance_cond);
        pthread_mutex_unlock(&maintenance_lock);
    }

    return replaced;
}

static volatile int do_run_maintenance_thread = 1;

#define DEFAULT_HASH_BULK_MOVE 1
int hash_bulk_move = DEFAULT_HASH_BULK_MOVE;

static void *assoc_maintenance_thread(void *arg) {
    int r = 0;

    pthread_mutex_lock(&maintenance_lock);
    while (do_run_maintenance_thread) {
        int i;
        for (i = 0; i < REGIONS_COUNT; i++) {
            pthread_rwlock_rdlock(&assoc_locks[r]);
            if (expanding[r]) {
                pthread_rwlock_unlock(&assoc_locks[r]);
                break;
            }
            pthread_rwlock_unlock(&assoc_locks[r]);
            if (++r >= REGIONS_COUNT)
            r = 0;
        }
        if (i >= REGIONS_COUNT) {
            /* We are done expanding.. just wait for next invocation */
            pthread_cond_wait(&maintenance_cond, &maintenance_lock);
            continue;
        }
        pthread_mutex_unlock(&maintenance_lock);

        int ii = 0;

        /* Lock the cache, and bulk move multiple buckets to the new
         * hash table. */
        pthread_rwlock_wrlock(&assoc_locks[r]);

        for (ii = 0; ii < hash_bulk_move && expanding[r]; ++ii) {
            item *it, *next;
            int bucket;

            for (it = old_hashtable[r][expand_bucket[r]]; NULL != it; it = next) {
                next = it->h_next;

                bucket = hash_bucket(hash(ITEM_key(it), it->nkey, 0), hashpower[r]);
                it->h_next = primary_hashtable[r][bucket];
                primary_hashtable[r][bucket] = it;
            }

            old_hashtable[r][expand_bucket[r]] = NULL;

            expand_bucket[r]++;
            if (expand_bucket[r] == hashsize(hashpower[r] - 1)) {
                expanding[r] = false;
                free(old_hashtable[r]);
                if (settings.verbose > 1)
                    fprintf(stderr, "Hash table expansion done\n");
            }

        }

        pthread_rwlock_unlock(&assoc_locks[r]);

        pthread_mutex_lock(&maintenance_lock);
    }
    return NULL;
}

static pthread_t maintenance_tid;

int start_assoc_maintenance_thread() {
    int ret;
    char *env = getenv("MEMCACHED_HASH_BULK_MOVE");
    if (env != NULL) {
        hash_bulk_move = atoi(env);
        if (hash_bulk_move == 0) {
            hash_bulk_move = DEFAULT_HASH_BULK_MOVE;
        }
    }
    if ((ret = pthread_create(&maintenance_tid, NULL,
                              assoc_maintenance_thread, NULL)) != 0) {
        fprintf(stderr, "Can't create thread: %s\n", strerror(ret));
        return -1;
    }
    return 0;
}

void stop_assoc_maintenance_thread() {
    pthread_mutex_lock(&maintenance_lock);
    do_run_maintenance_thread = 0;
    pthread_cond_signal(&maintenance_cond);
    pthread_mutex_unlock(&maintenance_lock);

    /* Wait for the maintenance thread to stop */
    pthread_join(maintenance_tid, NULL);
}


