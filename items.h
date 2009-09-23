/* See items.c */
void item_init(void);
uint64_t get_cas_id(void);

void item_free(item *it);
bool item_size_ok(const size_t nkey, const int flags, const int nbytes);

int item_ref(item* it);

item *item_get_nocheck(const char *key, const size_t nkey);
void item_stats_reset(void);
