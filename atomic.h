#define atomic_read(v) (*(v))
#define atomic_add(x, y) __sync_add_and_fetch(x, y)
#define atomic_sub(x, y) __sync_sub_and_fetch(x, y)
#define atomic_cmpxchg(v, o, n) __sync_val_compare_and_swap((v), (o), (n))
static inline unsigned short atomic_inc_not_zero(unsigned short* v) {
	int c, old;
	c = atomic_read(v);
	while (c && (old = atomic_cmpxchg((v), c, c + 1)) != c)
		c = old;
	return c;
}

#define atomic_inc(x) (void)atomic_add((x), 1)
#define atomic_dec(x) (void)atomic_sub((x), 1)


#define atomic_dec_and_test(v) (atomic_sub(v,1) == 0)
