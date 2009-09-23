// Emulate somehow 64 bit atomic operations on 32 bit machines
// TODO this assumes little endian
#if defined(HAVE_SYNC_BUILTINS) && !defined(HAVE_SYNC_BUILTINS_64)
uint64_t __sync_add_and_fetch_8(uint64_t* v, uint64_t x) {
	uint32_t* low = (uint32_t*)v;
	uint32_t low_old, low_new;
	low_new = atomic_add(low, (uint32_t)x);
	low_old = low_new - x;
	if (low_new < low_old) {
		uint32_t* high = ((uint32_t*)v) + 1;
		atomic_inc(high);
	}
	return *v;
}
uint64_t __sync_sub_and_fetch_8(uint64_t* v, uint64_t x) {
	uint32_t* low = (uint32_t*)v;
	uint32_t low_old, low_new;
	low_new = atomic_sub(low, (uint32_t)x);
	low_old = low_new + x;
	if (low_new > low_old) {
		uint32_t* high = ((uint32_t*)v) + 1;
		atomic_dec(high);
	}
	return *v;
}
#endif
