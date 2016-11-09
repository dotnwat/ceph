import itertools

check_epoch = ["READ_EPOCH_OMAP", "READ_EPOCH_HEADER"]
read_pos_md = ["READ_OMAP_INDEX_ENTRY"]
write_pos_md = ["WRITE_OMAP_INDEX_ENTRY"]
write_entry = ["APPEND_DATA"]
sections = [check_epoch, read_pos_md, write_pos_md, write_entry]

def remove_invalid(ops):
    seen_read_pos_md = False
    for op in ops:
        if op == "read_pos_md":
            seen_read_pos_md = True
        if op == "write_pos_md":
            return not seen_read_pos_md

count = 0
for combo in itertools.product(*sections):
    for perm in itertools.permutations(combo):
        if not remove_invalid(perm):
            print "{" + `count` + ', ' + ', '.join(list(perm)) + "},"
    count += 1
