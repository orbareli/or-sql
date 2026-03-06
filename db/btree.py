"""
B+ Tree — the index layer.
Maps user_id (key) → (page_id, slot_id) — the location of the record on disk.

Tree structure:
  - Internal nodes: hold keys + child page_ids (navigation only)
  - Leaf nodes:     hold keys + (page_id, slot_id) values (actual index data)
  - Leaf nodes are linked together for range scans

Every node is stored as exactly one page on disk.
The first page of the index file is always the root.

Node page layout:

  INTERNAL:
    [type=0: 1b | num_keys: 2b | key0: 4b | key1 | ... | child0: 4b | child1 | ...]

  LEAF:
    [type=1: 1b | num_keys: 2b | next_leaf: 4b | key0: 4b | ... | val0: 8b | val1 | ...]
    where val = (page_id: 4b, slot_id: 4b)
"""
import struct
from pager import Pager, PAGE_SIZE

# Node types
INTERNAL = 0
LEAF     = 1

# Sizes
KEY_SIZE = 4   # unsigned int
PTR_SIZE = 4   # page_id
VAL_SIZE = 8   # (page_id: 4, slot_id: 4)

# Use small ORDER during development so splits happen quickly and are easy to test.
# In production, set ORDER = (PAGE_SIZE - 7) // 12 = 340
ORDER = 4      # max keys per node

# Header formats
INT_HDR_FMT  = "!BH"    # type(1) + num_keys(2) = 3 bytes
INT_HDR_SIZE = struct.calcsize(INT_HDR_FMT)

LEAF_HDR_FMT  = "!BHI"  # type(1) + num_keys(2) + next_leaf(4) = 7 bytes
LEAF_HDR_SIZE = struct.calcsize(LEAF_HDR_FMT)


# ------------------------------------------------------------------ #
#  Node classes                                                        #
# ------------------------------------------------------------------ #

class InternalNode:
    def __init__(self, page_id: int, data: bytearray = None):
        self.page_id  = page_id
        self.is_dirty = False
        if data:
            _, self.num_keys = struct.unpack(INT_HDR_FMT, data[:INT_HDR_SIZE])
            self.data = bytearray(data)
        else:
            self.num_keys = 0
            self.data     = bytearray(PAGE_SIZE)
            self.data[0]  = INTERNAL
            self._flush_header()

    def _flush_header(self):
        struct.pack_into(INT_HDR_FMT, self.data, 0, INTERNAL, self.num_keys)

    def _key_offset(self, i):
        return INT_HDR_SIZE + i * KEY_SIZE

    def _child_offset(self, i):
        # Children stored after all key slots
        return INT_HDR_SIZE + ORDER * KEY_SIZE + i * PTR_SIZE

    def get_key(self, i):
        o = self._key_offset(i)
        return struct.unpack("!I", self.data[o:o+KEY_SIZE])[0]

    def set_key(self, i, key):
        struct.pack_into("!I", self.data, self._key_offset(i), key)
        self.is_dirty = True

    def get_child(self, i):
        o = self._child_offset(i)
        return struct.unpack("!I", self.data[o:o+PTR_SIZE])[0]

    def set_child(self, i, page_id):
        struct.pack_into("!I", self.data, self._child_offset(i), page_id)
        self.is_dirty = True

    def find_child_index(self, key):
        """Which child pointer to follow for this key."""
        i = 0
        while i < self.num_keys and key >= self.get_key(i):
            i += 1
        return i


class LeafNode:
    def __init__(self, page_id: int, data: bytearray = None):
        self.page_id   = page_id
        self.is_dirty  = False
        self.next_leaf = 0
        if data:
            _, self.num_keys, self.next_leaf = struct.unpack(
                LEAF_HDR_FMT, data[:LEAF_HDR_SIZE]
            )
            self.data = bytearray(data)
        else:
            self.num_keys = 0
            self.data     = bytearray(PAGE_SIZE)
            self.data[0]  = LEAF
            self._flush_header()

    def _flush_header(self):
        struct.pack_into(LEAF_HDR_FMT, self.data, 0, LEAF, self.num_keys, self.next_leaf)

    def _key_offset(self, i):
        return LEAF_HDR_SIZE + i * KEY_SIZE

    def _val_offset(self, i):
        # Values stored after all key slots
        return LEAF_HDR_SIZE + ORDER * KEY_SIZE + i * VAL_SIZE

    def get_key(self, i):
        o = self._key_offset(i)
        return struct.unpack("!I", self.data[o:o+KEY_SIZE])[0]

    def set_key(self, i, key):
        struct.pack_into("!I", self.data, self._key_offset(i), key)
        self.is_dirty = True

    def get_val(self, i):
        o = self._val_offset(i)
        return struct.unpack("!II", self.data[o:o+VAL_SIZE])  # (page_id, slot_id)

    def set_val(self, i, page_id, slot_id):
        struct.pack_into("!II", self.data, self._val_offset(i), page_id, slot_id)
        self.is_dirty = True

    def search(self, key):
        """Return (page_id, slot_id) or None."""
        for i in range(self.num_keys):
            if self.get_key(i) == key:
                return self.get_val(i)
        return None

    def find_insert_pos(self, key):
        """Index where this key should be inserted to keep sorted order."""
        i = 0
        while i < self.num_keys and self.get_key(i) < key:
            i += 1
        return i


# ------------------------------------------------------------------ #
#  Load helper                                                         #
# ------------------------------------------------------------------ #

def load_node(pager: Pager, page_id: int):
    """Read page from disk, return correct node type."""
    data = pager.get_page(page_id)
    if data[0] == INTERNAL:
        return InternalNode(page_id, data)
    else:
        return LeafNode(page_id, data)


def save_node(pager: Pager, node):
    """Write node back to disk."""
    node._flush_header()
    pager.write_page(node.page_id, node.data)
    node.is_dirty = False


# ------------------------------------------------------------------ #
#  B+ Tree                                                             #
# ------------------------------------------------------------------ #

class BPlusTree:
    """
    Stores the index in a separate file (e.g. my_database.db.idx).
    Root is always page_id 0.
    """

    def __init__(self, index_path: str):
        self.pager = Pager(index_path)
        if self.pager.num_pages == 0:
            # Brand new index — create empty root leaf
            root = LeafNode(self.pager.allocate_page())
            save_node(self.pager, root)

    # ---------------------------------------------------------------- #
    #  Search                                                           #
    # ---------------------------------------------------------------- #

    def search(self, key: int):
        """Return (page_id, slot_id) for key, or None if not found."""
        leaf = self._find_leaf(key)
        return leaf.search(key)

    def _find_leaf(self, key: int) -> LeafNode:
        """Traverse from root down to the correct leaf node."""
        node = load_node(self.pager, 0)  # start at root (always page 0)
        while isinstance(node, InternalNode):
            child_index = node.find_child_index(key)
            child_page  = node.get_child(child_index)
            node        = load_node(self.pager, child_page)
        return node

    # ---------------------------------------------------------------- #
    #  Insert                                                           #
    # ---------------------------------------------------------------- #

    def insert(self, key: int, page_id: int, slot_id: int):
        """Insert key → (page_id, slot_id) into the index."""
        result = self._insert_recursive(0, key, page_id, slot_id)
        if result:
            # Root was split — create a new root
            split_key, new_page_id = result
            old_root_data = self.pager.get_page(0)
            old_root_copy_id = self.pager.allocate_page()
            self.pager.write_page(old_root_copy_id, old_root_data)

            new_root = InternalNode(0)
            new_root.set_key(0, split_key)
            new_root.set_child(0, old_root_copy_id)
            new_root.set_child(1, new_page_id)
            new_root.num_keys = 1
            save_node(self.pager, new_root)

    def _insert_recursive(self, page_id: int, key: int, rec_page: int, rec_slot: int):
        """
        Recursively insert. Returns (split_key, new_page_id) if node split, else None.
        """
        node = load_node(self.pager, page_id)

        if isinstance(node, LeafNode):
            return self._insert_into_leaf(node, key, rec_page, rec_slot)
        else:
            child_index = node.find_child_index(key)
            child_page  = node.get_child(child_index)
            result      = self._insert_recursive(child_page, key, rec_page, rec_slot)

            if result is None:
                return None  # no split happened below, we're done

            # Child split — insert the promoted key into this internal node
            split_key, new_child_page = result
            return self._insert_into_internal(node, child_index, split_key, new_child_page)

    def _insert_into_leaf(self, leaf: LeafNode, key: int, rec_page: int, rec_slot: int):
        pos = leaf.find_insert_pos(key)

        if leaf.num_keys < ORDER:
            # Room available — shift right and insert
            for i in range(leaf.num_keys, pos, -1):
                leaf.set_key(i, leaf.get_key(i - 1))
                v = leaf.get_val(i - 1)
                leaf.set_val(i, v[0], v[1])
            leaf.set_key(pos, key)
            leaf.set_val(pos, rec_page, rec_slot)
            leaf.num_keys += 1
            save_node(self.pager, leaf)
            return None  # no split

        # Leaf is full — split it
        # Collect all entries + new one, sorted
        all_keys = [leaf.get_key(i) for i in range(leaf.num_keys)]
        all_vals = [leaf.get_val(i) for i in range(leaf.num_keys)]
        all_keys.insert(pos, key)
        all_vals.insert(pos, (rec_page, rec_slot))

        mid = len(all_keys) // 2

        # Left half stays in current leaf
        leaf.num_keys = mid
        for i in range(mid):
            leaf.set_key(i, all_keys[i])
            leaf.set_val(i, all_vals[i][0], all_vals[i][1])

        # Right half goes into new leaf
        new_leaf    = LeafNode(self.pager.allocate_page())
        new_leaf.num_keys = len(all_keys) - mid
        for i, idx in enumerate(range(mid, len(all_keys))):
            new_leaf.set_key(i, all_keys[idx])
            new_leaf.set_val(i, all_vals[idx][0], all_vals[idx][1])

        # Link leaves
        new_leaf.next_leaf = leaf.next_leaf
        leaf.next_leaf     = new_leaf.page_id

        save_node(self.pager, leaf)
        save_node(self.pager, new_leaf)

        # Promote first key of right leaf to parent
        return (all_keys[mid], new_leaf.page_id)

    def _insert_into_internal(self, node: InternalNode, child_index: int, split_key: int, new_child: int):
        if node.num_keys < ORDER:
            # Room available — shift right and insert
            for i in range(node.num_keys, child_index, -1):
                node.set_key(i, node.get_key(i - 1))
                node.set_child(i + 1, node.get_child(i))
            node.set_key(child_index, split_key)
            node.set_child(child_index + 1, new_child)
            node.num_keys += 1
            save_node(self.pager, node)
            return None  # no split

        # Internal node full — split it
        all_keys     = [node.get_key(i) for i in range(node.num_keys)]
        all_children = [node.get_child(i) for i in range(node.num_keys + 1)]
        all_keys.insert(child_index, split_key)
        all_children.insert(child_index + 1, new_child)

        mid          = len(all_keys) // 2
        promoted_key = all_keys[mid]

        # Left half stays
        node.num_keys = mid
        for i in range(mid):
            node.set_key(i, all_keys[i])
            node.set_child(i, all_children[i])
        node.set_child(mid, all_children[mid])

        # Right half goes to new node
        new_node          = InternalNode(self.pager.allocate_page())
        right_keys        = all_keys[mid+1:]
        right_children    = all_children[mid+1:]
        new_node.num_keys = len(right_keys)
        for i, k in enumerate(right_keys):
            new_node.set_key(i, k)
        for i, c in enumerate(right_children):
            new_node.set_child(i, c)

        save_node(self.pager, node)
        save_node(self.pager, new_node)

        return (promoted_key, new_node.page_id)
    def delete(self, key: int) -> bool:
        """
        Remove a key from the B+ Tree index.
        
        For now: simple deletion from the leaf only.
        We don't rebalance (merge underfull nodes) — this is called
        "lazy deletion" and is acceptable for most use cases.
        Real databases do this too when the tree is large enough.
        """
        leaf = self._find_leaf(key)

        # Find the key in the leaf
        idx = None
        for i in range(leaf.num_keys):
            if leaf.get_key(i) == key:
                idx = i
                break

        if idx is None:
            return False  # key not in tree

        # Shift everything left to fill the gap
        # Before: [1, 3, 5, 7]  delete 3
        # After:  [1, 5, 7]
        for i in range(idx, leaf.num_keys - 1):
            leaf.set_key(i, leaf.get_key(i + 1))
            v = leaf.get_val(i + 1)
            leaf.set_val(i, v[0], v[1])

        leaf.num_keys -= 1
        save_node(self.pager, leaf)
        return True