# ADR 0010. The canonical snapshot format

Status: accepted, Phase 3.

## Context

A version node's ID covers `SHA-256` of the document state at one log prefix. Anyone must be able to rebuild that state from the log and get the same hash, today and after every library upgrade. So the hashed bytes need one encoding per state, fixed forever. This is a one-way door. Changing a single byte rule changes every node ID.

## Decision

`crdt.CanonicalBytes` encodes a state with this layout. This section is the normative spec. `tests/internal/oracle` holds a second encoder written from it alone, and the two must agree byte for byte.

Every integer is an unsigned LEB128 varint in its minimal form: seven bits per byte, least significant group first, the high bit set on every byte except the last, and no trailing `0x80` groups. Zero is the single byte `00`. A rune is its Unicode code point. An ID is two integers, `client` then `clock`. Root is the ID `(0, 0)`.

```
magic        the 4 ASCII bytes "CSS1" (43 53 53 31)
n_nodes      integer
n_nodes times, in document order:
  id         client, clock
  parent     client, clock (0, 0 for Root)
  value      integer, the code point
  flags      integer; bit 0 set means the insert is a copy; every other bit is zero
  n_deleted  integer
  n_deleted times, ascending by (clock, client): the delete op's client, clock
n_vector     integer
n_vector times, ascending by client: client, clock
n_collected  integer
n_collected times, ascending by client: client, clock
```

Document order is the RGA pre-order. A node comes before its children, and siblings come in descending `(clock, client)` order. That is the order `Text()` reads. Vector and collected entries with a zero clock are omitted. Nothing follows `n_collected`'s entries.

A state that holds pending operations has no canonical form, and `CanonicalBytes` returns `ErrPending`. Log prefixes never hold pending operations, because hubs reject missing dependencies and merges append in dependency order.

`FromCanonical` decodes, validates the state through `FromSnapshot`, re-encodes, and requires byte equality. That rejects non-minimal varints, trailing bytes, unknown flags, misordered siblings, deletes or vector entries, and a copy whose value differs from its parent's. Every state therefore has exactly one accepted encoding.

### Worked example

Client 1 types `é` at `1:1` and `i` at `1:2` after it, then deletes the `i` with `1:3`. A restore by client 2 re-creates the `i` as the copy `2:4`, whose parent is `1:2`. The text is `éi`. The vector is `{1: 3, 2: 4}`, and nothing has been collected.

```
43 53 53 31             "CSS1"
03                      n_nodes = 3
01 01  00 00  e9 01  00 00
                        id 1:1, parent Root, value U+00E9 (233 = e9 01), flags 0, n_deleted 0
01 02  01 01  69  00  01  01 03
                        id 1:2, parent 1:1, value 'i', flags 0, deleted by 1:3
02 04  01 02  69  01  00
                        id 2:4, parent 1:2, value 'i', flags 1 (copy), n_deleted 0
02  01 03  02 04        vector {1: 3, 2: 4}
00                      collected is empty
```

As one string, `4353533103 01010000e9010000 010201016900010103 02040102690100 0201030204 00` without the spaces. The test suite pins this vector and three others: the empty state `43535331000000`, a state with a collected vector, and a client ID that needs two varint bytes.

## Alternatives rejected

- **Protobuf `Snapshot` bytes.** Go's deterministic marshaling is documented as unstable across library versions and between languages. Field order, unknown fields and packed encodings are all writer choices. A hash over them would change with a dependency bump.
- **JSONB.** Postgres normalizes JSONB on write. It reorders keys and drops duplicates and whitespace, so the stored bytes are not the hashed bytes. The canonical bytes live in `bytea`.
- **Hashing the operation log.** The same state can come from many logs (merges, retries, dedupe), and a fork's prefix would need its own log copy to verify. Hashing the state lets identical states share one blob.

## Consequences

The format keeps the version vector and the collected vector. After Phase 5 collects tombstones, the vectors still say which operations the state includes, and containment checks still work on a node's blob alone.

A 20k-rune document encodes to about 300 KB. Blobs are flat now. The hash covers the flat bytes, so Phase 5 can store them as content-defined chunks under a manifest without changing any node ID.

A new field means a new magic, `CSS2`, and a new node hash domain. Existing nodes keep their IDs.
