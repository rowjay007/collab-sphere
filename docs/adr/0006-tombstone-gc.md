# ADR 0006. Tombstone GC removes leaf tombstones under a stable version vector

Status: accepted for the library. Not yet wired into a service.

## Context

Tombstones never go away on their own. A document that is edited for months fills with deleted nodes. Removing one is only safe if no replica will ever again reference it, or place an insert relative to it.

## Decision

`(*crdt.Doc).GC(stable)` removes a node T when all of these hold:

1. T is deleted.
2. T has no children.
3. `stable` covers T's ID and at least one of T's delete IDs.

Removing a node can leave its parent eligible, so GC repeats up the tree. It never moves children to a new parent. Here is why. Say root has children A (clock 2) and T (clock 1), and T has a child B (clock 5). The text reads "AB". Moving B up to root sorts it before A, and the text becomes "BA".

`stable` must be a version-vector prefix that every replica has fully received, under per-client FIFO delivery. GC refuses a `stable` that its own vector does not cover. After GC, the doc remembers `stable` in a `collected` vector. An op that `collected` covers but the doc no longer holds is a late duplicate, and `Apply` reports it as one.

## Why it is safe

Each replica that has seen `stable` has a Lamport clock above every ID in it. So every future insert sorts ahead of every stable node among its siblings. A future insert never has T as its parent, because a replica that has seen T's delete never chooses a deleted character as a parent. An insert made before that replica saw the delete was sent before its acknowledgement, so under FIFO it has already arrived. Once it arrives, T has a child and condition 2 fails. T contributes nothing to visible text, and it has no subtree, so removing it moves no other node.

## Consequences

The hard part is computing `stable`. It needs the set of every replica, including offline ones that may come back after 24 hours, and each one's acknowledged vector. Guessing it from connected sockets would be unsafe. Phase 1 therefore ships GC in the library with tests and calls it from no service. The Phase 5 GC pipeline adds replica membership and acknowledgements and then calls it.
