# ADR 0007. Each hub delivers ops in strict seq order

Status: accepted, Phase 2.

## Context

document-service gives every op a `seq` under a row lock on the document, so seqs are the one global order of the log. In Phase 1 the hub broadcast each group commit as it returned. With one hub per document that order matched the log. Once several hubs append to one document, a hub can receive seq 13 before it learns about 10 to 12. Reconnecting clients also need a single number to resume from, and that only works if every frame they receive extends a contiguous prefix of the log.

## Decision

Each hub keeps `D`, the highest seq it has delivered to its sockets. `Load` returns the document together with `op_seq`, which is the starting `D`.

A `fanout.Sequencer` sits between the log and the sockets. It holds `D` and a sorted set of disjoint ranges above `D`. `Offer` trims whatever is already delivered or buffered and returns the batches that now continue from `D+1`. The hub applies each returned op to its document and sends `OperationBroadcast{first_seq, ops}` to every local peer, the sender included. The hub's own ops are already in the document, so `Apply` reports them as duplicates.

After a group commit, `Append` returns one seq per op. The hub splits the accepted ops into runs of contiguous seqs and offers every run. A run can contain ops the log already held above `D`, because a client resent an op another hub appended. That reveals ops the hub has not seen yet, the same way a gap does.

Between events the hub's document is `log[1..D]` plus `G`, the ops this hub appended that still wait above `D`. Remote ops are applied only when their seq is delivered.

**Gap healing.** When ranges wait above a missing `D+1`, the hub arms a 30ms timer. The timer is an event in the hub loop, tagged with a generation so a stale one is ignored. When it fires and the gap is still open, the hub calls `ReadOperations(after=D, limit=4096)` and offers the result, repeating until nothing waits. With more than 64 ranges buffered it reads at once. A failed or empty read retries on the next timer with doubled delay. Three failures in a row fail-stop the hub with `CODE_UNAVAILABLE`.

**DocumentSync.** Before building a snapshot for a joining client, the hub fills the gap until `G` is empty. The snapshot is then exactly `log[1..D]`, and `DocumentSync.seq` is `D`. A client never builds on an op that another hub may not have delivered yet.

**Acknowledgement.** A sender's op is acknowledged when it appears in the sequenced stream. There is no separate ack frame. The echo proves the op is durable and tells the client its seq.

**Newest connection wins.** A second join for a client ID already on this hub closes the old peer with reason `superseded` and admits the new one. Nobody sees a Left, because the client is still here. An auto-reconnect that lands on the same hub while the old socket is half-open therefore succeeds.

**Append failure** still fail-stops the hub. Peers now get `CODE_UNAVAILABLE`, which tells them to reconnect and resend.

## Alternatives rejected

- **Broadcast each commit as soon as it returns.** Frames would be out of seq order across hubs, and a client could only resume from a set of seqs instead of one number.
- **A separate `Ack` message.** It would be a second ordering channel to keep consistent with the stream, and the echo already carries durability and position.
- **Hold remote ops with missing dependencies in the CRDT's pending map.** Memory grows without bound for a hostile client, and the sender's view drifts from everyone else's.

## Consequences

An echo can wait for a gap to heal, at worst the grace period plus one range read. The fan-out unit measures this under cross-hub contention and tunes the grace period. The hub works with no bus at all. Its own appends are the only source of ranges, and a log written by another hub shows up through the duplicate signal or the next append's gap. `collabclient` checks that each frame starts at `Seq()+1` and closes the connection otherwise, since a break in the stream is a server bug.
