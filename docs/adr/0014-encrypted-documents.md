# ADR 0014. Encrypted documents

Status: accepted, Phase 4.

## Context

An encrypted document stores ciphertext in the log, in snapshots, and in version blobs. The server never holds the document key. Plain documents already have CSS1 bytes and CSV1 node ids. ADR 0010 says a new byte rule needs a new magic and a new node-id domain.

Restore copies an old character onto a new id. The server cannot re-seal that copy. If the ciphertext were sealed under the copy's own id, restore would not decrypt.

## Decision

### Value mode

A document is plain or encrypted from the first insert. The mode never changes.

`New` returns a plain document. `NewEncrypted` returns an encrypted document. A protobuf snapshot carries the mode in `Snapshot.encrypted`. An unset field means plain, so snapshots written before this field stay plain. `FromSnapshot` rejects a node that does not match the field.

Canonical bytes carry the mode in the magic. `CSS1` is plain. `CSS2` is encrypted. `FromCanonical` reads the magic and refuses any other prefix.

An insert carries a rune or a ciphertext, and never both. A plain document accepts only runes. An encrypted document accepts only ciphertext. `OpFromProto` and `Apply` reject every other shape. Ciphertext is AES-256-GCM output with the tag and without the nonce. Its length is 17, 18, 19, or 20 bytes. A rune is 1 to 4 UTF-8 bytes and the tag is 16 bytes.

`Text`, `CanonicalBytes`, and `Diff` return `ErrEncrypted` for an encrypted document. `ResolveRange` still returns positions and `Detached`. It sets `Text` empty and `TextOmitted` true. The server has no key, so those functions do not invent plaintext.

A copy repeats its parent's rune, or its parent's cipher bytes and epoch. `PlanRestore` copies those cipher bytes onto the new insert.

### Nonce and associated data

`pkg/docrypt` seals one rune under a 32-byte document key. The nonce is the first 12 bytes of SHA-256 over the document id, the epoch as 4 big-endian bytes, the lineage root client as 8 big-endian bytes, and the lineage root clock as 8 big-endian bytes. The associated data is the ASCII prefix `CSAD1` followed by those same fields. The nonce and the associated data are not stored.

The lineage root of an ordinary insert is its own id. The lineage root of a copy is the original character. That is the parent chain Phase 3 already walks. Restore copies the ciphertext and leaves it sealed under that root, so a client can still open it.

The nonce is deterministic, so a client must never seal two different runes under one (replica, clock) in one epoch. GCM with a repeated nonce leaks the XOR of the two runes and the authentication key to the server, which is the party this mode defends against. A client that loses its local clock state claims a fresh replica instead of resuming the old one. Resealing the same rune under the same id is harmless because it yields the same bytes.

### Key wrap

A document key is wrapped to a member's X25519 public key. The wrapper generates an ephemeral X25519 key, derives a shared secret, and derives a 32-byte wrap key with HKDF-SHA256. The salt is the document id. The info is the ASCII prefix `collab-wrap-v1`, the epoch as 4 big-endian bytes, and the user id. AES-256-GCM encrypts the document key with a random 12-byte nonce. The associated data is the document id, the epoch, the user id, and the key id. The wrap stores the ephemeral public key, the nonce, and the 48-byte sealed key. Services do not import `pkg/docrypt`.

### CSS2 and CSV2

`CanonicalCSS2` encodes an encrypted document. `CanonicalBytes` refuses one. Integers use the same unsigned LEB128 rules as ADR 0010.

```
magic "CSS2"
n_nodes, then per node in document order:
  id, parent, epoch,
  n_bytes, bytes,
  flags with bit 0 meaning copy,
  n_deleted, then delete ids ascending
n_vector, then n_collected, same rules as CSS1
```

The empty encrypted state is the eight bytes `43 53 53 32 00 00 00`. A version node whose content hash is a CSS2 blob uses the prefix `CSV2` and is otherwise the Phase 3 formula. Plain nodes stay `CSV1` and CSS1. `pkg/crdt` does not hash version nodes. version-service writes `CSV2` when it accepts encrypted blobs.

## Alternatives rejected

- Merge, branch, fork, and diff of an encrypted document. A copied op is sealed under the source document id, and the server cannot re-seal it. A diff that returned empty text would look like a real result.
- Sealing associated data under the copy's own id. Server restore would not decrypt.
- Reusing CSS1 with cipher bytes in the rune field. That changes plain hashes, or it stores ciphertext in a field the rest of the code reads as text.
- One ciphertext for the whole document. That gives up character ids.
- XChaCha20-Poly1305. The design names AES-256-GCM.
- A tree-wide key so merge could work. That is a second design.

## Consequences

The hub rejects a cipher whose length is outside 17..20, and an operation whose shape does not match the document mode, with `CODE_INVALID_OPERATION`. The check that the epoch is the open write epoch arrives with the service wiring. Clients seal and open. The server stores the bytes.
