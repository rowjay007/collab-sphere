# ADR 0013. Identity is a signed handle, roles live in permission-service

Status: accepted, Phase 4.

## Context

Nothing authenticates. The hub accepts a WebSocket with `client`, `seq`, and `conn`. Every gRPC server is `grpc.NewServer()` with no interceptor. `owner_id` and comment `author` are client-supplied strings. A revoke has to take effect inside two seconds. ADR 0008 treats Redis pub/sub loss as normal, so a lost invalidation would leave a revoked editor writing past that bound.

There is no user directory. Comments already store a handle that matches `[a-z0-9][a-z0-9._-]{0,63}`. That grammar is the user id.

## Decision

**Tokens.** `pkg/auth` is the only parser. A user token is Ed25519 over a length-prefixed payload of subject, issued-at, and expiry, both unix seconds. The wire form is `v1.` plus base64url of the payload, plus `.`, plus base64url of the signature. Services hold `AUTH_PUBLIC_KEY`, the 32-byte public key in standard base64. Only tests and `cmd/devtoken` hold `AUTH_PRIVATE_KEY`. A service cannot mint a user. Verification allows 30 seconds of skew. There is no denylist and no role claim inside the token. Document revoke does not wait for expiry, because roles are not inside the token.

**Credential classes.** User tokens and internal calls use different `Authorization` schemes. Internal calls send `Bearer internal.` plus 64 hex characters. Every backend process gets the same `INTERNAL_TOKEN`. The interceptor compares with constant time. A user token on an internal method is `Unauthenticated`, and so is an internal token on a user method. Unknown methods are refused. The interceptor puts the verified subject in the context.

`PolicyDump` and `EnsureOwner` take the internal token. Grant, revoke, share-link, and redeem take a user token. Owner-only actions use that token subject.

**Roles.** One enum, high to low: `owner`, `editor`, `commenter`, `viewer`. A document has one owner, the creator. The owner cannot be revoked. Ownership transfer is out of scope. permission-service owns `grants` and `share_links`. A share link is `s1.` signed by `SHARE_PRIVATE_KEY`. Redeem never demotes. A bad, revoked, or expired link is `FailedPrecondition` with reason `LINK_BAD`, `LINK_REVOKED`, or `LINK_EXPIRED`.

**Revocation poll.** The hub does not put a role in the socket and forget it. Every 500ms, while a document has peers, the hub calls `PolicyDump` and replaces that document's member map, write epoch, and epoch state. If `PolicyDump` has not succeeded for 2 seconds, the hub closes that document's peers with `CODE_UNAVAILABLE` and stops applying ops. It does not keep the last map past the bound. A Redis publish is not the revoke signal.

## Alternatives rejected

- **HS256 with a shared secret.** Every service could mint users.
- **A role claim inside the token.** The claim would stay true until expiry and miss the 2 second revoke bound.
- **A JWT library, a uuid user table, or a session denylist.** The handle already exists. There is no password and no directory.
- **Redis pub/sub as the revoke signal.** Pub/sub loss is an accepted property of ADR 0008. The 500ms poll meets the bound without a second channel.
- **An opaque random share URL with no signature, or a signed link with no row.** The first cannot bind fields. The second cannot be revoked.
- **Downgrade-on-redeem.** A viewer link would become a way to strip the owner.

## Consequences

A missing or bad token no longer opens a socket or an RPC. A stolen user token works until it expires. Revoke still closes the live socket because the hub rereads `PolicyDump`. Active documents cost one `PolicyDump` per hub per half second. Wave 2 wires these credentials across the other services. Epoch and wrap RPCs stay later. The tables exist now so encryption does not reopen the schema.
