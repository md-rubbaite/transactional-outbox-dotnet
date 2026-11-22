# ADR-0001: Transactional outbox with a leased relay, per-key ordering and an inbox

- Status: accepted
- Date: 2026-11-22

## Context

A service changes its database and must tell other services (billing, notifications, search,
analytics) through a broker. Writing to the database and publishing to the broker are two separate
systems; there is no shared transaction between PostgreSQL and Kafka, RabbitMQ or Service Bus.
Every ordering of the two writes fails somewhere:

| Order | Crash between the two writes |
|---|---|
| Publish, then commit | Event for a change that never happened (phantom order, double shipment) |
| Commit, then publish | Change without its event (customer charged, no invoice) |
| Distributed transaction (2PC/XA) | Most brokers and managed databases do not support it; it couples availability |

## Decision

1. **Outbox.** The event is a row in `outbox_message`, inserted in the same database transaction as
   the business change. The event exists if and only if the change committed.
2. **Relay with leases.** One or more relays claim ready rows with
   `FOR UPDATE SKIP LOCKED`, set `locked_until` and a fresh `lease_token`, publish, and then mark
   the row sent. A crashed relay's lease expires and another relay retries. Completion requires the
   current lease token (fencing), so a relay that wakes up after its lease ran out cannot overwrite
   the outcome of the relay that took over.
3. **Per-key ordering.** A row is eligible only if it is the oldest unsent row of its
   `partition_key`. While a message for order 42 is in flight or waiting for a retry, later messages
   for order 42 wait; other orders proceed in parallel. Consumers see the events of one aggregate in
   the order they were written.
4. **Bounded retries.** Exponential backoff with full jitter; after the last attempt, or at once for
   a `PermanentPublishException`, the row is dead-lettered and can be requeued after a fix.
   A publish is cancelled before its lease ends, so a slow broker never leads to two relays
   publishing the same message at the same time.
5. **At-least-once delivery, idempotent consumers.** A crash after the broker acknowledged but before
   the row was marked sent republishes the message. That cannot be avoided without a distributed
   transaction, so consumers record the message id in an `inbox_message` table in the same
   transaction as their own writes, and skip ids they have already processed.
6. **Database clock for leases.** The PostgreSQL store uses `now()` of the database, so relays on hosts
   with skewed clocks agree on lease expiry.

## Consequences

- Good: no lost or phantom events, without a distributed transaction or a new piece of
  infrastructure.
- Good: verified twice, by in-process tests (crashes, stale leases, flaky broker, four concurrent
  relays) and by a harness that runs the exact embedded SQL against PostgreSQL with six concurrent
  relays, random crashes, failures and dead letters (`tools/sql_harness.py`, run in CI).
- Cost: publish latency equals the polling interval (default 1 s). Use `LISTEN/NOTIFY` or CDC
  (Debezium) for lower latency.
- Cost: the outbox table grows; purge sent rows on a schedule (`PurgeSentAsync`).
- Trade-off: a dead-lettered message no longer blocks its key, so later events of that key flow.
  Alert on dead letters; requeue restores the message, but after later ones.
- Trade-off: a poison message blocks its own key until it is dead-lettered. With the default policy
  (10 attempts, 1 s base, 15 min cap) that is bounded but can take a while; tune per message type.
