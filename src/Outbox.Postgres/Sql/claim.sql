-- Lease the oldest pending message of each partition key, if it is due and not leased.
-- The row conditions are repeated in the locking query on purpose: under READ COMMITTED,
-- PostgreSQL re-checks them against the latest row version after waiting for a lock, so a row a
-- concurrent relay leased a moment ago is skipped instead of being leased twice.
with heads as (
  select distinct on (partition_key) seq
  from outbox_message
  where status = 0
  order by partition_key, seq
),
ready as (
  select m.seq
  from outbox_message m
  join heads h on h.seq = m.seq
  where m.status = 0
    and (m.locked_until is null or m.locked_until < now())
    and m.next_attempt_at <= now()
  order by m.seq
  limit @batch_size
  for update of m skip locked
)
update outbox_message m
set attempts     = m.attempts + 1,
    lease_token  = gen_random_uuid(),
    locked_until = now() + make_interval(secs => @lease_seconds)
from ready
where m.seq = ready.seq
returning m.seq, m.id, m.type, m.payload, m.partition_key, m.occurred_at, m.headers::text,
          m.attempts, m.lease_token, m.locked_until
