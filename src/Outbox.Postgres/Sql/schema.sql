-- transactional-outbox-dotnet schema, PostgreSQL 13+ (gen_random_uuid is built in).
create table if not exists outbox_message (
  seq             bigint generated always as identity primary key,
  id              uuid        not null unique,
  type            text        not null,
  payload         text        not null,
  partition_key   text        not null,
  occurred_at     timestamptz not null,
  headers         jsonb       not null default '{}'::jsonb,
  status          smallint    not null default 0 check (status in (0, 1, 2)), -- pending, sent, dead-lettered
  attempts        integer     not null default 0,
  next_attempt_at timestamptz not null default now(),
  locked_until    timestamptz,
  lease_token     uuid,
  last_error      text,
  created_at      timestamptz not null default now(),
  sent_at         timestamptz
);

-- Finds the head of each partition among pending rows; stays small because sent rows leave it.
create index if not exists outbox_message_pending on outbox_message (partition_key, seq) where status = 0;
create index if not exists outbox_message_sent_at on outbox_message (sent_at) where status = 1;
create index if not exists outbox_message_dead on outbox_message (seq) where status = 2;

create table if not exists inbox_message (
  consumer     text        not null,
  message_id   uuid        not null,
  processed_at timestamptz not null default now(),
  primary key (consumer, message_id)
);
