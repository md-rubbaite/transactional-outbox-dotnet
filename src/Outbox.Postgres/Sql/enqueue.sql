insert into outbox_message (id, type, payload, partition_key, occurred_at, headers)
values (@id, @type, @payload, @partition_key, @occurred_at, cast(@headers as jsonb))
