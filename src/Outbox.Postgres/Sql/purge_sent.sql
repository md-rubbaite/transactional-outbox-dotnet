delete from outbox_message
where seq in (
  select seq from outbox_message
  where status = 1 and sent_at < now() - make_interval(secs => @older_than_seconds)
  order by seq
  limit @batch_size
)
