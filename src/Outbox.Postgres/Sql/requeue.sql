update outbox_message
set status = 0, next_attempt_at = now(), attempts = 0
where id = @id and status = 2
