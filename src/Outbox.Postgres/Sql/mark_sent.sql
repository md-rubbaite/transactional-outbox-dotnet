update outbox_message
set status = 1, sent_at = now(), locked_until = null, lease_token = null
where seq = @seq and lease_token = @lease_token and status = 0
