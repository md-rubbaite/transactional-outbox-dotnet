update outbox_message
set next_attempt_at = @retry_at, last_error = @failure, locked_until = null, lease_token = null
where seq = @seq and lease_token = @lease_token and status = 0
