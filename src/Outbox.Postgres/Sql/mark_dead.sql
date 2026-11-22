update outbox_message
set status = 2, last_error = @failure, locked_until = null, lease_token = null
where seq = @seq and lease_token = @lease_token and status = 0
