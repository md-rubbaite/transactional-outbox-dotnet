insert into inbox_message (consumer, message_id) values (@consumer, @message_id)
on conflict (consumer, message_id) do nothing
