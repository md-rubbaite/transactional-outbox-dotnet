#!/usr/bin/env python3
"""
Concurrency check of the exact SQL the PostgreSQL store embeds (src/Outbox.Postgres/Sql/*.sql),
run against a real PostgreSQL server.

Six relays claim, publish, fail, crash and retry over 600 messages in 30 partitions. It asserts:
  * no message is leased by two relays at the same time;
  * a message is published only when every earlier message of its partition is finished
    (per-key ordering);
  * every message ends up sent or dead-lettered;
  * rolled-back business transactions leave no outbox rows;
  * the inbox reports a concurrent duplicate delivery exactly once.

Usage: OUTBOX_PG="host=127.0.0.1 port=5432 user=postgres dbname=postgres" python tools/sql_harness.py
"""
import os
import random
import re
import threading
import time
import uuid
from pathlib import Path

import psycopg

SQL_DIR = Path(__file__).resolve().parent.parent / "src" / "Outbox.Postgres" / "Sql"
DSN = os.environ.get("OUTBOX_PG", "host=127.0.0.1 port=5432 user=postgres dbname=postgres")
LEASE_SECONDS = 0.4


def sql(name: str) -> str:
    text = (SQL_DIR / f"{name}.sql").read_text()
    text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))
    return re.sub(r"(?<![@\w])@([a-z_]+)", r"%(\1)s", text)


def reset(conn):
    conn.execute("drop table if exists outbox_message, inbox_message")
    conn.execute((SQL_DIR / "schema.sql").read_text())


def main():
    rnd = random.Random(42)
    with psycopg.connect(DSN, autocommit=True) as admin:
        reset(admin)
        # 1. Enqueue in business transactions; every fifth one rolls back.
        partitions = [f"order-{i}" for i in range(30)]
        expected = 0
        for i in range(750):
            with psycopg.connect(DSN) as conn:
                conn.execute(sql("enqueue"), {
                    "id": uuid.uuid4(), "type": "order.updated.v1", "payload": '{"n": %d}' % i,
                    "partition_key": rnd.choice(partitions), "occurred_at": "now()", "headers": '{"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}',
                })
                if i % 5 == 4:
                    conn.rollback()
                else:
                    conn.commit()
                    expected += 1
        count = admin.execute("select count(*) from outbox_message").fetchone()[0]
        assert count == expected == 600, (count, expected)

        lock = threading.Lock()
        leases = {}          # seq -> locked_until of a lease not yet released
        published = []       # (partition, seq)
        violations = []
        stop = threading.Event()

        def relay(worker: int):
            wr = random.Random(worker)
            with psycopg.connect(DSN, autocommit=True) as conn:
                while not stop.is_set():
                    rows = conn.execute(sql("claim"), {"batch_size": 10, "lease_seconds": LEASE_SECONDS}).fetchall()
                    if not rows:
                        time.sleep(0.01)
                        continue
                    for seq, _id, _type, _payload, part, _occ, _headers, attempts, token, locked_until in rows:
                        with lock:
                            prior = leases.get(seq)
                            new_start = locked_until.timestamp() - LEASE_SECONDS
                            if prior is not None and new_start < prior - 1e-6:
                                violations.append(f"seq {seq} leased again while lease ran until {prior}")
                            leases[seq] = locked_until.timestamp()
                        earlier = conn.execute(
                            "select count(*) from outbox_message where partition_key = %s and seq < %s and status = 0",
                            (part, seq)).fetchone()[0]
                        if earlier:
                            violations.append(f"seq {seq} published before {earlier} earlier message(s) of {part}")
                        roll = wr.random()
                        if roll < 0.05:
                            continue  # crash: lease expires, another relay retries
                        with lock:
                            published.append((part, seq))
                            leases.pop(seq, None)  # released below, so an immediate re-lease is legitimate
                        params = {"seq": seq, "lease_token": token}
                        if roll < 0.15:
                            conn.execute(sql("mark_retry"), {**params, "failure": "broker timeout",
                                         "retry_at": conn.execute("select now() + interval '50 milliseconds'").fetchone()[0]})
                        elif roll < 0.16 and attempts >= 2:
                            conn.execute(sql("mark_dead"), {**params, "failure": "schema rejected"})
                        else:
                            conn.execute(sql("mark_sent"), params)

        threads = [threading.Thread(target=relay, args=(w,)) for w in range(6)]
        for t in threads:
            t.start()
        deadline = time.time() + 120
        while time.time() < deadline:
            left = admin.execute("select count(*) from outbox_message where status = 0").fetchone()[0]
            if left == 0:
                break
            time.sleep(0.1)
        stop.set()
        for t in threads:
            t.join()

        assert not violations, violations[:5]
        status = dict(admin.execute("select status, count(*) from outbox_message group by status").fetchall())
        assert status.get(0, 0) == 0, status
        assert status.get(1, 0) + status.get(2, 0) == 600, status
        sent = {r[0] for r in admin.execute("select seq from outbox_message where status = 1")}
        assert sent <= {s for _, s in published}, "a message marked sent was never published"
        print(f"relays: {len(published)} publishes for 600 messages, status {status}, no ordering or lease violations")

        # 2. Requeue a dead letter and purge sent rows.
        dead = admin.execute("select id from outbox_message where status = 2 limit 1").fetchone()
        if dead:
            assert admin.execute(sql("requeue"), {"id": dead[0]}).rowcount == 1
        purged = admin.execute(sql("purge_sent"), {"older_than_seconds": 0.0, "batch_size": 100}).rowcount
        assert purged == 100, purged

        # 3. Concurrent duplicate delivery to the inbox.
        msg = uuid.uuid4()
        a = psycopg.connect(DSN)
        b = psycopg.connect(DSN)
        assert a.execute(sql("inbox_record"), {"consumer": "billing", "message_id": msg}).rowcount == 1
        result = {}
        t = threading.Thread(target=lambda: result.setdefault(
            "b", b.execute(sql("inbox_record"), {"consumer": "billing", "message_id": msg}).rowcount))
        t.start()
        time.sleep(0.3)
        assert "b" not in result, "second delivery must wait for the first transaction"
        a.commit()
        t.join()
        assert result["b"] == 0, result
        b.commit()
        a.close()
        b.close()
        print("inbox: concurrent duplicate detected once; requeue and purge work")


if __name__ == "__main__":
    main()
