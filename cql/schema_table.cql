CREATE TABLE IF NOT EXISTS task (
    uid        BLOB,      -- user id, 12 bytes, https://docs.rs/xid/latest/xid/
    id         BLOB,      -- task id
    gid        BLOB,      -- group id
    status     TINYINT,   -- task status, -1: Rejected, 0: Processing, 1: Resolved
    kind       TEXT,      -- task kind
    created_at BIGINT,    -- create at
    updated_at BIGINT,    -- update at
    duedate    BIGINT,    -- due date
    threshold  SMALLINT,   -- threshold, default 1
    receivers  SET<BLOB>, -- receivers
    resolved   SET<BLOB>, -- who approved task from receivers
    rejected   SET<BLOB>, -- who rejected task from receivers
    message    TEXT,      -- additional message
    payload    BLOB,      -- task detail in CBOR format
    PRIMARY KEY (uid, id)
) WITH CLUSTERING ORDER BY (id DESC)
    AND caching = {'enabled': 'true'}
    AND comment = 'task'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX task_uid_status ON task ((uid), status);

CREATE TABLE IF NOT EXISTS notification (
    uid     BLOB,    -- user id, who receive task
    tid     BLOB,    -- task id
    sender  BLOB,    -- user id, who create task
    status  TINYINT, -- resolve or rejected task, -1: Rejected, 0: Processing, 1: Resolved
    message TEXT,    -- additional message
    PRIMARY KEY (uid, tid, sender)
) WITH CLUSTERING ORDER BY (tid DESC, sender ASC)
    AND caching = {'enabled': 'true'}
    AND comment = 'notification'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX notification_uid_status ON notification ((uid), status);
CREATE INDEX notification_uid_tid ON notification (tid);

CREATE TABLE IF NOT EXISTS group_notification (
    gid    BLOB,    -- group id, who receive task
    tid    BLOB,    -- task id
    sender BLOB,    -- user id, who create task
    role   TINYINT, -- minimum role to receive task, default 1, admins and owners
    PRIMARY KEY (gid, tid, sender)
) WITH CLUSTERING ORDER BY (tid DESC, sender ASC)
    AND caching = {'enabled': 'true'}
    AND comment = 'notification'
    AND compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'LZ4Compressor'}
    AND default_time_to_live = 0;

CREATE INDEX group_notification_gid_role ON group_notification ((gid), role);
