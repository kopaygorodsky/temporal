-- Oracle equivalent data types:
-- BINARY(16) -> RAW(16)
-- MEDIUMBLOB -> BLOB
-- TINYINT -> NUMBER(1)
-- BIGINT -> NUMBER(19)
-- INT -> NUMBER(10)
-- DATETIME(6) -> TIMESTAMP(6)
-- VARCHAR -> VARCHAR2
-- AUTO_INCREMENT -> Using sequences and triggers

-- Create sequences first
CREATE SEQUENCE buffered_events_seq START WITH 1 INCREMENT BY 1 NOCACHE;

CREATE TABLE namespaces (
                            partition_id NUMBER(10) NOT NULL,
                            id RAW(16) NOT NULL,
                            name VARCHAR2(255) NOT NULL,
                            notification_version NUMBER(19) NOT NULL,
                            data BLOB NOT NULL,
                            data_encoding VARCHAR2(16) NOT NULL,
                            is_global NUMBER(1) NOT NULL,
                            PRIMARY KEY(partition_id, id),
                            CONSTRAINT namespaces_name_unique UNIQUE (name)
);

CREATE TABLE namespace_metadata (
                                    partition_id NUMBER(10) NOT NULL,
                                    notification_version NUMBER(19) NOT NULL,
                                    PRIMARY KEY(partition_id)
);

INSERT INTO namespace_metadata (partition_id, notification_version) VALUES (54321, 1);

CREATE TABLE shards (
                        shard_id NUMBER(10) NOT NULL,
                        range_id NUMBER(19) NOT NULL,
                        data BLOB NOT NULL,
                        data_encoding VARCHAR2(16) NOT NULL,
                        PRIMARY KEY (shard_id)
);

CREATE TABLE executions (
                            shard_id NUMBER(10) NOT NULL,
                            namespace_id RAW(16) NOT NULL,
                            workflow_id VARCHAR2(255) NOT NULL,
                            run_id RAW(16) NOT NULL,
                            next_event_id NUMBER(19) NOT NULL,
                            last_write_version NUMBER(19) NOT NULL,
                            data BLOB NOT NULL,
                            data_encoding VARCHAR2(16) NOT NULL,
                            state BLOB NOT NULL,
                            state_encoding VARCHAR2(16) NOT NULL,
                            db_record_version NUMBER(19) DEFAULT 0 NOT NULL,
                            PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id)
);

CREATE TABLE current_executions (
                                    shard_id NUMBER(10) NOT NULL,
                                    namespace_id RAW(16) NOT NULL,
                                    workflow_id VARCHAR2(255) NOT NULL,
                                    run_id RAW(16) NOT NULL,
                                    create_request_id VARCHAR2(255) NOT NULL,
                                    state NUMBER(10) NOT NULL,
                                    status NUMBER(10) NOT NULL,
                                    start_version NUMBER(19) DEFAULT 0 NOT NULL,
                                    start_time TIMESTAMP(6),
                                    last_write_version NUMBER(19) NOT NULL,
                                    PRIMARY KEY (shard_id, namespace_id, workflow_id)
);

CREATE TABLE buffered_events (
                                 shard_id NUMBER(10) NOT NULL,
                                 namespace_id RAW(16) NOT NULL,
                                 workflow_id VARCHAR2(255) NOT NULL,
                                 run_id RAW(16) NOT NULL,
                                 id NUMBER(19) NOT NULL,
                                 data BLOB NOT NULL,
                                 data_encoding VARCHAR2(16) NOT NULL,
                                 PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, id)
);

-- Create trigger for buffered_events auto-increment
CREATE OR REPLACE TRIGGER buffered_events_trigger
              BEFORE INSERT ON buffered_events
                         FOR EACH ROW
BEGIN
SELECT buffered_events_seq.NEXTVAL INTO :NEW.id FROM dual;
END;
/

CREATE TABLE tasks (
                       range_hash NUMBER(10) NOT NULL,
                       task_queue_id RAW(272) NOT NULL,
                       task_id NUMBER(19) NOT NULL,
                       data BLOB NOT NULL,
                       data_encoding VARCHAR2(16) NOT NULL,
                       PRIMARY KEY (range_hash, task_queue_id, task_id)
);

CREATE TABLE task_queues (
                             range_hash NUMBER(10) NOT NULL,
                             task_queue_id RAW(272) NOT NULL,
                             range_id NUMBER(19) NOT NULL,
                             data BLOB NOT NULL,
                             data_encoding VARCHAR2(16) NOT NULL,
                             PRIMARY KEY (range_hash, task_queue_id)
);

CREATE TABLE task_queue_user_data (
                                      namespace_id RAW(16) NOT NULL,
                                      task_queue_name VARCHAR2(255) NOT NULL,
                                      data BLOB NOT NULL,
                                      data_encoding VARCHAR2(16) NOT NULL,
                                      version NUMBER(19) NOT NULL,
                                      PRIMARY KEY (namespace_id, task_queue_name)
);

CREATE TABLE build_id_to_task_queue (
                                        namespace_id RAW(16) NOT NULL,
                                        build_id VARCHAR2(255) NOT NULL,
                                        task_queue_name VARCHAR2(255) NOT NULL,
                                        PRIMARY KEY (namespace_id, build_id, task_queue_name)
);

CREATE TABLE history_immediate_tasks (
                                         shard_id NUMBER(10) NOT NULL,
                                         category_id NUMBER(10) NOT NULL,
                                         task_id NUMBER(19) NOT NULL,
                                         data BLOB NOT NULL,
                                         data_encoding VARCHAR2(16) NOT NULL,
                                         PRIMARY KEY (shard_id, category_id, task_id)
);

CREATE TABLE history_scheduled_tasks (
                                         shard_id NUMBER(10) NOT NULL,
                                         category_id NUMBER(10) NOT NULL,
                                         visibility_timestamp TIMESTAMP(6) NOT NULL,
                                         task_id NUMBER(19) NOT NULL,
                                         data BLOB NOT NULL,
                                         data_encoding VARCHAR2(16) NOT NULL,
                                         PRIMARY KEY (shard_id, category_id, visibility_timestamp, task_id)
);

CREATE TABLE transfer_tasks (
                                shard_id NUMBER(10) NOT NULL,
                                task_id NUMBER(19) NOT NULL,
                                data BLOB NOT NULL,
                                data_encoding VARCHAR2(16) NOT NULL,
                                PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE timer_tasks (
                             shard_id NUMBER(10) NOT NULL,
                             visibility_timestamp TIMESTAMP(6) NOT NULL,
                             task_id NUMBER(19) NOT NULL,
                             data BLOB NOT NULL,
                             data_encoding VARCHAR2(16) NOT NULL,
                             PRIMARY KEY (shard_id, visibility_timestamp, task_id)
);

CREATE TABLE replication_tasks (
                                   shard_id NUMBER(10) NOT NULL,
                                   task_id NUMBER(19) NOT NULL,
                                   data BLOB NOT NULL,
                                   data_encoding VARCHAR2(16) NOT NULL,
                                   PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE replication_tasks_dlq (
                                       source_cluster_name VARCHAR2(255) NOT NULL,
                                       shard_id NUMBER(10) NOT NULL,
                                       task_id NUMBER(19) NOT NULL,
                                       data BLOB NOT NULL,
                                       data_encoding VARCHAR2(16) NOT NULL,
                                       PRIMARY KEY (source_cluster_name, shard_id, task_id)
);

CREATE TABLE visibility_tasks (
                                  shard_id NUMBER(10) NOT NULL,
                                  task_id NUMBER(19) NOT NULL,
                                  data BLOB NOT NULL,
                                  data_encoding VARCHAR2(16) NOT NULL,
                                  PRIMARY KEY (shard_id, task_id)
);

-- Info Maps tables
CREATE TABLE activity_info_maps (
                                    shard_id NUMBER(10) NOT NULL,
                                    namespace_id RAW(16) NOT NULL,
                                    workflow_id VARCHAR2(255) NOT NULL,
                                    run_id RAW(16) NOT NULL,
                                    schedule_id NUMBER(19) NOT NULL,
                                    data BLOB NOT NULL,
                                    data_encoding VARCHAR2(16),
                                    PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, schedule_id)
);

CREATE TABLE timer_info_maps (
                                 shard_id NUMBER(10) NOT NULL,
                                 namespace_id RAW(16) NOT NULL,
                                 workflow_id VARCHAR2(255) NOT NULL,
                                 run_id RAW(16) NOT NULL,
                                 timer_id VARCHAR2(255) NOT NULL,
                                 data BLOB NOT NULL,
                                 data_encoding VARCHAR2(16),
                                 PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, timer_id)
);

CREATE TABLE child_execution_info_maps (
                                           shard_id NUMBER(10) NOT NULL,
                                           namespace_id RAW(16) NOT NULL,
                                           workflow_id VARCHAR2(255) NOT NULL,
                                           run_id RAW(16) NOT NULL,
                                           initiated_id NUMBER(19) NOT NULL,
                                           data BLOB NOT NULL,
                                           data_encoding VARCHAR2(16),
                                           PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE request_cancel_info_maps (
                                          shard_id NUMBER(10) NOT NULL,
                                          namespace_id RAW(16) NOT NULL,
                                          workflow_id VARCHAR2(255) NOT NULL,
                                          run_id RAW(16) NOT NULL,
                                          initiated_id NUMBER(19) NOT NULL,
                                          data BLOB NOT NULL,
                                          data_encoding VARCHAR2(16),
                                          PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE signal_info_maps (
                                  shard_id NUMBER(10) NOT NULL,
                                  namespace_id RAW(16) NOT NULL,
                                  workflow_id VARCHAR2(255) NOT NULL,
                                  run_id RAW(16) NOT NULL,
                                  initiated_id NUMBER(19) NOT NULL,
                                  data BLOB NOT NULL,
                                  data_encoding VARCHAR2(16),
                                  PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE signals_requested_sets (
                                        shard_id NUMBER(10) NOT NULL,
                                        namespace_id RAW(16) NOT NULL,
                                        workflow_id VARCHAR2(255) NOT NULL,
                                        run_id RAW(16) NOT NULL,
                                        signal_id VARCHAR2(255) NOT NULL,
                                        PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, signal_id)
);

-- History events V2
CREATE TABLE history_node (
                              shard_id NUMBER(10) NOT NULL,
                              tree_id RAW(16) NOT NULL,
                              branch_id RAW(16) NOT NULL,
                              node_id NUMBER(19) NOT NULL,
                              txn_id NUMBER(19) NOT NULL,
                              prev_txn_id NUMBER(19) DEFAULT 0 NOT NULL,
                              data BLOB NOT NULL,
                              data_encoding VARCHAR2(16) NOT NULL,
                              PRIMARY KEY (shard_id, tree_id, branch_id, node_id, txn_id)
);

CREATE TABLE history_tree (
                              shard_id NUMBER(10) NOT NULL,
                              tree_id RAW(16) NOT NULL,
                              branch_id RAW(16) NOT NULL,
                              data BLOB NOT NULL,
                              data_encoding VARCHAR2(16) NOT NULL,
                              PRIMARY KEY (shard_id, tree_id, branch_id)
);

CREATE TABLE queue (
                       queue_type NUMBER(10) NOT NULL,
                       message_id NUMBER(19) NOT NULL,
                       message_payload BLOB NOT NULL,
                       message_encoding VARCHAR2(16) NOT NULL,
                       PRIMARY KEY(queue_type, message_id)
);

CREATE TABLE queue_metadata (
                                queue_type NUMBER(10) NOT NULL,
                                data BLOB NOT NULL,
                                data_encoding VARCHAR2(16) NOT NULL,
                                version NUMBER(19) NOT NULL,
                                PRIMARY KEY(queue_type)
);

CREATE TABLE cluster_metadata_info (
                                       metadata_partition NUMBER(10) NOT NULL,
                                       cluster_name VARCHAR2(255) NOT NULL,
                                       data BLOB NOT NULL,
                                       data_encoding VARCHAR2(16) NOT NULL,
                                       version NUMBER(19) NOT NULL,
                                       PRIMARY KEY(metadata_partition, cluster_name)
);

CREATE TABLE cluster_membership (
                                    membership_partition NUMBER(10) NOT NULL,
                                    host_id RAW(16) NOT NULL,
                                    rpc_address VARCHAR2(128) NOT NULL,
                                    rpc_port NUMBER(5) NOT NULL,
                                    role NUMBER(3) NOT NULL,
                                    session_start TIMESTAMP(6) DEFAULT TIMESTAMP '1970-01-02 00:00:01',
                                    last_heartbeat TIMESTAMP(6) DEFAULT TIMESTAMP '1970-01-02 00:00:01',
                                    record_expiry TIMESTAMP(6) DEFAULT TIMESTAMP '1970-01-02 00:00:01',
                                    PRIMARY KEY (membership_partition, host_id)
);

-- Create indexes for cluster_membership
CREATE INDEX idx_membership_role_host ON cluster_membership(role, host_id);
CREATE INDEX idx_membership_role_heartbeat ON cluster_membership(role, last_heartbeat);
CREATE INDEX idx_membership_rpc_role ON cluster_membership(rpc_address, role);
CREATE INDEX idx_membership_heartbeat ON cluster_membership(last_heartbeat);
CREATE INDEX idx_membership_expiry ON cluster_membership(record_expiry);

CREATE TABLE queues (
                        queue_type NUMBER(10) NOT NULL,
                        queue_name VARCHAR2(255) NOT NULL,
                        metadata_payload BLOB NOT NULL,
                        metadata_encoding VARCHAR2(16) NOT NULL,
                        PRIMARY KEY (queue_type, queue_name)
);

CREATE TABLE queue_messages (
                                queue_type NUMBER(10) NOT NULL,
                                queue_name VARCHAR2(255) NOT NULL,
                                queue_partition NUMBER(19) NOT NULL,
                                message_id NUMBER(19) NOT NULL,
                                message_payload BLOB NOT NULL,
                                message_encoding VARCHAR2(16) NOT NULL,
                                PRIMARY KEY (queue_type, queue_name, queue_partition, message_id)
);

CREATE TABLE nexus_endpoints (
                                 id RAW(16) NOT NULL,
                                 data BLOB NOT NULL,
                                 data_encoding VARCHAR2(16) NOT NULL,
                                 version NUMBER(19) NOT NULL,
                                 PRIMARY KEY (id)
);

CREATE TABLE nexus_endpoints_partition_status (
                                                  id NUMBER(1) DEFAULT 0 NOT NULL,
                                                  version NUMBER(19) NOT NULL,
                                                  PRIMARY KEY (id),
                                                  CONSTRAINT chk_partition_status_id CHECK (id = 0)
);