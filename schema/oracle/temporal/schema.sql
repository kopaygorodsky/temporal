-- Oracle equivalent schema for Temporal database

-- Namespaces table
CREATE TABLE namespaces(
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

-- Namespace metadata table
CREATE TABLE namespace_metadata (
                                    partition_id NUMBER(10) NOT NULL,
                                    notification_version NUMBER(19) NOT NULL,
                                    PRIMARY KEY(partition_id)
);

-- Insert initial metadata
INSERT INTO namespace_metadata (partition_id, notification_version) VALUES (54321, 1);

-- Shards table
CREATE TABLE shards (
                        shard_id NUMBER(10) NOT NULL,
                        range_id NUMBER(19) NOT NULL,
                        data BLOB NOT NULL,
                        data_encoding VARCHAR2(16) NOT NULL,
                        PRIMARY KEY (shard_id)
);

-- Executions table
CREATE TABLE executions(
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

-- Current executions table
CREATE TABLE current_executions(
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
                                   data BLOB,
                                   data_encoding VARCHAR2(16) DEFAULT '' NOT NULL,
                                   PRIMARY KEY (shard_id, namespace_id, workflow_id)
);

-- Buffered events table
CREATE TABLE buffered_events (
                                 shard_id NUMBER(10) NOT NULL,
                                 namespace_id RAW(16) NOT NULL,
                                 workflow_id VARCHAR2(255) NOT NULL,
                                 run_id RAW(16) NOT NULL,
                                 id NUMBER(19) NOT NULL,
                                 data BLOB NOT NULL,
                                 data_encoding VARCHAR2(16) NOT NULL,
                                 PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, id),
                                 CONSTRAINT buffered_events_id_unique UNIQUE (id)
);

-- Sequence for buffered_events.id
CREATE SEQUENCE buffered_events_seq START WITH 1 INCREMENT BY 1 NOCACHE;

-- Trigger for auto-incrementing buffered_events.id
CREATE OR REPLACE TRIGGER buffered_events_bi
    BEFORE INSERT ON buffered_events
    FOR EACH ROW
BEGIN
    SELECT buffered_events_seq.NEXTVAL INTO :new.id FROM dual;
END;
/

-- Tasks table
CREATE TABLE tasks (
                       range_hash NUMBER(10) NOT NULL,
                       task_queue_id RAW(272) NOT NULL,
                       task_id NUMBER(19) NOT NULL,
                       data BLOB NOT NULL,
                       data_encoding VARCHAR2(16) NOT NULL,
                       PRIMARY KEY (range_hash, task_queue_id, task_id)
);

-- Task queues table
CREATE TABLE task_queues (
                             range_hash NUMBER(10) NOT NULL,
                             task_queue_id RAW(272) NOT NULL,
                             range_id NUMBER(19) NOT NULL,
                             data BLOB NOT NULL,
                             data_encoding VARCHAR2(16) NOT NULL,
                             PRIMARY KEY (range_hash, task_queue_id)
);

-- Task queue user data table
CREATE TABLE task_queue_user_data (
                                      namespace_id RAW(16) NOT NULL,
                                      task_queue_name VARCHAR2(255) NOT NULL,
                                      data BLOB NOT NULL,
                                      data_encoding VARCHAR2(16) NOT NULL,
                                      version NUMBER(19) NOT NULL,
                                      PRIMARY KEY (namespace_id, task_queue_name)
);

-- Build ID to task queue mapping table
CREATE TABLE build_id_to_task_queue (
                                        namespace_id RAW(16) NOT NULL,
                                        build_id VARCHAR2(255) NOT NULL,
                                        task_queue_name VARCHAR2(255) NOT NULL,
                                        PRIMARY KEY (namespace_id, build_id, task_queue_name)
);

-- History immediate tasks table
CREATE TABLE history_immediate_tasks(
                                        shard_id NUMBER(10) NOT NULL,
                                        category_id NUMBER(10) NOT NULL,
                                        task_id NUMBER(19) NOT NULL,
                                        data BLOB NOT NULL,
                                        data_encoding VARCHAR2(16) NOT NULL,
                                        PRIMARY KEY (shard_id, category_id, task_id)
);

-- History scheduled tasks table
CREATE TABLE history_scheduled_tasks (
                                         shard_id NUMBER(10) NOT NULL,
                                         category_id NUMBER(10) NOT NULL,
                                         visibility_timestamp TIMESTAMP(6) NOT NULL,
                                         task_id NUMBER(19) NOT NULL,
                                         data BLOB NOT NULL,
                                         data_encoding VARCHAR2(16) NOT NULL,
                                         PRIMARY KEY (shard_id, category_id, visibility_timestamp, task_id)
);

-- Transfer tasks table
CREATE TABLE transfer_tasks(
                               shard_id NUMBER(10) NOT NULL,
                               task_id NUMBER(19) NOT NULL,
                               data BLOB NOT NULL,
                               data_encoding VARCHAR2(16) NOT NULL,
                               PRIMARY KEY (shard_id, task_id)
);

-- Timer tasks table
CREATE TABLE timer_tasks (
                             shard_id NUMBER(10) NOT NULL,
                             visibility_timestamp TIMESTAMP(6) NOT NULL,
                             task_id NUMBER(19) NOT NULL,
                             data BLOB NOT NULL,
                             data_encoding VARCHAR2(16) NOT NULL,
                             PRIMARY KEY (shard_id, visibility_timestamp, task_id)
);

-- Replication tasks table
CREATE TABLE replication_tasks (
                                   shard_id NUMBER(10) NOT NULL,
                                   task_id NUMBER(19) NOT NULL,
                                   data BLOB NOT NULL,
                                   data_encoding VARCHAR2(16) NOT NULL,
                                   PRIMARY KEY (shard_id, task_id)
);

-- Replication tasks DLQ table
CREATE TABLE replication_tasks_dlq (
                                       source_cluster_name VARCHAR2(255) NOT NULL,
                                       shard_id NUMBER(10) NOT NULL,
                                       task_id NUMBER(19) NOT NULL,
                                       data BLOB NOT NULL,
                                       data_encoding VARCHAR2(16) NOT NULL,
                                       PRIMARY KEY (source_cluster_name, shard_id, task_id)
);

-- Visibility tasks table
CREATE TABLE visibility_tasks(
                                 shard_id NUMBER(10) NOT NULL,
                                 task_id NUMBER(19) NOT NULL,
                                 data BLOB NOT NULL,
                                 data_encoding VARCHAR2(16) NOT NULL,
                                 PRIMARY KEY (shard_id, task_id)
);

-- Activity info maps table
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

-- Timer info maps table
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

-- Child execution info maps table
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

-- Request cancel info maps table
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

-- Signal info maps table
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

-- Signals requested sets table
CREATE TABLE signals_requested_sets (
                                        shard_id NUMBER(10) NOT NULL,
                                        namespace_id RAW(16) NOT NULL,
                                        workflow_id VARCHAR2(255) NOT NULL,
                                        run_id RAW(16) NOT NULL,
                                        signal_id VARCHAR2(255) NOT NULL,
                                        PRIMARY KEY (shard_id, namespace_id, workflow_id, run_id, signal_id)
);

-- History node table
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

-- History tree table
CREATE TABLE history_tree (
                              shard_id NUMBER(10) NOT NULL,
                              tree_id RAW(16) NOT NULL,
                              branch_id RAW(16) NOT NULL,
                              data BLOB NOT NULL,
                              data_encoding VARCHAR2(16) NOT NULL,
                              PRIMARY KEY (shard_id, tree_id, branch_id)
);

-- Queue table
CREATE TABLE queue (
                       queue_type NUMBER(10) NOT NULL,
                       message_id NUMBER(19) NOT NULL,
                       message_payload BLOB NOT NULL,
                       message_encoding VARCHAR2(16) NOT NULL,
                       PRIMARY KEY(queue_type, message_id)
);

-- Queue metadata table
CREATE TABLE queue_metadata (
                                queue_type NUMBER(10) NOT NULL,
                                data BLOB NOT NULL,
                                data_encoding VARCHAR2(16) NOT NULL,
                                version NUMBER(19) NOT NULL,
                                PRIMARY KEY(queue_type)
);

-- Cluster metadata info table
CREATE TABLE cluster_metadata_info (
                                       metadata_partition NUMBER(10) NOT NULL,
                                       cluster_name VARCHAR2(255) NOT NULL,
                                       data BLOB NOT NULL,
                                       data_encoding VARCHAR2(16) NOT NULL,
                                       version NUMBER(19) NOT NULL,
                                       PRIMARY KEY(metadata_partition, cluster_name)
);

-- Cluster membership table
CREATE TABLE cluster_membership (
                                    membership_partition NUMBER(10) NOT NULL,
                                    host_id RAW(16) NOT NULL,
                                    rpc_address VARCHAR2(128) NOT NULL,
                                    rpc_port NUMBER(5) NOT NULL,
                                    role NUMBER(3) NOT NULL,
                                    session_start TIMESTAMP DEFAULT TIMESTAMP '1970-01-02 00:00:01',
                                    last_heartbeat TIMESTAMP DEFAULT TIMESTAMP '1970-01-02 00:00:01',
                                    record_expiry TIMESTAMP DEFAULT TIMESTAMP '1970-01-02 00:00:01',
                                    PRIMARY KEY (membership_partition, host_id)
);

-- Indexes for cluster membership table
CREATE INDEX idx_cluster_membership_role_host ON cluster_membership(role, host_id);
CREATE INDEX idx_cluster_membership_role_heartbeat ON cluster_membership(role, last_heartbeat);
CREATE INDEX idx_cluster_membership_rpc_role ON cluster_membership(rpc_address, role);
CREATE INDEX idx_cluster_membership_last_heartbeat ON cluster_membership(last_heartbeat);
CREATE INDEX idx_cluster_membership_expiry ON cluster_membership(record_expiry);

-- Queues table
CREATE TABLE queues (
                        queue_type NUMBER(10) NOT NULL,
                        queue_name VARCHAR2(255) NOT NULL,
                        metadata_payload BLOB NOT NULL,
                        metadata_encoding VARCHAR2(16) NOT NULL,
                        PRIMARY KEY (queue_type, queue_name)
);

-- Queue messages table
CREATE TABLE queue_messages (
                                queue_type NUMBER(10) NOT NULL,
                                queue_name VARCHAR2(255) NOT NULL,
                                queue_partition NUMBER(19) NOT NULL,
                                message_id NUMBER(19) NOT NULL,
                                message_payload BLOB NOT NULL,
                                message_encoding VARCHAR2(16) NOT NULL,
                                PRIMARY KEY (queue_type, queue_name, queue_partition, message_id)
);

-- Nexus endpoints table
CREATE TABLE nexus_endpoints (
                                 id RAW(16) NOT NULL,
                                 data BLOB NOT NULL,
                                 data_encoding VARCHAR2(16) NOT NULL,
                                 version NUMBER(19) NOT NULL,
                                 PRIMARY KEY (id)
);

-- Nexus endpoints partition status table
CREATE TABLE nexus_endpoints_partition_status (
                                                  id NUMBER(10) DEFAULT 0 NOT NULL,
                                                  version NUMBER(19) NOT NULL,
                                                  PRIMARY KEY (id),
                                                  CONSTRAINT check_id_zero CHECK (id = 0)
);