CREATE TABLE executions_visibility (namespace_id CHAR(64) NOT NULL, run_id CHAR(64) NOT NULL, _version NUMBER(19) DEFAULT 0 NOT NULL, start_time TIMESTAMP(6) NOT NULL, execution_time TIMESTAMP(6) NOT NULL, workflow_id VARCHAR2(255) NOT NULL, workflow_type_name VARCHAR2(255) NOT NULL, status NUMBER(10) NOT NULL, close_time TIMESTAMP(6) NULL, history_length NUMBER(19) NULL, history_size_bytes NUMBER(19) NULL, execution_duration NUMBER(19) NULL, state_transition_count NUMBER(19) NULL, memo BLOB NULL, encoding VARCHAR2(64) NOT NULL, task_queue VARCHAR2(255) DEFAULT '' NOT NULL, search_attributes CLOB NULL, parent_workflow_id VARCHAR2(255) NULL, parent_run_id VARCHAR2(255) NULL, root_workflow_id VARCHAR2(255) DEFAULT '' NOT NULL, root_run_id VARCHAR2(255) DEFAULT '' NOT NULL, TemporalChangeVersion CLOB GENERATED ALWAYS AS (search_attributes) VIRTUAL, BinaryChecksums CLOB GENERATED ALWAYS AS (search_attributes) VIRTUAL, BatcherUser VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.BatcherUser' RETURNING VARCHAR2(255))) VIRTUAL, TemporalScheduledStartTime TIMESTAMP(6) GENERATED ALWAYS AS (TO_TIMESTAMP_TZ(JSON_VALUE(search_attributes, '$.TemporalScheduledStartTime' RETURNING VARCHAR2(255)), 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM')) VIRTUAL, TemporalScheduledById VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.TemporalScheduledById' RETURNING VARCHAR2(255))) VIRTUAL, TemporalSchedulePaused NUMBER(1) GENERATED ALWAYS AS (CASE JSON_VALUE(search_attributes, '$.TemporalSchedulePaused' RETURNING VARCHAR2(5)) WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE NULL END) VIRTUAL, TemporalNamespaceDivision VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.TemporalNamespaceDivision' RETURNING VARCHAR2(255))) VIRTUAL, BuildIds CLOB GENERATED ALWAYS AS (search_attributes) VIRTUAL, TemporalPauseInfo CLOB GENERATED ALWAYS AS (search_attributes) VIRTUAL, TemporalWorkerDeploymentVersion VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.TemporalWorkerDeploymentVersion' RETURNING VARCHAR2(255))) VIRTUAL, TemporalWorkflowVersioningBehavior VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.TemporalWorkflowVersioningBehavior' RETURNING VARCHAR2(255))) VIRTUAL, TemporalWorkerDeployment VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.TemporalWorkerDeployment' RETURNING VARCHAR2(255))) VIRTUAL, PRIMARY KEY (namespace_id, run_id));
CREATE OR REPLACE FUNCTION get_close_time_or_max(close_time IN TIMESTAMP) RETURN TIMESTAMP IS BEGIN RETURN COALESCE(close_time, TIMESTAMP '9999-12-31 23:59:59'); END;
CREATE INDEX default_idx ON executions_visibility (namespace_id, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_execution_time ON executions_visibility (namespace_id, execution_time, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_id ON executions_visibility (namespace_id, workflow_id, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_workflow_type ON executions_visibility (namespace_id, workflow_type_name, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_status ON executions_visibility (namespace_id, status, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_history_length ON executions_visibility (namespace_id, history_length, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_history_size_bytes ON executions_visibility (namespace_id, history_size_bytes, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_execution_duration ON executions_visibility (namespace_id, execution_duration, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_state_transition_count ON executions_visibility (namespace_id, state_transition_count, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_task_queue ON executions_visibility (namespace_id, task_queue, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_parent_workflow_id ON executions_visibility (namespace_id, parent_workflow_id, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_parent_run_id ON executions_visibility (namespace_id, parent_run_id, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_root_workflow_id ON executions_visibility (namespace_id, root_workflow_id, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_root_run_id ON executions_visibility (namespace_id, root_run_id, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_batcher_user ON executions_visibility (namespace_id, BatcherUser, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_start_time ON executions_visibility (namespace_id, TemporalScheduledStartTime, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_scheduled_by_id ON executions_visibility (namespace_id, TemporalScheduledById, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_schedule_paused ON executions_visibility (namespace_id, TemporalSchedulePaused, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_namespace_division ON executions_visibility (namespace_id, TemporalNamespaceDivision, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_worker_deployment_version ON executions_visibility (namespace_id, TemporalWorkerDeploymentVersion, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_workflow_versioning_behavior ON executions_visibility (namespace_id, TemporalWorkflowVersioningBehavior, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_worker_deployment ON executions_visibility (namespace_id, TemporalWorkerDeployment, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_change_version ON executions_visibility (namespace_id, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_binary_checksums ON executions_visibility (namespace_id, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_build_ids ON executions_visibility (namespace_id, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE INDEX by_temporal_pause_info ON executions_visibility (namespace_id, get_close_time_or_max(close_time) DESC, start_time DESC, run_id);
CREATE TABLE custom_search_attributes (namespace_id CHAR(64) NOT NULL, run_id CHAR(64) NOT NULL, _version NUMBER(19) DEFAULT 0 NOT NULL, search_attributes CLOB NULL, Bool01 NUMBER(1) GENERATED ALWAYS AS (CASE JSON_VALUE(search_attributes, '$.Bool01' RETURNING VARCHAR2(5)) WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE NULL END) VIRTUAL, Bool02 NUMBER(1) GENERATED ALWAYS AS (CASE JSON_VALUE(search_attributes, '$.Bool02' RETURNING VARCHAR2(5)) WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE NULL END) VIRTUAL, Bool03 NUMBER(1) GENERATED ALWAYS AS (CASE JSON_VALUE(search_attributes, '$.Bool03' RETURNING VARCHAR2(5)) WHEN 'true' THEN 1 WHEN 'false' THEN 0 ELSE NULL END) VIRTUAL, Datetime01 TIMESTAMP(6) GENERATED ALWAYS AS (TO_TIMESTAMP_TZ(JSON_VALUE(search_attributes, '$.Datetime01' RETURNING VARCHAR2(255)), 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM')) VIRTUAL, Datetime02 TIMESTAMP(6) GENERATED ALWAYS AS (TO_TIMESTAMP_TZ(JSON_VALUE(search_attributes, '$.Datetime02' RETURNING VARCHAR2(255)), 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM')) VIRTUAL, Datetime03 TIMESTAMP(6) GENERATED ALWAYS AS (TO_TIMESTAMP_TZ(JSON_VALUE(search_attributes, '$.Datetime03' RETURNING VARCHAR2(255)), 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM')) VIRTUAL, Double01 NUMBER(20, 5) GENERATED ALWAYS AS (TO_NUMBER(JSON_VALUE(search_attributes, '$.Double01' RETURNING VARCHAR2(30)))) VIRTUAL, Double02 NUMBER(20, 5) GENERATED ALWAYS AS (TO_NUMBER(JSON_VALUE(search_attributes, '$.Double02' RETURNING VARCHAR2(30)))) VIRTUAL, Double03 NUMBER(20, 5) GENERATED ALWAYS AS (TO_NUMBER(JSON_VALUE(search_attributes, '$.Double03' RETURNING VARCHAR2(30)))) VIRTUAL, Int01 NUMBER(19) GENERATED ALWAYS AS (TO_NUMBER(JSON_VALUE(search_attributes, '$.Int01' RETURNING VARCHAR2(20)))) VIRTUAL, Int02 NUMBER(19) GENERATED ALWAYS AS (TO_NUMBER(JSON_VALUE(search_attributes, '$.Int02' RETURNING VARCHAR2(20)))) VIRTUAL, Int03 NUMBER(19) GENERATED ALWAYS AS (TO_NUMBER(JSON_VALUE(search_attributes, '$.Int03' RETURNING VARCHAR2(20)))) VIRTUAL, Keyword01 VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Keyword01' RETURNING VARCHAR2(255))) VIRTUAL, Keyword02 VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Keyword02' RETURNING VARCHAR2(255))) VIRTUAL, Keyword03 VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Keyword03' RETURNING VARCHAR2(255))) VIRTUAL, Keyword04 VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Keyword04' RETURNING VARCHAR2(255))) VIRTUAL, Keyword05 VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Keyword05' RETURNING VARCHAR2(255))) VIRTUAL, Keyword06 VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Keyword06' RETURNING VARCHAR2(255))) VIRTUAL, Keyword07 VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Keyword07' RETURNING VARCHAR2(255))) VIRTUAL, Keyword08 VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Keyword08' RETURNING VARCHAR2(255))) VIRTUAL, Keyword09 VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Keyword09' RETURNING VARCHAR2(255))) VIRTUAL, Keyword10 VARCHAR2(255) GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Keyword10' RETURNING VARCHAR2(255))) VIRTUAL, Text01 CLOB GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Text01' RETURNING CLOB)) STORED, Text02 CLOB GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Text02' RETURNING CLOB)) STORED, Text03 CLOB GENERATED ALWAYS AS (JSON_VALUE(search_attributes, '$.Text03' RETURNING CLOB)) STORED, KeywordList01 CLOB GENERATED ALWAYS AS (search_attributes) VIRTUAL, KeywordList02 CLOB GENERATED ALWAYS AS (search_attributes) VIRTUAL, KeywordList03 CLOB GENERATED ALWAYS AS (search_attributes) VIRTUAL, PRIMARY KEY (namespace_id, run_id));
CREATE INDEX by_bool_01 ON custom_search_attributes (namespace_id, Bool01);
CREATE INDEX by_bool_02 ON custom_search_attributes (namespace_id, Bool02);
CREATE INDEX by_bool_03 ON custom_search_attributes (namespace_id, Bool03);
CREATE INDEX by_datetime_01 ON custom_search_attributes (namespace_id, Datetime01);
CREATE INDEX by_datetime_02 ON custom_search_attributes (namespace_id, Datetime02);
CREATE INDEX by_datetime_03 ON custom_search_attributes (namespace_id, Datetime03);
CREATE INDEX by_double_01 ON custom_search_attributes (namespace_id, Double01);
CREATE INDEX by_double_02 ON custom_search_attributes (namespace_id, Double02);
CREATE INDEX by_double_03 ON custom_search_attributes (namespace_id, Double03);
CREATE INDEX by_int_01 ON custom_search_attributes (namespace_id, Int01);
CREATE INDEX by_int_02 ON custom_search_attributes (namespace_id, Int02);
CREATE INDEX by_int_03 ON custom_search_attributes (namespace_id, Int03);
CREATE INDEX by_keyword_01 ON custom_search_attributes (namespace_id, Keyword01);
CREATE INDEX by_keyword_02 ON custom_search_attributes (namespace_id, Keyword02);
CREATE INDEX by_keyword_03 ON custom_search_attributes (namespace_id, Keyword03);
CREATE INDEX by_keyword_04 ON custom_search_attributes (namespace_id, Keyword04);
CREATE INDEX by_keyword_05 ON custom_search_attributes (namespace_id, Keyword05);
CREATE INDEX by_keyword_06 ON custom_search_attributes (namespace_id, Keyword06);
CREATE INDEX by_keyword_07 ON custom_search_attributes (namespace_id, Keyword07);
CREATE INDEX by_keyword_08 ON custom_search_attributes (namespace_id, Keyword08);
CREATE INDEX by_keyword_09 ON custom_search_attributes (namespace_id, Keyword09);
CREATE INDEX by_keyword_10 ON custom_search_attributes (namespace_id, Keyword10);
BEGIN CTX_DDL.CREATE_PREFERENCE('TEXT_DATASTORE', 'DIRECT_DATASTORE'); CTX_DDL.CREATE_PREFERENCE('TEXT_LEXER', 'BASIC_LEXER'); END;
CREATE INDEX by_text_01 ON custom_search_attributes (Text01) INDEXTYPE IS CTXSYS.CONTEXT PARAMETERS ('DATASTORE TEXT_DATASTORE LEXER TEXT_LEXER');
CREATE INDEX by_text_02 ON custom_search_attributes (Text02) INDEXTYPE IS CTXSYS.CONTEXT PARAMETERS ('DATASTORE TEXT_DATASTORE LEXER TEXT_LEXER');
CREATE INDEX by_text_03 ON custom_search_attributes (Text03) INDEXTYPE IS CTXSYS.CONTEXT PARAMETERS ('DATASTORE TEXT_DATASTORE LEXER TEXT_LEXER');