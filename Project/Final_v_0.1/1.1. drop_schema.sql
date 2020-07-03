--JOBS
/
begin
  dbms_scheduler.drop_job(job_name => 'create_mothly_cashback_report');
  dbms_scheduler.drop_job(job_name => 'end_report_period');
  dbms_scheduler.drop_job(job_name => 'check_and_process_new_files');
end;
/
--INDEXES
/
drop index idx_file_data_file_id;
drop index idx_accounts_card_id;
drop index idx_refund_state;
drop index idx_purchase_state;
/
--TABLES
/
DROP TABLE cashback_results;
DROP TABLE purchases_results;
DROP TABLE refunds_results;
DROP TABLE refunds;
DROP TABLE purchases;
drop table accounts;
DROP TABLE cards;
DROP TABLE merchants cascade constraints;
DROP TABLE mcc_codes cascade constraints;
DROP TABLE std_amount;
drop table cashback_errors;
DROP TABLE file_data;
DROP TABLE str_error_codes;
drop table transaction_temp_tab;
DROP TABLE processed_files;
drop table processed_files_temp;
DROP TABLE files;
DROP TABLE file_error_codes;
drop table file_state;
/
--SEQUENCES
/
DROP SEQUENCE seq_purc_result_id;
DROP SEQUENCE seq_refund_id;
DROP SEQUENCE seq_purchase_id;
DROP SEQUENCE seq_card_id;
DROP SEQUENCE seq_mcc_merch_id;
DROP SEQUENCE seq_str_id;
drop sequence seq_file_id;
drop sequence seq_acc_id;
drop sequence seq_refund_result_id;
/
