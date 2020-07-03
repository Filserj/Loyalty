begin
  dbms_scheduler.create_job(job_name => 'check_and_process_new_files', 
                            job_type => 'PLSQL_BLOCK',
                            job_action => 'processing.check_and_process_files',
                            repeat_interval => 'FREQ=DAILY; INTERVAL=1; BYHOUR=1',
                            enabled => TRUE);
  dbms_scheduler.create_job(job_name => 'end_report_period',
                            job_type => 'PLSQL_BLOCK',
                            job_action => 'processing.end_report_period',
                            repeat_interval => 'FREQ=MONTHLY; BYMONTHDAY=1',
                            enabled => TRUE);
  dbms_scheduler.create_job(job_name => 'create_mothly_cashback_report',
                            job_type => 'PLSQL_BLOCK',
                            job_action => 'processing.write_response_file();',
                            repeat_interval => 'FREQ=MONTHLY; BYMONTHDAY=11; BYHOUR=0',
                            enabled => TRUE);
                          
end;
/
