--Таблица ошибок файлов 
CREATE TABLE file_error_codes
       ( id NUMBER NOT NULL,
       e_code VARCHAR2(5),
       text VARCHAR2(50),
       CONSTRAINT pk_f_error_code PRIMARY KEY (id)
       )
       TABLESPACE USERS;
/
--Таблица статусов файлов
CREATE TABLE file_state
       ( id NUMBER NOT NULL,
       state VARCHAR2(250),
       CONSTRAINT pk_f_state_id PRIMARY KEY(id)
       )
       TABLESPACE USERS;
/
--Files table
CREATE TABLE files
       ( id NUMBER NOT NULL, --OWN FILE ID
       file_name VARCHAR2(200) NOT NULL, --FILE NAME
       file_type VARCHAR2(10) NOT NULL, --FILE TYPE (RECIEVE/CASHBACK/RESULT)
       file_date DATE NOT NULL, --RECIEVE/SEND DATE
       file_state NUMBER NOT NULL, --FILE STATUS (NEW/IN PROGRESS/PROCESSED/PARTIALLY PROCESSED)
       file_error_code NUMBER,
       file_proc_date DATE DEFAULT SYSDATE NOT NULL,
       CONSTRAINT pk_file_id PRIMARY KEY(id),
       CONSTRAINT fk_file_e_code FOREIGN KEY(file_error_code)
       REFERENCES file_error_codes(id),
       CONSTRAINT fk_f_state_id FOREIGN KEY(file_state)
       REFERENCES file_state(id)
       )
       TABLESPACE USERS;
/
CREATE SEQUENCE seq_file_id
       MINVALUE 1
       START WITH 1
       INCREMENT BY 1
       NOCACHE;
/
create global temporary table transaction_temp_tab
   ( t_str_id number,
     t_file_id number,
     t_type varchar2(1),
     t_card varchar2(250),
     t_trans_id varchar2(250),
     t_date varchar2(250),
     t_sum varchar2(250),
     t_merchant varchar2(250),
     t_mcc_trans_id varchar2(250),
     t_description varchar2(4000))
     ON COMMIT PRESERVE ROWS;
/
--Таблица обработанных файлов
CREATE TABLE processed_files
       ( id NUMBER NOT NULL,  --ID файла из таблицы файлов
       r_file_id VARCHAR2(40) NOT NULL,  --Уникальный идентификатор файла в системе отправителя
       r_file_date date,  --Дата из файла
       proc_date DATE DEFAULT SYSDATE NOT NULL,  --Дата обработки
       calc_purc_num NUMBER, --Подсчитанное количество покупок
       calc_ref_num NUMBER,  --Подсчитанное количество возвратов
       f_purc_num NUMBER NOT NULL,  --Количество покупок из концевика файла
       f_ref_num NUMBER NOT NULL,  --Количество возвратов из концевика файла
       f_error_code NUMBER, --Код ошибки
       CONSTRAINT pk_proc_file_id PRIMARY KEY(id),
       CONSTRAINT fk_proc_file_id FOREIGN KEY(id)
       REFERENCES files(id),
       CONSTRAINT fk_f_error_code FOREIGN KEY(f_error_code)
       REFERENCES file_error_codes(id)
       )
       TABLESPACE USERS;
/  
create global temporary table processed_files_temp 
ON COMMIT PRESERVE ROWS
as (select * from processed_files where 1 = 0);
/     
--Таблица ошибок строк
CREATE TABLE str_error_codes
       ( id NUMBER NOT NULL,
       e_code VARCHAR2(5),
       text VARCHAR2(50),
       CONSTRAINT pk_str_error_code PRIMARY KEY (id)
       )
       TABLESPACE USERS;
/
--Таблица со строками из файлов
CREATE TABLE file_data
       ( id NUMBER NOT NULL, --FILE ID OWN
       file_id NUMBER NOT NULL, --FILE ID FROM FILES TABLE
       str_num NUMBER NOT NULL, --STRING NUMBER IN FILE
       str_value VARCHAR2(4000) NOT NULL, --STRING VALUE
       str_state VARCHAR2(20) NOT NULL, --STRING STATUS (NEW/PROCESSED/REFUSED)
       str_error_code number,  --STRING REFUSE ERROR CODE 
       CONSTRAINT pk_file_data_id PRIMARY KEY(id),
       CONSTRAINT fk_files_id FOREIGN KEY (file_id)
       REFERENCES files(id),
       CONSTRAINT fk_str_e_code FOREIGN KEY(str_error_code)
       REFERENCES str_error_codes(id)
       )
       TABLESPACE USERS;
/
CREATE INDEX idx_file_data_file_id ON file_data(file_id) TABLESPACE USERS;
/
CREATE SEQUENCE seq_str_id
       MINVALUE 1
       START WITH 1
       INCREMENT BY 1
       CACHE 50;             
/       
--Таблица МСС кодов
CREATE TABLE mcc_codes
       ( id NUMBER NOT NULL,
       mcc VARCHAR2(4) NOT NULL,
       mcc_desc NVARCHAR2(250),
       cashback_percent NUMBER,  --Если NULL значит программы нет, 0-исключение из программы, иначе указанный процент
       start_date DATE,
       end_date DATE,
       CONSTRAINT pk_mcc_code PRIMARY KEY(id)
       )
       TABLESPACE USERS;
/
--Таблица мерчантов из системы заказчика
CREATE TABLE merchants
       ( id NUMBER NOT NULL,
       merchant_name VARCHAR2(50),
       amount_percent NUMBER,  --Если NULL значит программы нет, 0-исключение из программы, иначе указанный процент
       start_date DATE,
       end_date DATE,
       MERCHANT_CATEGORY_ID number not null,
       trans_prefix varchar2(10),
       CONSTRAINT pk_merchant_id PRIMARY KEY(id),
       constraint fk_merch_cat_id foreign key(MERCHANT_CATEGORY_ID) 
       references mcc_codes(id)
       )
       TABLESPACE USERS;
/
CREATE SEQUENCE seq_mcc_merch_id
       MINVALUE 1
       START WITH 1
       INCREMENT BY 1
       CACHE 20;
/  
--Таблица с данными карт карт из системы заказчика
CREATE TABLE cards
       ( id NUMBER NOT NULL, -- Искусственный id карты
         card_hash VARCHAR2(40) NOT NULL, -- Hash номера карты
         is_main VARCHAR2(1), --Главная карта? (да/нет)
         main_card_id NUMBER, --Ссылка на id главной карты в данной таблице
         CONSTRAINT pk_card_id PRIMARY KEY(id),
         CONSTRAINT fk_main_card_id FOREIGN KEY(main_card_id)
         REFERENCES cards(id)
         )
         TABLESPACE USERS;
/
CREATE SEQUENCE seq_card_id
       MINVALUE 1
       START WITH 1
       INCREMENT BY 1
       CACHE 20;
/
--Таблица счетов по главным картам
CREATE TABLE accounts
       ( id number,
       card_id number,
       curent_period_trans_num number,
       current_period_cashback_sum number,
       last_period_trans_num number,       
       last_period_cashback_sum number,
       current_report_start date,
       current_report_end date,
       last_report_start date,
       lasr_report_end date, 
       CONSTRAINT pk_acc_id PRIMARY KEY(id),
       CONSTRAINT fk_cards_card_id FOREIGN KEY(card_id)
       REFERENCES cards(id)
       )
       TABLESPACE USERS;
/
CREATE INDEX idx_accounts_card_id ON accounts(card_id) TABLESPACE USERS;
/
CREATE SEQUENCE seq_acc_id
       MINVALUE 111111
       START WITH 111111
       INCREMENT BY 1
       CACHE 20;
/
--Таблица покупок, содержит информацию о покупках
CREATE TABLE purchases
       ( id NUMBER NOT NULL, --id покупки 
       card_id NUMBER NOT NULL, --id карты из таблицы карт
       cust_trans_id VARCHAR2(20) NOT NULL, --PURCHASE TRANSACTION ID FROM FILE
       purchase_date DATE NOT NULL, --PURCHASE DATE 
       purchase_sum NUMBER NOT NULL, --PURCHASE SUM
       merchant_id NUMBER NOT NULL, --PURCHASE MERCHANT ID
       purchase_mcc NUMBER NOT NULL, --PURCHASE MCC
       purchase_desc VARCHAR2(2000), --PURCHASE DESCRIPTION
       file_str_id NUMBER NOT NULL,
       purchase_state varchar2(1),  --Статус обработки 1-processed, 0-new
       CONSTRAINT pk_purchase_id PRIMARY KEY(id),
       CONSTRAINT fk_files_str_id FOREIGN KEY(file_str_id)
       REFERENCES file_data(id),
       CONSTRAINT fk_purc_card_id FOREIGN KEY(card_id)
       REFERENCES cards(id),
       CONSTRAINT fk_mcc_id FOREIGN KEY(purchase_mcc)
       REFERENCES mcc_codes(id),
       CONSTRAINT fk_merchant_id FOREIGN KEY(merchant_id)
       REFERENCES merchants(id)
       )
       TABLESPACE USERS;
/
CREATE INDEX idx_purchase_state ON purchases(purchase_state) TABLESPACE USERS;
/
CREATE SEQUENCE seq_purchase_id
       MINVALUE 1
       START WITH 1
       INCREMENT BY 1
       CACHE 50;
/
--Таблица возвратов
CREATE TABLE refunds
       ( id NUMBER NOT NULL,
       card_id NUMBER NOT NULL, --CARD ID FROM CARDS TABLE
       return_trans_id VARCHAR2(20) NOT NULL, --RETURN TRANSACTION ID FROM CUSTOMERS SYSTEM
       return_date DATE NOT NULL, --RETURN DATE
       return_sum NUMBER NOT NULL, --RETURN SUM
       merchant_id NUMBER NOT NULL, --RETURN MERCHANT ID
       purchase_trans_id NUMBER NOT NULL, --PURCHASE TRANSACTION ID FROM PURCHASES TABLE
       return_desc VARCHAR2(50) NOT NULL, --RETURN DESCRIPTION
       file_str_id NUMBER NOT NULL, --Номер строки в файле откуда была считан возврат
       refuse_state varchar2(1),   --Статус обработки 1-processed, 0-new
       CONSTRAINT pk_return_id PRIMARY KEY(id),
       CONSTRAINT fk_file_str_num FOREIGN KEY(file_str_id)
       REFERENCES file_data(id),
       CONSTRAINT fk_ret_card_id FOREIGN KEY(card_id)
       REFERENCES cards(id),
       CONSTRAINT fk_purchase_trans_id FOREIGN KEY(purchase_trans_id)
       REFERENCES purchases(id),
       CONSTRAINT fk_ret_merchant_id FOREIGN KEY(merchant_id)
       REFERENCES merchants(id)
       )
       TABLESPACE USERS;
/
CREATE INDEX idx_refund_state on refunds(refuse_state) TABLESPACE USERS;
/
CREATE SEQUENCE seq_refund_id
       MINVALUE 1
       START WITH 1
       INCREMENT BY 1
       CACHE 5;
/             
--Таблица успешных кэшбэков по покупкам 
CREATE TABLE purchases_results
       ( id NUMBER NOT NULL,
       card_id NUMBER NOT NULL,
       purchase_id NUMBER NOT NULL, --id(ВНУТРЕННИЙ) ПОКУПКИ ЗА КОТОРУЮ НАЧИСЛЕН КЭШБЭК
       cashback_sum NUMBER NOT NULL, --СУММА НАЧИСЛЕННОГО ЗА ПОКУПКУ КЭШБЭКА 
       amount_date TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL, --дАТА НАЧИСЛЕНИЯ КЭШБЭКА(ДАТА/ВРЕМЯ ОБРАБОТКИ ПОКУПКИ)
       CONSTRAINT pk_cashback_id PRIMARY KEY(id),
       CONSTRAINT fk_purc_res_card_id FOREIGN KEY(card_id)
       REFERENCES cards(id),
       CONSTRAINT fk_purc_res_purc_id FOREIGN KEY(purchase_id)
       REFERENCES purchases(id)
       )
       TABLESPACE USERS;
/
CREATE SEQUENCE seq_purc_result_id
       MINVALUE 1
       START WITH 1
       INCREMENT BY 1
       CACHE 100;
/
--Таблица успешных кэшбэков(отрицательных) по возратам 
CREATE TABLE refunds_results
       ( id NUMBER NOT NULL,
       card_id NUMBER NOT NULL,
       refuse_id NUMBER NOT NULL, --id(ВНУТРЕННИЙ) ВОЗВРАТА ЗА КОТОРЫЙ НАЧИСЛЕН КЭШБЭК
       cashback_sum NUMBER NOT NULL, --СУММА НАЧИСЛЕННОГО ЗА ВОЗРАТ КЭШБЭКА (ОТРИЦАТЕЛЬНАЯ) 
       amount_date TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL, --дАТА НАЧИСЛЕНИЯ КЭШБЭКА(ДАТА/ВРЕМЯ ОБРАБОТКИ ВОЗВРАТА)
       CONSTRAINT pk_refund_result_id PRIMARY KEY(id),
       CONSTRAINT fk_ref_res_card_id FOREIGN KEY(card_id)
       REFERENCES cards(id),
       CONSTRAINT fk_ref_res_ref_id FOREIGN KEY(refuse_id)
       REFERENCES refunds(id)
       )
       TABLESPACE USERS;
/
CREATE SEQUENCE seq_refund_result_id
       MINVALUE 1
       START WITH 1
       INCREMENT BY 1
       CACHE 100;
/
--Таблица неуспешных кэшбэков по транзакциям
CREATE TABLE cashback_errors
       ( id NUMBER NOT NULL,  --id строки из таблицы file_data
       e_code varchar2(10),
       e_message varchar2(250),
       CONSTRAINT pk_cashback_error_id PRIMARY KEY(id),
       CONSTRAINT fk_cashback_error_id_file_data_id FOREIGN KEY(id)
       REFERENCES file_data(id)
       )
       TABLESPACE USERS;
/

--Финальная таблица для формирования отчета по начислениям в банк выгружается 11 числа месяца за предыдущий месяц
CREATE TABLE cashback_results 
       ( id NUMBER NOT NULL,  --id строки из таблицы file_data
       card_id NUMBER NOT NULL,
       cashbk_sum NUMBER NOT NULL,
       CONSTRAINT pk_cb_result PRIMARY KEY(id),
       CONSTRAINT fk_cb_res_card_id FOREIGN KEY(card_id)
       REFERENCES cards(id),
       CONSTRAINT fk_cashback_result_id_file_data_id FOREIGN KEY(id)
       REFERENCES file_data(id)
       )
       TABLESPACE USERS;                      
/
--Таблица стандартных ставок и параметров начисления cashback
CREATE TABLE std_amount
       ( cb_perc NUMBER,
       min_cb_amount NUMBER,
       max_cb_amount NUMBER,
       min_oper_col NUMBER
       )
       TABLESPACE USERS;        
/
