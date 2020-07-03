create or replace package processing is

  -- Purpose : Обработка успешно проверенных транзакций

procedure process_purchases(p_file_id in number);
procedure process_refunds(p_file_id in number);
procedure end_report_period;
procedure write_response_file (p_file_id in number DEFAULT 0);
procedure check_and_process_files;
--procedure write_monthly_file;
end processing;
/
create or replace package body processing is

v_fetch_limit number := 1000;
v_file_str varchar2(4000);  --строка для записи в таблицу file_data
--коллекция для выборки параметров транзакции необходимых для записи в ответный файл
type transaction_params_rec_type is record   
     ( card_id cards.id%type,
       card_hash cards.card_hash%type,
       cust_transaction_id purchases.cust_trans_id%type,
       cashback_sum number,
       file_id processed_files.r_file_id%type);
type transaction_params_tab_type is table of transaction_params_rec_type;
v_transactions_params transaction_params_tab_type;
-- коллекция для строк файлов
type file_data_rec_type is record (str_num number, str_value file_data.str_value%type);
type file_data_tab_type is table of file_data_rec_type; 
v_file_strings file_data_tab_type := file_data_tab_type();
/* курсор для выборки необработанных входящих файлов*/
cursor cur_unproc_files is
  select f.id from files f where f.file_type = 'IN' and f.file_state = 30;
/*курсор для выборки необработанных транзакций покупки 
по id входящего файла для дальнейшей обработки*/
cursor cur_unproc_purchases (p_file_id in number) is 
    select p.* from purchases p 
    join file_data fd on fd.id = p.file_str_id
    where fd.file_id = p_file_id;
/*курсор для выборки парамеров обработанных транзакций покупки 
по id входящего файла для записи в исходящий файл*/
cursor cur_proc_purchases (p_file_id in number) is  
select c.id, c.card_hash, p.cust_trans_id, pr.cashback_sum, pf.r_file_id  
    from purchases_results pr 
    join purchases p on p.id = pr.purchase_id
    join file_data fd on fd.id = p.file_str_id
    join cards c on pr.card_id = c.id
    join processed_files pf on pf.id = fd.file_id 
    where fd.file_id = p_file_id;
/*курсор для выборки необработанных транзакций возврата 
по id входящего файла для дальнейшей обработки*/
cursor cur_unproc_refunds (p_file_id in number) is 
    select r.* from refunds r 
    join file_data fd on fd.id = r.file_str_id
    where fd.file_id = p_file_id;
/*курсор для выборки парамеров обработанных транзакций возврата 
по id входящего файла для записи в исходящий файл*/
cursor cur_proc_refunds (p_file_id in number) is
select c.id, c.card_hash, r.return_trans_id, rr.cashback_sum, pf.r_file_id 
    from refunds_results rr 
    join refunds r on r.id = rr.refuse_id
    join file_data fd on fd.id = r.file_str_id
    join cards c on rr.card_id = c.id
    join processed_files pf on pf.id = fd.file_id 
    where fd.file_id = p_file_id;

procedure end_report_period is
  /* завершение отчета за прошлый месяц и начало нового на текущий месяц*/
  begin
    update accounts acc 
    set acc.last_report_start = acc.current_report_start,
    acc.current_report_start = trunc (SYSDATE, 'MM'),
    acc.lasr_report_end = acc.current_report_end,
    acc.current_report_end = trunc (last_day(sysdate)),
    acc.last_period_trans_num = acc.curent_period_trans_num,
    acc.curent_period_trans_num = 0,
    acc.last_period_cashback_sum = acc.current_period_cashback_sum,
    acc.current_period_cashback_sum = 0
    where acc.curent_period_trans_num > 0;
  end end_report_period;

function get_main_card_id (p_card_id in number) return number is
  /* нахождение id главной карты. на входе id карты с которой совершена покупка
  на выходе id главной карты */
  v_main_card_id number;
  begin
    select c.main_card_id into v_main_card_id from cards c where c.id = p_card_id;
  return v_main_card_id;
  end;

function get_cashback_percent (p_mcc_id in number, p_merchant_id in number,
  p_trans_date in date) return number
  /* опередедление процента кэшбэка к начислению */
  is
  v_mcc_percent number;
  v_merch_percent number;
  v_std_percent number;
  v_cashback_percent number;
  begin
    --определение стандартного кэшбэка
    select std.cb_perc into v_std_percent from std_amount std;
    --определение % кэшбэка по МСС коду
    begin
      select mc.cashback_percent into v_mcc_percent from mcc_codes mc 
      where mc.id = p_mcc_id and p_trans_date between mc.start_date and mc.end_date;
    exception
      when NO_DATA_FOUND then
        v_mcc_percent := NULL;
    end;
    --определение % кэшбэка по мерчанту
    begin
      select m.amount_percent into v_merch_percent from merchants m
      where m.id = p_merchant_id and p_trans_date between m.start_date and m.end_date;
    exception
      when NO_DATA_FOUND then
        v_merch_percent := NULL;
    end;
    --определение конечного процента кэшбэка
    if v_mcc_percent is null and v_merch_percent is null then  
      v_cashback_percent := v_std_percent; --если не заполнены в таблиах мсс/мерчантов то стандартный процент 
    elsif v_mcc_percent = 0 or v_merch_percent = 0 then
      v_cashback_percent := 0; --если ноль в любой из таблиц мсс/мерчатны то исключение(0%)
    elsif v_mcc_percent > 0 and v_merch_percent > 0 then
      v_cashback_percent := v_merch_percent;  --если у мсс и мерчанта больше 0 то процент мерчанта
    elsif v_mcc_percent is null and v_merch_percent > 0 then
      v_cashback_percent := v_merch_percent;  --если NULL в мсс но больше 0 в мерчанте то процент мерчанта 
    elsif v_merch_percent is null and v_mcc_percent > 0 then
      v_cashback_percent := v_mcc_percent;  --если NULL в мерчанте но больше 0 в мсс то мсс
    end if; 
  return v_cashback_percent / 100;
  end; 

procedure update_account_info (p_card_id in number, p_sum in number, p_trans_date in date) is
  /* обновление информации по суммме и количестве транзакций в аккаунте */
  v_increment number;
  begin 
    if p_sum < 0 then v_increment := 0;
    elsif p_sum = 0 then v_increment := -1;
    else v_increment := 1; 
    end if;
    --dbms_output.put_line('update '||p_sum||';'||p_card_id||';'||p_trans_date||';'||v_increment);
      update accounts acc 
      set acc.curent_period_trans_num = acc.curent_period_trans_num + v_increment,
      acc.current_period_cashback_sum = acc.current_period_cashback_sum + p_sum
      where acc.card_id = p_card_id and 
      p_trans_date between acc.current_report_start and acc.current_report_end;
      update accounts acc 
      set acc.last_period_trans_num = acc.last_period_trans_num + v_increment,
      acc.last_period_cashback_sum = acc.last_period_cashback_sum + p_sum   
      where acc.card_id = p_card_id and
      p_trans_date between acc.last_report_start and acc.lasr_report_end;
    commit;
  end update_account_info;

procedure update_purchases_state (p_file_id in number, p_state in number)
  /* обновление статуса обработки транзакций покупки по id файла */
  is
  begin
    update purchases p set p.purchase_state = p_state 
    where p.file_str_id in (select fd.id from file_data fd where fd.file_id = p_file_id);
  end update_purchases_state;

procedure update_refunds_state (p_file_id in number, p_state in number)
  /* обновление статуса обработки транзакций возврата по id файла */
  is
  begin
    update refunds r set r.refuse_state = p_state 
    where r.file_str_id in (select fd.id from file_data fd where fd.file_id = p_file_id);
  end update_refunds_state;

procedure process_purchases (p_file_id in number) is
  /* обработка транзакций покупки по id входного файла*/
  type purchases_type is table of purchases%rowtype;
  v_purchases purchases_type;
  type account_update_params_rec_type is record (card_id number, t_sum number, t_date date);
  type account_update_params_tab is table of account_update_params_rec_type;
  v_acc_upd_params account_update_params_tab := account_update_params_tab();
  type purc_results_type is table of purchases_results%rowtype;
  v_purc_results purc_results_type := purc_results_type();
  v_cashback_percent number;
  v_purc_res_idx number := 1; 
  begin
    open cur_unproc_purchases(p_file_id);
    loop
      fetch cur_unproc_purchases bulk collect into v_purchases limit v_fetch_limit;
      for i in 1..v_purchases.count
        loop
          v_purc_results.extend;
          v_cashback_percent := get_cashback_percent(v_purchases(i).purchase_mcc,
                                                     v_purchases(i).merchant_id,
                                                     v_purchases(i).purchase_date);
          v_purc_results(v_purc_res_idx).id := seq_purc_result_id.nextval;
          v_purc_results(v_purc_res_idx).card_id := get_main_card_id(v_purchases(i).card_id);
          v_purc_results(v_purc_res_idx).purchase_id := v_purchases(i).id;
          v_purc_results(v_purc_res_idx).cashback_sum := round(
                                                      v_purchases(i).purchase_sum * v_cashback_percent, 0);                                                                                                         
          v_purc_results(v_purc_res_idx).amount_date := systimestamp;
          v_acc_upd_params.extend;
          v_acc_upd_params(v_acc_upd_params.count).card_id := v_purc_results(v_purc_res_idx).card_id;
          v_acc_upd_params(v_acc_upd_params.count).t_sum := v_purc_results(v_purc_res_idx).cashback_sum;
          v_acc_upd_params(v_acc_upd_params.count).t_date := v_purchases(i).purchase_date;
          v_purc_res_idx := v_purc_res_idx + 1;
        end loop;
      forall i in 1..v_purc_results.count
      insert into purchases_results values v_purc_results(i);
    exit when cur_unproc_purchases%NOTFOUND; 
    end loop;
    close cur_unproc_purchases;
    for i in 1..v_acc_upd_params.count
      loop
        update_account_info(v_acc_upd_params(i).card_id, v_acc_upd_params(i).t_sum, v_acc_upd_params(i).t_date);
      end loop;
    update_purchases_state(p_file_id, 1);
    v_purc_results.delete;
  end process_purchases;

function check_refunds_sum (p_purc_trans_id in number) return number is
  /* возвращает разницу сумм покупки и всех возвратов по этой покупке, на входе id транзакции покупки */
  v_purc_sum number;
  v_refunds_sum number;
  begin
    select sum(r.return_sum), p.purchase_sum into v_refunds_sum, v_purc_sum from refunds r
    join purchases p on p.id = r.purchase_trans_id
    where r.purchase_trans_id = p_purc_trans_id
    group by r.return_sum, p.purchase_sum;
  return v_purc_sum - v_refunds_sum;  
  end check_refunds_sum;
  
procedure process_refunds (p_file_id in number)  is
  /* обработка транзакций возврата по id входного файла*/
  type account_update_params_rec_type is record (card_id number, t_sum number, t_date date);
  type account_update_params_tab is table of account_update_params_rec_type;
  v_acc_upd_params account_update_params_tab := account_update_params_tab();
  type refunds_type is table of refunds%rowtype;
  v_refunds refunds_type;
  type ref_results_type is table of refunds_results%rowtype;
  v_ref_results ref_results_type := ref_results_type();
  v_cashback_percent number;
  v_refunds_sum number;
  v_mcc number;
  v_merchant number;
  v_error_message varchar2(4000);
  v_ref_res_idx number := 1;
  begin
    open cur_unproc_refunds(p_file_id);
    loop
      fetch cur_unproc_refunds bulk collect into v_refunds limit v_fetch_limit;
      for i in 1..v_refunds.count
        loop
          v_refunds_sum := check_refunds_sum(v_refunds(i).purchase_trans_id);
          v_ref_results.extend;
          select p.merchant_id, p.purchase_mcc into v_merchant, v_mcc from purchases p 
          where p.id = v_refunds(i).purchase_trans_id;
          v_cashback_percent := get_cashback_percent(v_mcc, v_merchant, v_refunds(i).return_date);
          v_ref_results(v_ref_res_idx).id := seq_refund_result_id.nextval;
          v_ref_results(v_ref_res_idx).card_id := get_main_card_id(v_refunds(i).card_id);
          v_ref_results(v_ref_res_idx).refuse_id := v_refunds(i).id;
          if v_refunds_sum > 0 then 
            v_ref_results(v_ref_res_idx).cashback_sum := round(
                                                      (v_refunds(i).return_sum * v_cashback_percent) * -1, 0);
          elsif v_refunds_sum < 0 then 
            v_ref_results(v_ref_res_idx).cashback_sum := 0;
            v_error_message := validator.get_e_message('SE012', v_refunds(i).file_str_id);
            insert into cashback_errors (id, e_code, e_message) values
            (v_refunds(i).file_str_id, 'SE012', v_error_message);
          end if;                                                    
          v_ref_results(v_ref_res_idx).amount_date := systimestamp;
          v_acc_upd_params.extend;
          v_acc_upd_params(v_acc_upd_params.count).card_id := v_ref_results(v_ref_res_idx).card_id;
          v_acc_upd_params(v_acc_upd_params.count).t_sum := v_ref_results(v_ref_res_idx).cashback_sum;
          v_acc_upd_params(v_acc_upd_params.count).t_date := v_refunds(i).return_date;
          v_ref_res_idx := v_ref_res_idx + 1;          
        end loop;
      forall i in 1..v_ref_results.count
        insert into refunds_results values v_ref_results(i);
    exit when cur_unproc_refunds%NOTFOUND;
    end loop;
    close cur_unproc_refunds;
    for i in 1..v_acc_upd_params.count
      loop
        update_account_info(v_acc_upd_params(i).card_id, v_acc_upd_params(i).t_sum, v_acc_upd_params(i).t_date);
      end loop;
    update_refunds_state(p_file_id, 1);
    commit;
  end process_refunds;

function get_current_cashback_sum (p_card_id in number) return number
  /* на вход получает id карты. возвращает текущую сумму кэшбэка, с учетом количества совершенных транзакций*/
  is 
  v_trans_number number;
  v_cur_cashback_sum number;
  begin
    select acc.curent_period_trans_num, acc.current_period_cashback_sum into v_trans_number, v_cur_cashback_sum 
    from accounts acc where acc.card_id = p_card_id;
    if v_trans_number < 10 then
      v_cur_cashback_sum := 0;
    end if;
  --dbms_output.put_line('current cashback sum = '||v_cur_cashback_sum);
  return v_cur_cashback_sum;   
  exception
    when NO_DATA_FOUND then
      return 0; 
  end get_current_cashback_sum;

procedure write_new_file (p_file_type in varchar2) is
  /* запиь нового файла в таблицу файлов, на входе тип файла 'T' - запись файла с транзакциями
  'S' - итоговый файл за месяц */
  v_file_name files.file_name%type;
  begin
    case p_file_type
      when 'T' then v_file_name := 'out_transactions_'||to_char(sysdate, 'YYYYMMDD');
      when 'S' then v_file_name := 'out_summary_'||to_char(sysdate, 'YYYYMMDD');
    end case;
    insert into files f (id, file_name, file_type, file_date, file_state) values
    (seq_file_id.nextval, v_file_name, 'OUT', sysdate, 30);
  end write_new_file;

function create_header (p_file_id in number default 0) return file_data_tab_type
  /* запись заголовка файла, на входе id входящего файла, 
  если 0 значит формируется заголовок для итогового файла за предыдущий месяц*/
  is
  v_r_file_id processed_files.r_file_id%type;   --идентификатор входного файла в системе отправителя
  v_file_date varchar2(4000) := to_char(sysdate, 'YYYYMMDDHH24MISS');
  v_period varchar2(20) := to_char(add_months(sysdate, -1), 'YYYYMM');
  v_header file_data_tab_type;
  begin
    v_header := file_data_tab_type();
    case 
      when p_file_id = 0 then 
        v_r_file_id := 'MON'||v_period;
        v_file_str := 'H;'||v_r_file_id||';'||v_file_date||';'||v_period;
      when p_file_id > 0 then
        select pf.r_file_id into v_r_file_id from processed_files pf where pf.id = p_file_id;
        v_file_str := 'H;'||'RESP'||v_r_file_id||';'||v_file_date;
    end case;
    v_header.extend;
    v_header(1).str_num := 1;
    v_header(1).str_value := v_file_str;
  return v_header;
  end create_header;

function create_purchases_strings (p_file_id in number, p_str_count in out number) return file_data_tab_type is
  /* формирование строк успешных транзакций покупки на входе id файла, 
  номер строки с которой необходимо начинать запись, на выходе номер последней записанной строки */
  v_cur_cashback_sum number;
  v_purchases file_data_tab_type;
  v_buffer file_data_tab_type;
  begin
    v_purchases := file_data_tab_type();
    open cur_proc_purchases(p_file_id);
      loop
        v_buffer := file_data_tab_type();
        fetch cur_proc_purchases bulk collect into v_transactions_params limit v_fetch_limit;
        for i in 1..v_transactions_params.count
          loop
            p_str_count := p_str_count + 1;
            v_cur_cashback_sum := get_current_cashback_sum(get_main_card_id(v_transactions_params(i).card_id));
            v_file_str := 'S;'||v_transactions_params(i).card_hash||';'
                              ||v_transactions_params(i).cust_transaction_id||';'
                              ||v_transactions_params(i).cashback_sum||';'
                              ||v_cur_cashback_sum;
            v_buffer.extend;
            v_buffer(v_buffer.count).str_num := p_str_count;
            v_buffer(v_buffer.count).str_value := v_file_str;            
          end loop;  
      v_purchases := v_purchases multiset union v_buffer;       
      exit when cur_proc_purchases%NOTFOUND;        
      end loop;      
    close cur_proc_purchases;
    v_transactions_params.delete;
  return v_purchases;           
  end create_purchases_strings;

function get_original_trans_sum (p_trans_id in number) return number
  /* получение суммы транзакции покупки для транзакции возрата на входе id(внутренний) транзакции возврата*/ 
  is
  v_trans_sum number;
  begin
    select p.purchase_sum into v_trans_sum from purchases p 
    join refunds r on r.purchase_trans_id = p.id 
    where r.return_trans_id = p_trans_id;
  return v_trans_sum;
  end get_original_trans_sum;

function create_refunds_strings (p_file_id in number, p_str_count in out number) return file_data_tab_type
  /* формирование строк успешных транзакций возврата на входе id файла, 
  номер строки с которой необходимо начинать запись, на выходе номер последней записанной строки */
  is
  v_cur_cashback_sum number;
  v_refunds file_data_tab_type;
  v_buffer file_data_tab_type;
  begin
    v_refunds := file_data_tab_type();
    open cur_proc_refunds(p_file_id);
      loop
        v_buffer := file_data_tab_type();
        fetch cur_proc_refunds bulk collect into v_transactions_params limit v_fetch_limit;
        for i in 1..v_transactions_params.count
          loop
            p_str_count := p_str_count + 1;
            v_cur_cashback_sum := get_current_cashback_sum(get_main_card_id(v_transactions_params(i).card_id));
            v_file_str := 'S;'||v_transactions_params(i).card_hash||';'
                              ||v_transactions_params(i).cust_transaction_id||';'
                              ||v_transactions_params(i).cashback_sum||';'
                              ||v_cur_cashback_sum;
            v_buffer.extend;
            v_buffer(v_buffer.count).str_num := p_str_count;
            v_buffer(v_buffer.count).str_value := v_file_str;                                  
          end loop;
      v_refunds := v_refunds multiset union v_buffer;
      exit when cur_proc_refunds%NOTFOUND;  
      end loop;   
    close cur_proc_refunds;
    v_transactions_params.delete; 
  return v_refunds;    
  end create_refunds_strings;

function create_error_strings (p_file_id in number, p_str_count in out number) 
  return file_data_tab_type is
  /* формирование строк с ошибками обработки */ 
  v_errors file_data_tab_type;
  begin
    v_errors := file_data_tab_type();
    for rec in 
      (select ce.* from cashback_errors ce
      join file_data fd on fd.id = ce.id
      where fd.file_id = p_file_id )
      loop
        p_str_count := p_str_count + 1;
        v_file_str := 'E;'||rec.e_code||';'||rec.e_message;
        v_errors.extend;
        v_errors(v_errors.count).str_num := p_str_count;
        v_errors(v_errors.count).str_value := v_file_str; 
      end loop;
  return v_errors;
  end create_error_strings;

function create_trailer (p_succ_trans_count in number, p_str_count in out number,
  p_err_trans_count in integer DEFAULT -1) return file_data_tab_type is
  /* формирование концевика файла, на входе количество успешных/неуспешных транзакций и количесто строк
  если  p_err_trans_count = -1 то формируется концевик итогового файла за месяц*/
  v_trailer file_data_tab_type;
  begin
    v_trailer := file_data_tab_type();
    case 
      when p_err_trans_count < 0 then v_file_str := 'T;'||p_succ_trans_count; 
      else
        v_file_str := 'T;'||p_succ_trans_count||';'||p_err_trans_count;
    end case;
    p_str_count := p_str_count + 1;
    v_trailer.extend;
    v_trailer(v_trailer.count).str_num := p_str_count;
    v_trailer(v_trailer.count).str_value := v_file_str;
  return v_trailer;
  end create_trailer;

function create_monthly_file return file_data_tab_type is
  /* формирование итогового файла за месяц */
  v_monhtly_file_strings file_data_tab_type;
  v_buffer file_data_tab_type := file_data_tab_type();
  v_str_num number := 1;
  v_file_string varchar2(4000);
  begin
    v_monhtly_file_strings := create_header();
    for rec in (select * from accounts acc 
      join cards c on c.id = acc.card_id
      where acc.last_report_start = trunc(ADD_MONTHS(SYSDATE, -1), 'MM') and 
      acc.last_period_cashback_sum > 0)
      loop
        v_file_string := 'C;'||rec.card_hash||';'||rec.last_period_cashback_sum;
        v_str_num := v_str_num + 1;
        v_monhtly_file_strings.extend;
        v_monhtly_file_strings(v_str_num).str_num := v_str_num;
        v_monhtly_file_strings(v_str_num).str_value := v_file_string;
      end loop;
    v_buffer := create_trailer(v_str_num - 1, v_str_num);
    v_monhtly_file_strings := v_monhtly_file_strings multiset union v_buffer;
  return v_monhtly_file_strings;
  end create_monthly_file;

function create_rejected_file_strings (p_file_id in number) return file_data_tab_type is
  /* формирование строк отбитого файла */
  v_rejected_file_strings file_data_tab_type := file_data_tab_type();
  v_rejected_file_string varchar2(4000);
  begin
    v_rejected_file_strings := create_header(p_file_id);
    select 'E;'||fec.e_code||';'||fec.text into v_rejected_file_string from processed_files pf 
    join file_error_codes fec on fec.id = pf.f_error_code
    where pf.id = p_file_id;
    v_rejected_file_strings.extend(2);
    v_rejected_file_strings(2).str_num := 2;
    v_rejected_file_strings(2).str_value := v_rejected_file_string;
    v_rejected_file_strings(3).str_num := 3;
    v_rejected_file_strings(3).str_value := 'T;0;1';
  return v_rejected_file_strings;
  end create_rejected_file_strings;

function create_success_file_strings (p_file_id in number) return file_data_tab_type is
   /* формирование строк успешно обработанного файла */
  v_success_file_strings file_data_tab_type := file_data_tab_type();
  v_buffer file_data_tab_type := file_data_tab_type();
  v_str_num number := 1;
  v_succ_trans_num number := 0;
  v_err_trans_num integer := 0;
  begin
    v_success_file_strings := create_header(p_file_id);
    v_str_num := v_success_file_strings.count;
    v_buffer := create_purchases_strings(p_file_id, v_str_num);
    v_success_file_strings := v_success_file_strings multiset union v_buffer;
    v_str_num := v_file_strings.count;
    v_buffer := create_refunds_strings(p_file_id, v_str_num);
    v_success_file_strings := v_success_file_strings multiset union v_buffer;
    v_succ_trans_num := v_success_file_strings.count - 1;
    v_str_num := v_success_file_strings.count;
    v_buffer := create_error_strings(p_file_id, v_str_num);
    v_success_file_strings := v_success_file_strings multiset union v_buffer;
    v_err_trans_num := v_success_file_strings.count - 1 - v_succ_trans_num;
    v_str_num := v_success_file_strings.count;
    v_buffer := create_trailer(v_succ_trans_num, v_str_num, v_err_trans_num);
    v_success_file_strings := v_success_file_strings multiset union v_buffer;
  return v_success_file_strings;
  end create_success_file_strings;

procedure write_response_file (p_file_id in number DEFAULT 0)
  /* запись ответного файла на входной файл, на входе id входящего файла, если 0 то записывается
  итоговый файл за прошедший месяц*/
  is 
  v_file_strings file_data_tab_type;
  v_file_state number;
  v_err_code number;
  begin
    select f.file_state, f.file_error_code into v_file_state, v_err_code from files f where f.id = p_file_id;
    if v_file_state = 31 then
      case 
        when p_file_id = 0 then 
          v_file_strings := create_monthly_file;
          write_new_file('S');
        when p_file_id > 0 then 
          v_file_strings := create_success_file_strings(p_file_id);
          write_new_file('T');
      end case;
    elsif v_file_state = 33 then
      v_file_strings := create_rejected_file_strings(p_file_id);
      write_new_file('T');
    end if;
    forall i in 1..v_file_strings.count
      insert into file_data (id, file_id, str_num, str_value, str_state) values
      (seq_str_id.nextval, seq_file_id.currval, v_file_strings(i).str_num, v_file_strings(i).str_value, 'NEW');
    update files f set f.file_state = 32 where f.id = p_file_id;
    commit;
  end write_response_file;

procedure process_file (p_file_id in number) is
  /* обработка файла по id */
  v_lock VARCHAR2(30);
  v_status NUMBER;
  v_lock_name varchar2(30) := 'lock_proc_file_'||p_file_id;
  begin
    
    dbms_lock.allocate_unique(v_lock_name, v_lock);
    v_status := dbms_lock.request(v_lock, dbms_lock.s_mode);
    parser.write_file_transactions(p_file_id);
    validator.file_validate(p_file_id);
    validator.write_transactions;
    processing.process_purchases(p_file_id);
    processing.process_refunds(p_file_id);
    processing.write_response_file(p_file_id);
    dbms_lock.sleep(5);
    v_status := dbms_lock.release(v_lock);
  exception
    when others then
      v_status := dbms_lock.release(v_lock);
  end process_file;

procedure check_and_process_files is
  /* проверка на наличие необработанных файлов и обработка если имеются
  каждый файл обрабатывается в отдельной сессии */
  v_unproc_file_count number;
  type file_id_tab_type is table of number;
  v_file_id file_id_tab_type;
  v_lock VARCHAR2(30);
  v_status NUMBER;
  v_lock_name varchar2(30);
  begin
    select count(*) into v_unproc_file_count from files f where f.file_type = 'IN' and f.file_state = 30;
    if v_unproc_file_count > 0 then
      open cur_unproc_files;
      loop
        fetch cur_unproc_files bulk collect into v_file_id;
        for i in 1..v_file_id.count
          loop
            v_lock_name := 'lock_proc_file_'||v_file_id(i);
            dbms_scheduler.create_job(
                    job_name => 'processing_file_' || v_file_id(i),
                    job_type => 'PLSQL_BLOCK',
                    job_action => 'processing.check_and_process_files(' || v_file_id(i) || ');',
                    enabled => TRUE,
                    auto_drop => TRUE
                );
          dbms_lock.sleep(1);
          dbms_lock.allocate_unique(v_lock_name, v_lock);
          v_status := dbms_lock.request(v_lock, dbms_lock.x_mode);
          v_status := dbms_lock.release(v_lock);
          end loop;
      exit when cur_unproc_files%NOTFOUND;
      end loop;
      close cur_unproc_files;
    else
      raise_application_error(-20000, 'Nothing to do. All incoming files are processed');
    end if;
  end check_and_process_files;

end processing;
/
