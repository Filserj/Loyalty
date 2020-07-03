create or replace package validator is
  -- Purpose : Проверка валидности файлов и тразакций
  
procedure file_validate (p_file_id in number);
procedure write_transactions;
procedure write_purchases;
procedure write_refunds;
function get_e_message (p_e_code in varchar2, p_str_id in number) return varchar2;

end validator;
/
create or replace package body validator is

type transaction_type_tab is table of transaction_temp_tab%rowtype; --Запись для парсинга в нее транзакций
v_temp_transactions transaction_type_tab;
cursor cur_temp_transactions (p_file_id in number) is  --выборка строк из временной таблицы транзакций
  select * from transaction_temp_tab ttt where ttt.t_file_id = p_file_id;
type error_strings_rec_type is record (str_id number, e_code varchar2(250));
type error_strings_tab is table of error_strings_rec_type;
error_string error_strings_tab;
v_fetch_limit number := 1000;
type cashback_error_type is table of cashback_errors%rowtype;
type transaction_type is table of transaction_temp_tab%rowtype;

function get_e_message (p_e_code in varchar2, p_str_id in number) return varchar2
  is 
  v_message varchar2(4000);
  begin
    select se.text||'"'||fd.str_value||'"' into v_message from str_error_codes se 
    join file_data fd on fd.id = p_str_id
    where se.e_code = p_e_code;
  return v_message;
  end get_e_message; 

function reject_string ( p_str_id in number, p_str_err_code in varchar2 default 'SE007') 
  return  cashback_error_type is
  /*Пометка строки как невалидной в исходной таблице для исключения из дальнейшей обработки */
  v_cashback_error cashback_error_type := cashback_error_type();
  begin
    update file_data fd 
    set 
    fd.str_state = 'REFUSED',
    fd.str_error_code = (select se.id from str_error_codes se where se.e_code = p_str_err_code)
    where fd.id = p_str_id;
    delete from transaction_temp_tab ttt where ttt.t_str_id = p_str_id; 
    v_cashback_error.extend;
    v_cashback_error(v_cashback_error.count).id := p_str_id;
    v_cashback_error(v_cashback_error.count).e_code := p_str_err_code;
    v_cashback_error(v_cashback_error.count).e_message := get_e_message(p_str_err_code, p_str_id);
    commit;
  return v_cashback_error;
  end reject_string;

procedure reject_file (p_file_id in number, p_file_err_code in varchar2 ) is
  /*Пометка файла как невалидного для исключения из дальнейшей обработки */
  v_err_code varchar2(4000);
  begin
    select fec.id into v_err_code from file_error_codes fec where fec.e_code = p_file_err_code;
    update files f
    set
    f.file_state = 33,
    f.file_error_code = v_err_code,
    f.file_proc_date = sysdate
    where f.id = p_file_id;
    update file_data fd
    set 
    fd.str_state = 'REFUSED',
    fd.str_error_code = 17  --(select se.id from str_error_codes se where se.e_code = 'SE007')
    where fd.file_id = p_file_id;
    update processed_files pf set pf.f_error_code = v_err_code where pf.id = p_file_id;
    commit;
  end reject_file;
  
procedure process_string (
  p_str_id in number)
  is 
  begin
    update file_data fd 
    set 
    fd.str_state = 'PROCESSED',
    fd.str_error_code = 10  --(select se.id from str_error_codes se where se.e_code = p_str_err_code)
    where fd.id = p_str_id;
    commit;
  end process_string;

procedure file_validate (p_file_id in number) is
  /* проверка целостности файла, на входе id проверяемого файла */
  v_purc_num number := 0;  --подсчитанное количество покупок
  v_file_purc_num number;  --количество покупок из таблицы processed_files записанное туда во время парсинга
  v_ref_num number := 0;
  v_file_ref_num number;
  v_other_string_num number := 0; --счетчик для неизвестных типов строк
  v_proc_file_count number := 0;
  v_file_date date;
  v_cashback_error cashback_error_type;
  begin
    select count(*) into v_proc_file_count from processed_files pf 
    where pf.r_file_id = parser.file_rec.r_file_id;
    if NOT cur_temp_transactions%ISOPEN then
      open cur_temp_transactions(p_file_id);
    end if;
    loop 
      fetch cur_temp_transactions bulk collect into v_temp_transactions limit v_fetch_limit;
      for i in 1..v_temp_transactions.count
        loop
          case v_temp_transactions(i).t_type
            when 'P' then v_purc_num := v_purc_num + 1;
            when 'R' then v_ref_num := v_ref_num + 1;
            else
              v_other_string_num := v_other_string_num + 1;
              error_string.extend;
              error_string(error_string.count).str_id := v_temp_transactions(i).t_str_id;
              error_string(error_string.count).e_code := 'SE006';
            end case;
        end loop;
    exit when cur_temp_transactions%NOTFOUND;
    end loop;
    close cur_temp_transactions;
    parser.file_rec.calc_purc_num := v_purc_num;
    parser.file_rec.calc_ref_num := v_ref_num;
    
    insert into processed_files values parser.file_rec;
    commit;
    select pf.r_file_date, pf.f_purc_num, pf.f_ref_num
    into v_file_date, v_file_purc_num, v_file_ref_num from processed_files pf where pf.id = p_file_id;
    if v_proc_file_count > 0 then
      reject_file(p_file_id, 'FE004'); --файл уже был обработан
    elsif extract(YEAR FROM v_file_date) = 1976 then
      reject_file(p_file_id, 'FE003');  --некорректный формат даты в заголовке файла
    elsif v_file_purc_num != v_purc_num or v_file_ref_num != v_ref_num then
      reject_file(p_file_id, 'FE002');  --некорректный концевик файла
    elsif v_other_string_num != 0 then
      reject_file(p_file_id, 'FE005');  --некорректный тип строки в файле
      for i in 1..error_string.count
        loop
          v_cashback_error := reject_string(error_string(i).str_id, error_string(i).e_code);
        end loop;
    end if;
  exception 
    when NO_DATA_FOUND then
      raise_application_error(-20000, 'All files was processed. Nothing to do.');
    when DUP_VAL_ON_INDEX then 
      reject_file(p_file_id, 'FE004');
  end file_validate;

FUNCTION card_validate ( P_STR IN VARCHAR2) RETURN VARCHAR2 IS
    v_query number;
    BEGIN
      IF LENGTH(P_STR) = 40 THEN
        begin
          select count(*) into v_query from cards where card_hash = p_str;
          if v_query = 1 then 
            return 'SE000';
          elsif v_query > 1 then 
            raise TOO_MANY_ROWS;
          else 
            return 'SE013';
          end if;
        end;
      ELSE 
        RETURN 'SE002';
      END IF;
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          RETURN 'SE013';
        WHEN TOO_MANY_ROWS THEN
          RETURN 'SE014';
    END card_validate;

function purchase_validate (
       p_purchase in varchar2 ) return varchar2 
       is
       v_query number;
       begin
         --dbms_output.put_line('purc_val params '||p_purchase);
         SELECT COUNT(*) INTO v_query FROM PURCHASES P 
         WHERE P.CUST_TRANS_ID = p_purchase;
         IF v_query != 0 THEN
           RETURN 'SE008';
         ELSE
           RETURN 'SE000';
         END IF;
         EXCEPTION
           WHEN TOO_MANY_ROWS THEN
             RETURN 'SE008'; 
           WHEN NO_DATA_FOUND THEN
             RETURN 'SE000';        
       end purchase_validate;       

function refund_purchase_validate (
       p_purchase in varchar2 ) return varchar2 
       is
       v_query number;
       begin
         SELECT COUNT(*) INTO v_query FROM PURCHASES P 
         WHERE P.CUST_TRANS_ID = p_purchase;
         IF v_query IS NOT NULL THEN
           RETURN 'SE000';
         ELSE
           RETURN 'SE015';
         END IF;
         EXCEPTION
           WHEN TOO_MANY_ROWS THEN
             RETURN 'SE000'; 
           WHEN NO_DATA_FOUND THEN
             RETURN 'SE015';        
       end refund_purchase_validate;       

FUNCTION DATE_VALIDATE ( p_str in varchar2) return varchar2 is
   --Проверка корректности даты. На входе получает строку, возвращает код ошибки, если SE000 дата корректная.
   v_date date; 
   begin
     v_date := to_date(p_str, 'YYYYMMDDHH24MISS');
     IF V_DATE IS NOT NULL THEN 
         RETURN 'SE000';
     ELSIF V_DATE < trunc(ADD_MONTHS(SYSDATE, -1), 'MM') THEN
       RETURN 'SE010';
     ELSIF V_DATE > SYSDATE THEN
       RETURN 'SE011';
     ELSE 
       RETURN 'SE001';
     END IF;
     EXCEPTION
       WHEN OTHERS THEN
         RETURN 'SE001';
   end DATE_VALIDATE;

function merchant_validate (p_in_merch_name in varchar2) return varchar2 is
       v_query number;
       begin
         SELECT COUNT(*) INTO v_query FROM MERCHANTS M WHERE M.MERCHANT_NAME = p_in_merch_name;
         if v_query != 0 then
           return 'SE000';
         else 
           return 'SE005';
         end if;
       EXCEPTION
         WHEN NO_DATA_FOUND THEN
           RETURN 'SE005';         
       end merchant_validate;

function mcc_validate (p_mcc in varchar2) return varchar2 is
       v_query number;
       begin
         SELECT COUNT(*) INTO v_query FROM mcc_codes M WHERE M.MCC = p_mcc;
         IF v_query IS NOT NULL THEN
           RETURN 'SE000';
         ELSE 
           RETURN 'SE004';
         END IF;
         EXCEPTION
           WHEN NO_DATA_FOUND THEN
             RETURN 'SE004';
           WHEN TOO_MANY_ROWS THEN
             RETURN 'SE000';
       end mcc_validate;     

function refund_validate( p_ret_id in varchar2) return varchar2 is
       v_query number;
       begin
         SELECT COUNT(*) INTO v_query FROM refunds r 
         WHERE r.return_trans_id = p_ret_id;
         if v_query = 0 then
           return 'SE000';
         else
           return 'SE009';
         end if;
       exception
         when too_many_rows then
           return 'SE009';
       end refund_validate;         
  
function transaction_validate (p_str in varchar2, p_validate_param in varchar2) return varchar2 is
  /* проверка полей транзакции */
  v_result varchar2(10);
  begin
    case p_validate_param
      when 'card' then v_result := card_validate(p_str);
      when 'purchase' then v_result := purchase_validate(p_str);
      when 'refund' then v_result := refund_validate(p_str);
      when 'purc_ref' then v_result := refund_purchase_validate(p_str);
      when 'date' then v_result := DATE_VALIDATE(p_str);
      when 'merch' then v_result := merchant_validate(p_str);
      when 'mcc' then v_result := mcc_validate(p_str);
    end case;
    return v_result;
  end transaction_validate;


function validate_transactions (p_trans_type in varchar2) return transaction_type is
  /* валидация транзакций, на входе тип проверяемой транзакции, запись транзакций с ошибками в таблицу
  ошибок обработки */
  v_transactions transaction_type := transaction_type();
  v_buffer transaction_type := transaction_type();
  v_cashback_error cashback_error_type := cashback_error_type();
  type param_type_rec is record (str varchar2(250), param varchar2(20));
  type param_tab_type is table of param_type_rec index by pls_integer;
  v_param param_tab_type;
  v_result varchar2(10);
  j number;
  cursor cur_transactions (p_t_type in varchar2) is
  select * from transaction_temp_tab ttt where ttt.t_type = p_t_type;
  begin
    open cur_transactions(p_trans_type);
    loop
      fetch cur_transactions bulk collect into v_buffer limit v_fetch_limit;
      for i in 1..v_buffer.count
        loop
          j := 1;
          v_param(1).str := v_buffer(i).t_card;
          v_param(1).param := 'card';          
          v_param(3).str := v_buffer(i).t_date;
          v_param(3).param := 'date';
          v_param(4).str := v_buffer(i).t_merchant;
          v_param(4).param := 'merch';
          case p_trans_type
            when 'P' then
              v_param(2).str := v_buffer(i).t_trans_id;
              v_param(2).param := 'purchase';   
              v_param(5).str := v_buffer(i).t_mcc_trans_id;
              v_param(5).param := 'mcc';       
            when 'R' then
              v_param(2).str := v_buffer(i).t_trans_id;
              v_param(2).param := 'refund';
              v_param(5).str := v_buffer(i).t_mcc_trans_id;
              v_param(5).param := 'purc_ref';
          end case;           
          loop
            v_result := transaction_validate(v_param(j).str, v_param(j).param);
            j := j + 1;
            exit when v_result != 'SE000' or j > 5;   
          end loop;
          if v_result != 'SE000' then
            v_cashback_error := reject_string(v_buffer(i).t_str_id, v_result);
          else 
            process_string(v_buffer(i).t_str_id);
          end if;
        end loop;
      v_transactions := v_transactions multiset union v_buffer;
    exit when cur_transactions%NOTFOUND;
    end loop;
    if v_cashback_error.count > 0 then
      forall i in 1..v_cashback_error.count
        insert into cashback_errors values v_cashback_error(i);
        commit;
    end if;
    close cur_transactions;
  return v_transactions;
  end validate_transactions;

procedure write_purchases is
  /* запись успешно проверенных транзакций покупки в таблицу с результатами */
  v_purchases transaction_type := transaction_type();
  begin
    v_purchases := validate_transactions('P');
    forall i in 1..v_purchases.count
      insert into purchases 
      (id, card_id, cust_trans_id, purchase_date, purchase_sum, merchant_id,
      purchase_mcc, purchase_desc, file_str_id, purchase_state)    
      values (seq_purchase_id.nextval, 
      (select c.id from cards c where c.card_hash = v_purchases(i).t_card),
      v_purchases(i).t_trans_id, to_date(v_purchases(i).t_date, 'YYYYMMDDHH24MISS'), 
      v_purchases(i).t_sum, 
      (select m.id from merchants m where m.merchant_name = v_purchases(i).t_merchant),
      (select mcc.id from mcc_codes mcc where mcc.mcc = v_purchases(i).t_mcc_trans_id),
      v_purchases(i).t_description, v_purchases(i).t_str_id, 0);
      commit;
  end write_purchases;

procedure write_refunds is
  /* запись успешно проверенных транзакций возврата в таблицу результатов обработки возвратов */
  v_refunds transaction_type;
  begin
    v_refunds := validate_transactions('R');
    forall i in 1..v_refunds.count  
      insert into refunds 
      (id, card_id, return_trans_id, return_date, 
      return_sum, merchant_id, purchase_trans_id, return_desc, file_str_id, refuse_state)
      values (seq_refund_id.nextval, 
      (select c.id from cards c where c.card_hash = v_refunds(i).t_card),
      v_refunds(i).t_trans_id, to_date(v_refunds(i).t_date, 'YYYYMMDDHH24MISS'), 
      v_refunds(i).t_sum,
      (select m.id from merchants m where m.merchant_name = v_refunds(i).t_merchant),
      (select p.id from purchases p where p.cust_trans_id = v_refunds(i).t_mcc_trans_id),
      v_refunds(i).t_description, v_refunds(i).t_str_id, 0);
      commit;
  end write_refunds;

procedure write_transactions is
  /* запись транзакций в соответствующие таблицы */
  begin
    write_purchases;
    commit;
    write_refunds;
    commit;
  end write_transactions;

end validator;
/
