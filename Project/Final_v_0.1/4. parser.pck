create or replace package parser is
file_rec processed_files%rowtype;
--Чтение файла, запись его во временные таблицы

procedure write_file_transactions (p_file_id in number);
function getfield (p_str in varchar2, p_number_pos in number) return varchar2;

end parser;
/
create or replace package body parser is

cursor cur_file_data (p_file_id in number) is --курсор для выборки всех строк файла
  select fd.* from file_data fd where file_id = p_file_id;
v_string_field_seperator varchar2(1) := ';'; --разделитель полей строки
v_fetch_limit number := 1000; 
type transaction_type_tab is table of transaction_temp_tab%rowtype; --Запись для парсинга в нее транзакций
type string_table_type is table of file_data%rowtype;  --Тип для считывания данных из таблицы строк файлов
v_transactions transaction_type_tab := transaction_type_tab();

FUNCTION DATE_VALIDATE (
   str in varchar2) return varchar2
   --Проверка корректности даты. На входе получает строку, возвращает код ошибки, если SE000 дата корректная.
   is
   v_date date := to_date(str, 'YYYYMMDDHH24MISS'); 
   begin
     IF V_DATE IS NOT NULL THEN RETURN 'SE000';
     ELSIF V_DATE < trunc(ADD_MONTHS(SYSDATE, -1), 'MM') THEN RETURN 'SE010';
     ELSIF V_DATE > SYSDATE THEN RETURN 'SE011';
     ELSE  RETURN 'SE001';
     END IF;
     EXCEPTION
       WHEN OTHERS THEN
         RETURN 'SE001';
   end DATE_VALIDATE;

FUNCTION GET_FIELD_COUNT (p_str IN VARCHAR2, p_separator in varchar2) RETURN NUMBER IS
       --Возвращает количество полей в строке которая подается на вход. Разделитель полей p_separator.       
       v_field_count number := 0;
       v_str_len number := LENGTH(p_str);
       no_sep_in_string exception;
       str_len_is_null exception;
       BEGIN
         if v_str_len > 0 then
           FOR I IN 1..v_str_len
             LOOP
               IF SUBSTR(p_str, I, 1) = p_separator THEN v_field_count := v_field_count + 1; END IF;
             END LOOP;
         else 
           raise str_len_is_null;
         end if; 
         if v_field_count <= 0 then raise no_sep_in_string; end if;
         return v_field_count + 1;
         exception
           when str_len_is_null then
             return null;
           when no_sep_in_string then
             return 1000;
       end GET_FIELD_COUNT;
       
function getfield (p_str in varchar2, p_number_pos in number) return varchar2 is
  /*Функция на вход принимает строку и номер поля которое необходимо взять из строки, возвращает поле*/
  v_pos number;
  v_tail varchar2(250);
  v_field varchar2(250);
  v_number_of_sep number := GET_FIELD_COUNT(p_str, v_string_field_seperator) - 1;
begin
  if p_number_pos = 1 then
    v_pos := instr(p_str, v_string_field_seperator, 1, p_number_pos);
    v_field := substr(p_str, 1, v_pos - 1);
  elsif p_number_pos <= v_number_of_sep then
    v_pos := instr(p_str, v_string_field_seperator, 1, p_number_pos - 1);
    v_tail := substr(p_str, v_pos + 1);
    v_pos := instr(v_tail, v_string_field_seperator, 1);
    v_field := substr(v_tail, 1, v_pos - 1);
  else
    v_pos := instr(p_str, v_string_field_seperator, 1, p_number_pos - 1);
    v_tail := substr(p_str, v_pos + 1);
    v_field := substr(v_tail, 1, v_pos - 1);
  end if;
  return v_field;
end getfield;

function transaction_parse (p_str in varchar2, p_str_id in number, p_file_id in number) 
  return transaction_temp_tab%rowtype 
  --Парсинг транзакции в Record
  is
  rec transaction_temp_tab%rowtype;
  begin
    rec.t_str_id := p_str_id;
    rec.t_file_id := p_file_id;
    rec.t_type := getfield(p_str, 1);
    rec.t_card := getfield(p_str, 2);
    rec.t_trans_id := getfield(p_str, 3);
    rec.t_date := getfield(p_str, 4);
    rec.t_sum := getfield(p_str, 5);
    rec.t_merchant := getfield(p_str, 6);
    rec.t_mcc_trans_id := getfield(p_str, 7);
    rec.t_description := getfield(p_str, 8);
  return rec;
  end transaction_parse;

function read_header (p_str in varchar2) return processed_files%rowtype is 
  /* чтение заголовка файла в record  file_rec*/
  begin
    file_rec.r_file_id := getfield(p_str, 2);
    if date_validate(getfield(p_str, 3)) = 'SE000' then
      file_rec.r_file_date := to_date(getfield(p_str, 3), 'YYYYMMDDHH24MISS');
    else
      file_rec.r_file_date := to_date('19760101000000', 'YYYYMMDDHH24MISS');
    end if;
    file_rec.proc_date := sysdate;
  return file_rec;
  end read_header;

function read_trailer (p_str in varchar2) return processed_files%rowtype is
  /* чтение концевика файла в record file_rec */
  begin
    file_rec.f_purc_num := getfield(p_str, 2);
    file_rec.f_ref_num := getfield(p_str, 3); 
    file_rec.calc_purc_num := 0;
    file_rec.calc_ref_num := 0;
    file_rec.f_error_code := 20;
  return file_rec;
  end read_trailer;

procedure update_file_state (p_file_id in number) is
  /* обновление статуса файла */
  begin
    update files f 
    set 
    f.file_state = 31, --(select fs.id from file_state fs where fs.state = 'IN PROGRESS'),
    f.file_proc_date = sysdate
    where f.id = p_file_id;
    commit;
  end update_file_state;

function read_file (p_file_id in number) return transaction_type_tab is
  /* чтение строк файла в массив */
  file_strings string_table_type;
  v_trans_rec_idx number := 1;
  begin
    file_rec.id := p_file_id;
    open cur_file_data (p_file_id);
    loop
      fetch cur_file_data bulk collect into file_strings limit v_fetch_limit;
      for i in 1..file_strings.count
        loop
          case getfield(file_strings(i).str_value, 1) 
            when 'H' then              
              file_rec := read_header(file_strings(i).str_value);        
            when 'T' then
              file_rec := read_trailer(file_strings(i).str_value);
            else             
              v_transactions.extend;
              v_transactions(v_trans_rec_idx) := transaction_parse(file_strings(i).str_value, file_strings(i).id, file_strings(i).file_id);
              v_trans_rec_idx := v_trans_rec_idx + 1;                        
          end case;
        end loop;
    exit when cur_file_data%NOTFOUND;
    end loop;
    close cur_file_data;
  return v_transactions;
  exception
    when NO_DATA_FOUND then
      raise_application_error(-20000, 'File with ID ['||p_file_id||'] not exists');
  end read_file; 

procedure write_file_transactions (p_file_id in number) is
  /* запись массива строк файла во временную таблицу транзакций */
  begin
    delete from transaction_temp_tab;
    commit;
    v_transactions := read_file(p_file_id);
    forall i in 1..v_transactions.count
      insert into transaction_temp_tab values v_transactions(i);
    update_file_state (p_file_id);
    commit;
  end write_file_transactions;

end parser;
/
