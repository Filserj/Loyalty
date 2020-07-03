create or replace package gen_test_data 
is

  -- Author  : SEREGA
  -- Created : 09.04.2020 18:32:20
  -- Purpose : Generate test data for cashback application
procedure gen_cards(p_card_col in number);
--procedure gen_files(p_str_col in number);
procedure gen_accounts;
procedure write_files (p_str_num in number, p_file_num in number default 0);
end gen_test_data;
/
create or replace package body gen_test_data is

type file_string_tab_type is table of file_data%rowtype;

function card_number_gen return varchar2 
  is 
  c_num varchar2(16);
  x varchar2(2):=0;
  s varchar2(3):=0;
begin
  c_num := '985265'; --Íàçíà÷åíèå BIN-à êàğòû
  FOR i IN 1..9  --Ãåíåğàöèÿ ïîñëåäóşùèõ 9 èñìâîëîâ íîìåğà êàğòû
  LOOP
   c_num := c_num || to_char(TRUNC(DBMS_RANDOM.VALUE(0,9)));      
   END LOOP;
    for i in 1..length(c_num)  --Generating control sum of card number (last digit of card number)
    loop
        x:= substr(c_num,-i,1);
        if mod(i,2) != 0 then x:=x*2; if x>9 then x:=x-9; end if; end if;
        s:= s+x;
    end loop;
    s:=10-mod(s,10);
    if s = 10 then s:=0; end if;
    c_num := c_num || s;
    --dbms_output.put_line('luhn= '||s||' card= '||c_num||s);
    return(c_num);
end card_number_gen;

function hash_gen return varchar2 
  is 
  c_hash varchar2(40);
  c_num varchar2(16);
begin
 c_num := card_number_gen();
 c_hash := 'gIMUBPwUUGAQfcB';
for j in 7..length(c_num)
  loop
    --dbms_output.put_line(j);
    if mod(j, 2) = 0
      then c_hash := c_hash || DBMS_RANDOM.STRING('a',2);
        else
          c_hash := c_hash || DBMS_RANDOM.STRING('a',3);
    end if;
  end loop;
  --dbms_output.put_line(c_hash);
  return(c_hash);
end hash_gen;

procedure gen_cards ( p_card_col in number)
  is
  c_hash cards.card_hash%TYPE;
  c_id cards.id%TYPE;
  main_card_id cards.main_card_id%TYPE;
  is_main cards.is_main%TYPE;
begin
  for j in 1..p_card_col
  loop
    for i in 1..10
      loop
        c_id := seq_card_id.NEXTVAL;
        c_hash := hash_gen();
        if i = 1 then 
          main_card_id := c_id;
          is_main := 1;
        elsif i>=2 and i<=6 then
          main_card_id := c_id - i + 1;
          is_main := 0;
        else
          main_card_id := c_id;
          is_main := 1;
        end if;
        --DBMS_OUTPUT.put_line(c_id||' '||c_hash||' '||is_main||' '||main_card_id);
        INSERT INTO cards VALUES (c_id, c_hash, is_main, main_card_id);
        commit;
      end loop;
  end loop;
end gen_cards;

procedure gen_write_new_file (p_date in date) is
  /* çàïèñü íîâîãî ôàéëà â òàáëèöó ôàéëîâîãî îáìåíà */
  file_name varchar2(250);
  begin
    file_name := 'in_transactions_'||to_char(p_date, 'YYYYMMDD');
    --dbms_output.put_line('file id = '||SEQ_FILE_ID.NEXTVAL||' name = '||file_name);
    INSERT INTO FILES ("ID", "FILE_NAME", "FILE_TYPE", "FILE_DATE", "FILE_PROC_DATE", "FILE_STATE") VALUES
    (SEQ_FILE_ID.NEXTVAL, file_name, 'IN', p_date, sysdate,
    (SELECT FS.ID FROM file_state fs WHERE fs.state = 'NEW'));
    commit;
  end gen_write_new_file;

function gen_file_string (p_str_col in number, p_date in date, p_str_type in varchar2,
                          p_purc_col in number default 0, p_refunds_col in number default NULL, 
                          p_orig_trans_id in varchar2 default NULL,
                          p_orig_trans_sum in number default NULL, p_card_hash in varchar2 default NULL, 
                          p_orig_trans_date in date default NULL, p_orig_trans_merch in varchar2 default 0) 
                          return file_data%rowtype is
  /* ãåíåğàöèÿ ñòğîê ôàéëà äëÿ çàïèñè â òàáëèöó ñòğîê ôàéëîâîãî îáìåíà, ãåíåğèğóåò ñòğîêó ôàéëà
  â çàâèñìîñòè îò p_str_type è ïåğåäàííûõ ïàğåìåòğîâ*/                          
  v_transaction file_data%rowtype;
  v_str varchar2(4000);
  v_rand_terminal number := round(dbms_random.value(0, 10), 0);
  v_rand_val number := round(dbms_random.value(0, 1000000), 0); --äëÿ ñóììû è äàòû
  v_date varchar2(20) := to_char(p_date, 'YYYYMMDD');  --äàòà/âğåìÿ 
  v_card_hash varchar2(50);
  v_time varchar2(40) := to_char(p_date + v_rand_val/3600, 'HH24MISS');
  v_ref_sum number := round(dbms_random.value(0, p_orig_trans_sum), 0);
  v_ref_time varchar2(40) := to_char(p_orig_trans_date + v_rand_val/3600, 'HH24MISS');
  v_receipt_remaind number := p_orig_trans_sum - v_ref_sum;
  begin
    v_transaction.id := seq_str_id.nextval;
    v_transaction.file_id := seq_file_id.currval;
    v_transaction.str_num := p_str_col + 1;
    v_transaction.str_state := 'NEW';
    case p_str_type
      when 'H' then 
        v_str := p_str_type||';ONL'||to_char(p_date, 'YYYYMMDD')||'235959;'||to_char(p_date, 'YYYYMMDD')||'235959';
      when 'T' then v_str := p_str_type||';'||p_purc_col||';'||p_refunds_col;
      when 'P' then 
        SELECT c.card_hash into v_card_hash FROM (SELECT c1.card_hash FROM cards c1 
        ORDER BY dbms_random.value) c WHERE rownum = 1;
        select * into v_str  
        from(select p_str_type||';'||v_card_hash||';'||m.trans_prefix||v_date||v_time||';'
        ||v_date||v_time||';'||v_rand_val||';'
        ||m.merchant_name||';'||mcc.mcc||';'||'Êàññà '||p_str_col||' Òåğìèíàë '||v_rand_terminal 
        from merchants m
        join mcc_codes mcc on mcc.id = m.merchant_category_id ORDER BY m.merchant_category_id)
        where rownum = 1;
      when 'R' then 
        select * into v_str  
        from(select p_str_type||';'||p_card_hash||';'||'R'||p_orig_trans_id||';'||v_date||v_ref_time||';'
        ||v_ref_sum||';'||p_orig_trans_merch||';'||p_orig_trans_id||';'||'Îñòàòîê ÷åêà '
        ||v_receipt_remaind  
        from merchants m
        join mcc_codes mcc on mcc.id = m.merchant_category_id ORDER BY dbms_random.value)
        where rownum = 1;
    end case;
    v_transaction.str_value := v_str;  
  return v_transaction;
  end gen_file_string;

function gen_file (p_str_col in number, p_file_date in date) return file_string_tab_type is
  /* ãåíåğàöèÿ ñòğîê ôàéëà */
  v_purc_n number;
  v_refunds_num number;
  v_file_strings file_string_tab_type := file_string_tab_type();
  v_str_col number := 1;
begin
    v_file_strings := file_string_tab_type();
    v_purc_n := 0;
    v_refunds_num := 0;
    v_file_strings.extend;
    v_file_strings(v_file_strings.count) := gen_file_string(0, p_file_date, 'H');
    for i in 1..p_str_col
      loop
         v_file_strings.extend;
         if mod(i,5) = 0 then  --êàæäàÿ ïÿòàÿ òğàíçàêöèÿ â ôàéëå - âîçâğàò íà ïîêóïêó òğåìÿ ñòğîêàìè ğàíåå  
           v_refunds_num := v_refunds_num + 1; 
           v_file_strings(v_file_strings.count) := gen_file_string(i, p_file_date, 'R', 0, 0, 
                       parser.getfield(v_file_strings(v_file_strings.count-3).str_value, 3),
                       parser.getfield(v_file_strings(v_file_strings.count-3).str_value, 5),
                       parser.getfield(v_file_strings(v_file_strings.count-3).str_value, 2), 
       to_date(parser.getfield(v_file_strings(v_file_strings.count-3).str_value, 4), 'YYYYMMDDHH24MISS'),
                       parser.getfield(v_file_strings(v_file_strings.count-3).str_value, 6));
         else
           v_purc_n := v_purc_n + 1;
           v_file_strings(v_file_strings.count) := gen_file_string(i, p_file_date, 'P');
         end if; 
         v_str_col := i + 1;                          
      end loop;
    v_file_strings.extend;
    v_file_strings(v_file_strings.count) := gen_file_string(v_str_col, p_file_date, 'T',
                                                            v_purc_n, v_refunds_num);             
/*  for i in 1..v_file_strings.count
    loop
      dbms_output.put_line(v_file_strings(i).str_num||';'||v_file_strings(i).str_value);
    end loop;*/
  return v_file_strings;
end gen_file;

procedure write_file_strings (p_file_strings in file_string_tab_type) is 
  begin
   /* for i in 1..p_file_strings.count
      loop
        dbms_output.put_line(p_file_strings(i).file_id||';'||p_file_strings(i).str_value);
      end loop;*/
    forall i in 1..p_file_strings.count
      INSERT INTO FILE_DATA VALUES p_file_strings(i);
    COMMIT;
  end write_file_strings;

procedure write_files (p_str_num in number, p_file_num in number default 0) is
  v_file_data file_string_tab_type := file_string_tab_type();
  v_date_s date := trunc(ADD_MONTHS(SYSDATE, -1), 'MM');
  v_date_e date := trunc (SYSDATE, 'MM') -1;
  v_date_step integer := 1;
  begin
    if p_file_num = 0 then
      while v_date_s <= v_date_e
        loop
          gen_write_new_file (v_date_s);
          v_file_data := gen_file(p_str_num, v_date_s);
          v_date_s := v_date_s + v_date_step;
          write_file_strings(v_file_data);
        end loop;
    else 
      for i in 1..p_file_num
        loop
          v_date_s := trunc(ADD_MONTHS(SYSDATE, -1), 'MM') + round(dbms_random.value(0, 28), 0);
          gen_write_new_file (v_date_s);
          v_file_data := gen_file(p_str_num, v_date_s);
          write_file_strings(v_file_data);
        end loop;
    end if;
  end write_files;

procedure gen_accounts is  --Ãåíåğàöèÿ áîíóñíûõ ñ÷åòîâ
  CURSOR cur_main_cards IS
  SELECT C.ID FROM CARDS C WHERE C.IS_MAIN = 1;
  begin
    for card in cur_main_cards
      loop
        insert into accounts (id, card_id, current_report_start, current_report_end, 
        last_report_start, lasr_report_end, curent_period_trans_num, current_period_cashback_sum,
        last_period_trans_num, last_period_cashback_sum) values
        (seq_acc_id.Nextval, card.id, trunc (SYSDATE, 'MM'), trunc (last_day(sysdate)), 
        trunc(ADD_MONTHS(SYSDATE, -1), 'MM'), trunc (SYSDATE, 'MM') -1, 0, 0, 0, 0);
      end loop;
      commit;
  end gen_accounts;

end gen_test_data;
/
