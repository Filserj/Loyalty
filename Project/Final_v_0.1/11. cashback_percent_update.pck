create or replace package cashback_percent_update is

  -- Author  : SEREGA
  -- Created : 08.05.2020 15:55:49
  -- Purpose : Установка параметров начисления кэшбэка

procedure set_mcc_percent 
  (p_mcc in number, 
  p_percent in number,
  p_start_date in varchar2,
  p_end_date in varchar2);

procedure set_merchant_percent
  (p_merchant in varchar2,
  p_percent in number,
  p_start_date in varchar2,
  p_end_date in varchar2);
  
procedure set_std_amount_params
  (p_min_oper_col in number,
  p_min_cashback_amount in number,
  p_max_cashback_amount in number,
  p_std_cashback_percent in number);


end cashback_percent_update;
/
create or replace package body cashback_percent_update is

incorrect_date exception;

function check_date (p_str_date in varchar2) return date is
  v_date date;
  begin
     v_date := to_date(p_str_date, 'YYYY-MM-DD');
  exception
    when others then
      raise incorrect_date;
  return v_date;
  end check_date;

procedure set_mcc_percent (p_mcc in number, p_percent in number, p_start_date in varchar2, p_end_date in varchar2) is
  v_start_date date;
  v_end_date date;
  begin
    begin
      v_start_date := check_date(p_start_date);
     exception
       when incorrect_date then 
         raise_application_error(-20001, 'Дата начала периода ['||p_start_date||'] введена не корректно. Введите корректную дату в формате ГГГГММДД');
    end;
    begin
      v_end_date := to_date(p_end_date, 'YYYY-MM-DD');
    exception
      when incorrect_date then
        raise_application_error(-20001, 'Дата окончания периода ['||p_end_date||'] введена не корректно. Введите корректную дату в формате ГГГГММДД');
    end;    
    update mcc_codes mc 
    set mc.cashback_percent = p_percent, mc.start_date = v_start_date, mc.end_date = v_end_date
    where mc.mcc = p_mcc;
    if SQL%ROWCOUNT = 0 then
      raise_application_error(-20000, 'MCC = ['||p_mcc||'] не найден. Введите корректный МСС');
      rollback;
    else 
      commit;
    end if;
  end set_mcc_percent;

procedure set_merchant_percent (p_merchant in varchar2, p_percent in number, p_start_date in varchar2, p_end_date in varchar2) is 
  v_start_date date;
  v_end_date date;
  begin
    begin
      v_start_date := check_date(p_start_date);
     exception
       when incorrect_date then 
         raise_application_error(-20001, 'Дата начала периода ['||p_start_date||'] введена некорректно. Введите корректную дату в формате ГГГГММДД');
    end;
    begin
      v_end_date := to_date(p_end_date, 'YYYY-MM-DD');
    exception
      when incorrect_date then
        raise_application_error(-20001, 'Дата окончания периода ['||p_end_date||'] введена некорректно. Введите корректную дату в формате ГГГГММДД');
    end;
  update merchants m 
  set m.amount_percent = p_percent, m.start_date = v_start_date, m.end_date = v_end_date
  where m.merchant_name = p_merchant;
  if SQL%ROWCOUNT = 0 then
    raise_application_error(-20001, 'Мерчант ['||p_merchant||'] не найден. Введите корректное наименование мерчанта');
    rollback;
  else
    commit;
  end if;
  end set_merchant_percent;
  
procedure set_std_amount_params (p_min_oper_col in number, p_min_cashback_amount in number, 
                                 p_max_cashback_amount in number, p_std_cashback_percent in number) is 
  begin
    update std_amount sa
    set sa.cb_perc = p_std_cashback_percent, sa.min_cb_amount = p_min_cashback_amount, 
      sa.min_oper_col = p_min_oper_col, sa.max_cb_amount = p_max_cashback_amount;
    commit;
  end set_std_amount_params;
  
end cashback_percent_update;
/
