--Генерация карт
--Генерация файлов. Генерирует по одному файлу за каждый день прошедшего месяца
declare
card_col number := 100;  --Количество карт
str_col number := 100;  --Количество транзакций в файле
begin
  gen_test_data.gen_cards(card_col);  --сгенерирует card_col*10 карт
  commit;
  gen_test_data.gen_accounts;  --сгенерирует бонусные счета на каждую главную карту
  commit;
  gen_test_data.write_files(str_col, 1);  --сгенерирует 1 файл с количеством транзакций str_col
  commit;
  /*gen_test_data.write_files(str_col);  --сгенерирует по 1 файлу за предыдущий месяц с количеством транзакций str_col
  commit;*/
end;
