--��������� ����
--��������� ������. ���������� �� ������ ����� �� ������ ���� ���������� ������
declare
card_col number := 100;  --���������� ����
str_col number := 100;  --���������� ���������� � �����
begin
  gen_test_data.gen_cards(card_col);  --����������� card_col*10 ����
  commit;
  gen_test_data.gen_accounts;  --����������� �������� ����� �� ������ ������� �����
  commit;
  gen_test_data.write_files(str_col, 1);  --����������� 1 ���� � ����������� ���������� str_col
  commit;
  /*gen_test_data.write_files(str_col);  --����������� �� 1 ����� �� ���������� ����� � ����������� ���������� str_col
  commit;*/
end;
