%macro tabulate_household(dname, savename);
ods excel file="/userdata07/room285/data_out/output-household_validation/&savename..xlsx"
	options(sheet_interval='table');
proc tabulate data=&dname;
title "�ֹε�ϰ��������� �Ǻ����� ������ �α�";
class jumin_hh_size nhis_hh_size head_or_dependent;
table (jumin_hh_size='�ֹε�� ��������' all='Total')*(n='n' colpctn='colp' rowpctn='rowp' pctn='p'),
	(nhis_hh_size='' all='Total')*(head_or_dependent='');
quit;

proc tabulate data=&dname;
title "�ֹε�ϰ��������� �Ǻ����� ������ ���ҵ�/����� �α�";
var no_inc no_prop no_inc_no_prop;
class jumin_hh_size nhis_hh_size head_or_dependent;
table (jumin_hh_size='�ֹε�� ��������' all='Total')*(no_inc no_prop no_inc_no_prop)*(n=''),
	(nhis_hh_size='' all='Total')*head_or_dependent='';
quit;

proc tabulate data=&dname;
title "�ֹε�ϰ��������� �Ǻ����� ������ ����Ѽҵ�, ���������ǥ";
var inc_tot prop_txbs_tot;
class jumin_hh_size nhis_hh_size head_or_dependent;
table jumin_hh_size='�ֹε�� ��������'*(inc_tot prop_txbs_tot)*(MEAN=''),
	nhis_hh_size=''*head_or_dependent='';
quit;

proc tabulate data=&dname(where=(inc_tot>0));
title "�ֹε�ϰ��������� �Ǻ����� ������ �ҵ�>0�� ����Ѽҵ�";
var inc_tot;
class jumin_hh_size nhis_hh_size head_or_dependent;
table jumin_hh_size='�ֹε�� ��������'*(inc_tot)*(MEAN=''),
	nhis_hh_size=''*head_or_dependent='';
quit;

proc tabulate data=&dname(where=(prop_txbs_tot>0));
title "�ֹε�ϰ��������� �Ǻ����� ������ ���>0�� ���������ǥ";
var prop_txbs_tot;
class jumin_hh_size nhis_hh_size head_or_dependent;
table jumin_hh_size='�ֹε�� ��������'*(prop_txbs_tot)*(MEAN=''),
	nhis_hh_size=''*head_or_dependent='';
quit;
ods excel close;
%mend;

proc sql;
create table store.seoul_2018_hhval as
select a.INDI_DSCM_NO
	, a.HHRR_HEAD_INDI_DSCM_NO
	, a.inc_tot
	, a.prop_txbs_tot
	, a.cnt_id_hhhi_fd
	, a.gaibja_type
	, a.age
	, case when b.hh_size = 1 then " 1" else "2+" end 
		as jumin_hh_size
	, case when cnt_id_hhhi_fd=1 then " 1" else "2+" end 
		as nhis_hh_size
	, case
		when a.GAIBJA_TYPE='' then ''
		when a.GAIBJA_TYPE="1" or a.GAIBJA_TYPE="5" or a.GAIBJA_TYPE="7" then " ������"
		else "�����" end 
		as head_or_dependent
	, case 
		when a.inc_tot=0 then 1 else . end 
		as no_inc
	, case 
		when a.prop_txbs_tot=0 then 1 else . end 
		as no_prop
from store.seoul as a
left join store.SEOUL_HH as b 
on a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO
where a.STD_YYYY="2018" and b.STD_YYYY="2018";
quit;

proc sql;
alter table store.seoul_2018_hhval
add no_inc_no_prop num;
update store.seoul_2018_hhval
set no_inc_no_prop=(case 
		when no_inc=1 and no_prop=1 then 1 
		else . end);
quit;

%tabulate_household(store.seoul_2018_hhval, jumin_hh_x_nhis_hh);

/* ���� ����: �ֹε��1�ΰ��� �� ������� ������ ������ ����*/
/* 1. 1�� ����� ������ ����ȣ �ľ�*/
PROC SQL;
create table single_dependent_hh as
select HHRR_HEAD_INDI_DSCM_NO, JUNG_NO
from store.SEOUL_HH 
where STD_YYYY="2018"
	and HH_SIZE=1
	and (GAIBJA_TYPE="2" or GAIBJA_TYPE="6" or GAIBJA_TYPE="8");
quit;

/* 2. single_dependent_hh�� ����ȣ���� �ҵ��� ���� ���� �������� ������ID �ľ�
		- Ư�� ����ȣ�� �����ָ� �� ã�´ٸ�, ���￡ ���ٴ� �̾߱�
		- ���� �м����� ���� */
/* 2.1  1�ΰ����̸� ������� ������� �Ǻ������� ���� �ҷ�����*/
proc sql;
create table single_dependent_hh_jung AS
select a.HHRR_HEAD_INDI_DSCM_NO
	, a.JUNG_NO
	, a.inc_tot
from store.seoul as a
inner join work.single_dependent_hh as b
on a.JUNG_NO=b.JUNG_NO
where a.STD_YYYY="2018" and 
	(a.GAIBJA_TYPE="1" or a.GAIBJA_TYPE="5" or a.GAIBJA_TYPE="7");
quit;

/* 2.2  ����ȣ���� �ִ�ҵ� �ľ� */
proc sql;
create table single_dependent_hh_jung_max as
select a.*, b.max_inc_tot
from work.single_dependent_hh_jung as a
left join (
	select JUNG_NO, max(inc_tot) as max_inc_tot
	from work.single_dependent_hh_jung 
	group by JUNG_NO
) as b 
on a.JUNG_NO=b.JUNG_NO;
quit;

/* 2.3  �ִ�ҵ��� ���� �����ָ� �����ַ� ���� */
proc sql;
create table jumin_head_of_nhis_head as
select JUNG_NO
	, max(HHRR_HEAD_INDI_DSCM_NO) as HHRR_HEAD_INDI_DSCM_NO
from single_dependent_hh_jung_max
where inc_tot=max_inc_tot
group by JUNG_NO;
quit;

/* 3. ���� ���� �����Ϳ� ���ο� �������� ���� ID �÷� �����
		- 1�ΰ��� ������� ��� ������ ���� ������ ID �Է�
		- �������� ���� ������ ID �Է�*/
proc sql;
create table seoul_hh_18 as
select a.*
	, case 
		when a.hh_size=1 and (GAIBJA_TYPE="2" or GAIBJA_TYPE="6" or GAIBJA_TYPE="8") then
			b.HHRR_HEAD_INDI_DSCM_NO
		else a.HHRR_HEAD_INDI_DSCM_NO end 
	as new_hh_id
from store.seoul_hh as a
left join jumin_head_of_nhis_head as b
on a.JUNG_NO=b.JUNG_NO
where a.STD_YYYY="2018";
quit;

/* 4. ���ο� ���� ���� ���� ���� ũ�� �ľ�*/
proc sql;
create table seoul_hh_new_18 as
select new_hh_id
	, sum(hh_size) as HH_SIZE
	, sum(INC_TOT) as INC_TOT
	, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
from seoul_hh_18
where new_hh_id
group by new_hh_id;
quit;


/* 5. ���ο� ���� KEY�� ���ο��� �ο�*/
proc sql;
create table seoul_18_new as
select a.*
	, b.new_hh_id
	, case when c.hh_size~=. then c.hh_size else b.hh_size end as HH_SIZE
from store.seoul as a
left join seoul_hh_18 as b
on a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO
left join seoul_hh_new_18 as c
on b.new_hh_id=c.new_hh_id /* ��Ī�� �����ְ� ���ٸ� ������ ���� �� */
where a.STD_YYYY="2018";
QUIT;

/* 6. Cross-tabulation�� �ʿ��� ���� ����*/
proc sql;
create table store.seoul_2018_hhval_new as
select INDI_DSCM_NO
	, HHRR_HEAD_INDI_DSCM_NO
	, inc_tot
	, prop_txbs_tot
	, cnt_id_hhhi_fd
	, gaibja_type
	, age
	, case when hh_size = 1 then " 1" else "2+" end 
		as jumin_hh_size
	, case when cnt_id_hhhi_fd=1 then " 1" else "2+" end 
		as nhis_hh_size
	, case
		when GAIBJA_TYPE='' then ''
		when GAIBJA_TYPE="1" or GAIBJA_TYPE="5" or GAIBJA_TYPE="7" then " ������"
		else "�����" end 
		as head_or_dependent
	, case 
		when inc_tot=0 then 1 else . end 
		as no_inc
	, case 
		when prop_txbs_tot=0 then 1 else . end 
		as no_prop
from seoul_18_new
where new_hh_id~=.;
quit;

proc sql;
alter table store.seoul_2018_hhval_new
add no_inc_no_prop num;
update store.seoul_2018_hhval_new
set no_inc_no_prop=(case 
		when no_inc=1 and no_prop=1 then 1 
		else . end);
quit;

/* 7. Generate cross-tabs */
%tabulate_household(store.seoul_2018_hhval_new, jumin_hh_x_nhis_hh_new);

/*-----------------201130-----up to here*/
