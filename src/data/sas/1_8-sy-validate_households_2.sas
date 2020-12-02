%macro tabulate_household(dname, savename);
ods excel file="/userdata07/room285/data_out/output-household_validation/&savename..xlsx"
	options(sheet_interval='table');
proc tabulate data=&dname;
title "주민등록가구원수별 건보세대 유형별 인구";
class jumin_hh_size nhis_hh_size head_or_dependent;
table (jumin_hh_size='주민등록 가구원수' all='Total')*(n='n' colpctn='colp' rowpctn='rowp' pctn='p'),
	(nhis_hh_size='' all='Total')*(head_or_dependent='');
quit;

proc tabulate data=&dname;
title "주민등록가구원수별 건보세대 유형별 무소득/무재산 인구";
var no_inc no_prop no_inc_no_prop;
class jumin_hh_size nhis_hh_size head_or_dependent;
table (jumin_hh_size='주민등록 가구원수' all='Total')*(no_inc no_prop no_inc_no_prop)*(n=''),
	(nhis_hh_size='' all='Total')*head_or_dependent='';
quit;

proc tabulate data=&dname;
title "주민등록가구원수별 건보세대 유형별 평균총소득, 평균총재산과표";
var inc_tot prop_txbs_tot;
class jumin_hh_size nhis_hh_size head_or_dependent;
table jumin_hh_size='주민등록 가구원수'*(inc_tot prop_txbs_tot)*(MEAN=''),
	nhis_hh_size=''*head_or_dependent='';
quit;

proc tabulate data=&dname(where=(inc_tot>0));
title "주민등록가구원수별 건보세대 유형별 소득>0의 평균총소득";
var inc_tot;
class jumin_hh_size nhis_hh_size head_or_dependent;
table jumin_hh_size='주민등록 가구원수'*(inc_tot)*(MEAN=''),
	nhis_hh_size=''*head_or_dependent='';
quit;

proc tabulate data=&dname(where=(prop_txbs_tot>0));
title "주민등록가구원수별 건보세대 유형별 재산>0의 평균총재산과표";
var prop_txbs_tot;
class jumin_hh_size nhis_hh_size head_or_dependent;
table jumin_hh_size='주민등록 가구원수'*(prop_txbs_tot)*(MEAN=''),
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
		when a.GAIBJA_TYPE="1" or a.GAIBJA_TYPE="5" or a.GAIBJA_TYPE="7" then " 세대주"
		else "세대원" end 
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

/* 가구 재편: 주민등록1인가구 중 세대원은 세대주 가구로 편입*/
/* 1. 1인 세대원 가구의 증번호 파악*/
PROC SQL;
create table single_dependent_hh as
select HHRR_HEAD_INDI_DSCM_NO, JUNG_NO
from store.SEOUL_HH 
where STD_YYYY="2018"
	and HH_SIZE=1
	and (GAIBJA_TYPE="2" or GAIBJA_TYPE="6" or GAIBJA_TYPE="8");
quit;

/* 2. single_dependent_hh의 증번호마다 소득이 제일 높은 세대주의 가구주ID 파악
		- 특정 증번호의 세대주를 못 찾는다면, 서울에 없다는 이야기
		- 따라서 분석에서 제외 */
/* 2.1  1인가구이며 세대원인 사람들의 건보세대주 전부 불러오기*/
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

/* 2.2  증번호마다 최대소득 파악 */
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

/* 2.3  최대소득을 가진 세대주를 가구주로 결정 */
proc sql;
create table jumin_head_of_nhis_head as
select JUNG_NO
	, max(HHRR_HEAD_INDI_DSCM_NO) as HHRR_HEAD_INDI_DSCM_NO
from single_dependent_hh_jung_max
where inc_tot=max_inc_tot
group by JUNG_NO;
quit;

/* 3. 기존 가구 데이터에 새로운 가구개념 기준 ID 컬럼 만들기
		- 1인가구 세대원인 경우 위에서 구한 가구주 ID 입력
		- 나머지는 기존 가구주 ID 입력*/
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

/* 4. 새로운 가구 개념 기준 가구 크기 파악*/
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


/* 5. 새로운 가구 KEY를 개인에게 부여*/
proc sql;
create table seoul_18_new as
select a.*
	, b.new_hh_id
	, case when c.hh_size~=. then c.hh_size else b.hh_size end as HH_SIZE
from store.seoul as a
left join seoul_hh_18 as b
on a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO
left join seoul_hh_new_18 as c
on b.new_hh_id=c.new_hh_id /* 매칭된 세대주가 없다면 빈값으로 남을 것 */
where a.STD_YYYY="2018";
QUIT;

/* 6. Cross-tabulation에 필요한 변수 생성*/
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
		when GAIBJA_TYPE="1" or GAIBJA_TYPE="5" or GAIBJA_TYPE="7" then " 세대주"
		else "세대원" end 
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
