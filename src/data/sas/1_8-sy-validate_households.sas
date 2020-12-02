options symbolgen nosqlremerge;

/* CHANGE OUTPUT FOLDER NAME */
libname OUT '/userdata07/room285/data_out/output-household_validation';
libname STORE '/userdata07/room285/data_out/data_store';

/* GROUP BY NHIS JUNG_NO*/
proc sql;
create table STORE.SEOUL_HH_NHIS as
	select STD_YYYY
		, JUNG_NO
		, count(*) as COUNT
		, max(CNT_ID_HHHI_FD) as HH_SIZE
		, sum(INC_TOT) as INC_TOT
		, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
	from STORE.SEOUL
	group by STD_YYYY, JUNG_NO;
quit;

/* GROUP BY JUMIN NO. (개인 정보 없는 가구주 가구 제외할지?)*/
proc sql;
create table STORE.SEOUL_HH_JUMIN as
	select STD_YYYY
		, HHRR_HEAD_INDI_DSCM_NO
		, count(*) as HH_SIZE
		, sum(INC_TOT) as INC_TOT
		, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
	from STORE.SEOUL
	group by STD_YYYY, HHRR_HEAD_INDI_DSCM_NO;
quit;

/* MAKE BASE DATASET FOR HOUSEHOLD VALIDATION */
proc sql;
create table STORE.SEOUL_HHVAL as
	select a.STD_YYYY
		, a.INDI_DSCM_NO
		, a.HHRR_HEAD_INDI_DSCM_NO
		, a.JUNG_NO
		, a.age
		, a.INC_TOT
		, a.PROP_TXBS_TOT
		, a.GAIBJA_TYPE
		, b.hh_size as hh_size_jumin
		, c.hh_size as hh_size_nhis
	from store.SEOUL as a
	left join store.SEOUL_HH_JUMIN as b
		on a.STD_YYYY=b.STD_YYYY and a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO
	left join store.SEOUL_HH_NHIS as c
		on a.STD_YYYY=c.STD_YYYY and a.JUNG_NO=c.JUNG_NO;
quit;

/* GET HOUSEHOLD HEAD'S GAIBJA_TYPE*/
proc sql;
create table STORE.SEOUL_HHVAL as
	select a.*
		, b.HH_GAIBJA_TYPE_JUMIN
	from STORE.SEOUL_HHVAL as a
	left join (
		select STD_YYYY
			, HHRR_HEAD_INDI_DSCM_NO
			, GAIBJA_TYPE as HH_GAIBJA_TYPE_JUMIN
		from STORE.SEOUL
		where HHRR_HEAD_INDI_DSCM_NO eq INDI_DSCM_NO
	) as b
	on a.STD_YYYY=b.STD_YYYY and a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO;
quit;

/* COMPUTE CATEGORIES FOR CROSS TABULATION */
proc sql;
alter table STORE.SEOUL_HHVAL
	add hh_size_jumin_group1 char(32)
		,hh_size_nhis_group1 char(32)
		, hh_size_jumin_group2 char(32)
		, hh_size_nhis_group2 char(32)
		, gaibja_type_major char(32)
		, head_or_dependent char(32)
		, hh_type_jumin char(32);
	update STORE.SEOUL_HHVAL
	set	hh_size_jumin_group1=(case
			when hh_size_jumin=. then ''
			when hh_size_jumin=1 then "1"
			when hh_size_jumin=2 or hh_size_jumin=3 then "2-3"
			else "4+" end)
		, hh_size_nhis_group1=(case
			when hh_size_nhis=. then ''
			when hh_size_nhis=1 then "1"
			when hh_size_nhis=2 or hh_size_nhis=3 then "2-3"
			else "4+" end)
		, hh_size_jumin_group2=(case
			when hh_size_jumin=. then ''
			when hh_size_jumin=1 then "1"
			else "2+" end)
		, hh_size_nhis_group2=(case
			when hh_size_nhis=. then ''
			when hh_size_nhis=1 then "1"
			else "2+" end)
		, gaibja_type_major=(case 
			when GAIBJA_TYPE="1" or GAIBJA_TYPE="2" then "  지역"
			when GAIBJA_TYPE="5" or GAIBJA_TYPE="6" then " 직장"
			when GAIBJA_TYPE="7" or GAIBJA_TYPE="8" then "의료"
			else '' end)
		, head_or_dependent=(case
			when GAIBJA_TYPE='' then ''
			when GAIBJA_TYPE="1" or GAIBJA_TYPE="5" or GAIBJA_TYPE="7" then " 세대주"
			else "세대원" end)
		, hh_type_jumin=(case
			when hh_size_jumin=1 and age<30 then "   1인 청년"
			when hh_size_jumin=1 and age>=30 and age <55 then "  1인 30-54세"
			when hh_size_jumin=1 and age>=55 then " 1인 55세이상"
			WHEN hh_size_jumin>=2 then "2인 이상"
			else '' end);
quit;
run;

/*---------------------------------------------------------------------------*/
/* 1. 주민등록 가구원수별 건강보험증 세대원수 분포								   */
/*---------------------------------------------------------------------------*/
proc freq data=STORE.SEOUL_HHVAL(where=(STD_YYYY="2018"));
title "1-1. 2018년 서울, 주민등록 가구원수별 건강보험증 세대원수 분포";
tables hh_size_jumin_group1*hh_size_nhis_group1 ;
run;

proc freq data=STORE.SEOUL_HHVAL(where=(STD_YYYY="2018" and age<30));
title "1-2. 2018년 서울 29세 이하, 주민등록 가구원수별 건강보험증 세대원수 분포";
tables hh_size_jumin_group1*hh_size_nhis_group1 ;
run;

proc freq data=STORE.SEOUL_HHVAL(where=(STD_YYYY="2018" and age<30 and inc_tot=0 and prop_txbs_tot=0));
title "1-3. 2018년 서울 29세 이하 무소득 무재산, 주민등록 가구원수별 건강보험증 세대원수 분포";
tables hh_size_jumin_group1*hh_size_nhis_group1 ;
run;

proc freq data=STORE.SEOUL_HHVAL(where=(STD_YYYY="2018" and age>=55));
title "1-4. 2018년 서울 55세 이상, 주민등록 가구원수별 건강보험증 세대원수 분포";
tables hh_size_jumin_group1*hh_size_nhis_group1 ;
run;

proc freq data=STORE.SEOUL_HHVAL(where=(STD_YYYY="2018" and age>=55 and inc_tot=0));
title "1-5. 2018년 서울 55세 이상 무소득, 주민등록 가구원수별 건강보험증 세대원수 분포";
tables hh_size_jumin_group1*hh_size_nhis_group1 ;
run;

/*---------------------------------------------------------------------------*/
/* 2. 주민등록 가구원수별 건강보험증 가입유형별 가입자수						   */
/*---------------------------------------------------------------------------*/
proc tabulate data=STORE.SEOUL_HHVAL(WHERE=(STD_YYYY="2018"));
TITLE "2. 주민등록 가구원수별 건강보험증 가입유형별 가입자수";
CLASS hh_type_jumin gaibja_type_major hh_size_nhis_group2 head_or_dependent;
table hh_type_jumin='주민등록 가구 유형',
	gaibja_type_major=''*hh_size_nhis_group2=''*head_or_dependent=''*(n='n' colpctn='colp' rowpctn='rowp' pctn='p')*F=10.2/RTS=13.;
run;

/*---------------------------------------------------------------------------*/
/* 3. 주민등록가구주의 가입형태별 가구원(세대원)의 가입 형태					*/
/*---------------------------------------------------------------------------*/
proc tabulate data=STORE.SEOUL_HHVAL(WHERE=(STD_YYYY="2018"));
TITLE "3. 주민등록가구주의 가입형태별 가구원(세대원)의 가입 형태 분포";
CLASS GAIBJA_TYPE gaibja_type_major head_or_dependent;
table GAIBJA_TYPE='주민등록가구주의 가입자 유형'*(n='n' colpctn='colp'*f=PERCENT9.2 rowpctn='rowp'*f=PERCENT9.2 pctn='p'*f=PERCENT9.2),
	gaibja_type_major=''*head_or_dependent='';
run;

/* 개인 정보 없는 주민등록가구주 */
/* 개인 정보 없는 건보세대주 */

proc sql;
create table jung_all as
select jung_no
	, GAIBJA_TYPE
	, case
		when GAIBJA_TYPE='' then ''
		when GAIBJA_TYPE="1" or GAIBJA_TYPE="5" or GAIBJA_TYPE="7" then " 세대주"
		else "세대원" end as head_or_dependent
	, cnt_id_hhhi_fd
	, sido 
from store.SEOULPANEL where STD_YYYY="2018";
quit;
proc sql;
create table jung_unique as
select distinct(jung_no) as jung_no
from work.jung_all;
quit;

proc sql;
create table jung_present as
select *
from work.jung_all
where head_or_dependent=" 세대주";
quit;

proc sql;
create table jung_grouped as
select jung_no
	, min(cnt_id_hhhi_fd)
	, max(cnt_id_hhhi_fd)
	, count(*) count
from store.seoul

proc sql;
create table jung_unq as
select a.* 
from work.jung_all as a
where head_or_dependent=" 세대주"
inner join work.jung_unique as b
on a.jung_no=b.jung_no;
quit;

proc sql;
create table NUM_HEADS_PER_DISTINCT_JUNG_NO as
select jung_no
	, count(*) as n
from work.jung_present
group by jung_no;
quit;

proc sql;
title "증번호당 세대주/직장가입자 수 분포";
select n, count(*) as count
from NUM_HEADS_PER_DISTINCT_JUNG_NO group by n;
quit;

proc sql;
title "세대주나 직장가입자 수 (서울시 패널데이터에서)";
select count(*) as n from work.jung_present;
quit;
proc sql;
title "세대주나 직장가입자 수 (서울시 패널데이터에서)";
select count(*) as n from work.jung_unq;
quit;

/*========================*/
proc sql;
create table single_person_hh as
select *
from store.SEOUL_HH where STD_YYYY="2018";
quit;

proc sql;
create table jung_all as
select jung_no
	, HHRR_HEAD_INDI_DSCM_NO
	, GAIBJA_TYPE
	, case
		when GAIBJA_TYPE='' then ''
		when GAIBJA_TYPE="1" or GAIBJA_TYPE="5" or GAIBJA_TYPE="7" then " 세대주"
		else "세대원" end as head_or_dependent
	, cnt_id_hhhi_fd
from store.SEOUL where STD_YYYY="2018";
quit;

proc sql;
create table tmp as
select a.*
	, case when a.hh_size = 1 then " 1" else "2+" end as jumin_hh_size
	, b.head_or_dependent
	, case when b.cnt_id_hhhi_fd=1 then " 1" else "2+" end as nhis_hh_size_type
from single_person_hh as a
left join jung_all as b
on a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO;
quit;

proc tabulate data=tmp;
class jumin_hh_size nhis_hh_size_type head_or_dependent;
table jumin_hh_size='주민등록 가구원수'*(n='n' colpctn='colp' rowpctn='rowp' pctn='p'),
	nhis_hh_size_type=''*head_or_dependent='';
quit;


proc tabulate data=tmp;
var inc_tot prop_txbs_tot;
class jumin_hh_size nhis_hh_size_type head_or_dependent;
table jumin_hh_size='주민등록 가구원수'*(inc_tot prop_txbs_tot)*(mean median),
	nhis_hh_size_type=''*head_or_dependent='';
quit;

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
	, case 
		when no_inc=1 and no_prop=1 then 1 else . end
		as no_inc_no_prop
from store.seoul as a
left join store.SEOUL_HH as b 
on a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO
where a.STD_YYYY="2018" and b.STD_YYYY="2018";
quit;


%macro tabulate_household(dname, savename);
ods excel file="/userdata07/room285/data_out/output-household_validation/&savename.xlsx"
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

%tabulate_household(store.seoul_2018_hhval, jumin_hh_x_nhis_hh);

/* 가구 재편: 주민등록1인가구 중 세대원은 세대주 가구로 편입*/
proc sql;
create table jumin_hh_1_dependent as
select *
from store.seoul_2018_hhval
where jumin_hh_size=" 1" and head_or_dependent="세대원";
quit;

proc sql;
create table rest as
select *
from store.seoul_2018_hhval
where jumin_hh_size="2+" or head_or_dependent=" 세대주";
quit;

proc sql;
create table hhs as
select HHRR_HEAD_INDI_DSCM_NO
		, max(jumin_hh_size) as jumin_hh_size
		, sum(INC_TOT) as INC_TOT
		, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
		, case 
			when sum(case when head_or_dependent=" 세대주" then 1 
				else 0 end) >= 1 then 1
			else . end as nhis_head_present 
		, CNT_ID_HHHI_FD
from store.seoul_2018_hhval
group by HHRR_HEAD_INDI_DSCM_NO;
quit;

proc sql;
select nmiss(nhis_head_present)
from hhs;
quit;

proc sql;
	create table WORK.TMP_HH_DEM as
	select HHRR_HEAD_INDI_DSCM_NO
		, SEX_TYPE
		, age
		, JUNG_NO
	from STORE.seoul_2018_hhval
	where HHRR_HEAD_INDI_DSCM_NO eq INDI_DSCM_NO;
quit;

proc sql;
	create table hhs as
	select a.*
		, b.SEX_TYPE
		, b.age
		, b.JUNG_NO
	from WORK.HHS as a
	left join WORK.TMP_HH_DEM as b
	on a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO;
quit;

/* 주민등록 가구 데이터 준비 완료.
이제 증번호대로 묶기?*/

proc sql;
create table store.SEOUL_HH_NEW as /*after figuring out missing household heads, change this to work.&region._HH2*/
select JUNG_NO
	, sum(jumin_hh_size) as hh_size
	, sum(INC_TOT) as INC_TOT
	, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
	, max(CNT_ID_HHHI_FD) as CNT_ID_HHHI_FD
	, case when sum(nhis_head_present)>0 then 1 else . end as nhis_head_present
from hhs
group by JUNG_NO;
quit;

/* 증번호 -> unique 세대주?*/
proc sql;
create table NUM_HEADS_PER_DISTINCT_JUNG_NO as
select jung_no
	, count(*) as n
from STORE.seoul_2018_hhval
where head_or_dependent=" 세대주"
group by jung_no;
quit;
proc sql;
title "증번호수";
select count(distinct(jung_no))
from STORE.seoul_2018_hhval;
quit;
proc sql;
title "증번호당 세대주/직장가입자 수 분포";
select n, count(*) as count
from NUM_HEADS_PER_DISTINCT_JUNG_NO group by n;
quit;

/* new household */

proc sql;
create table jung_grouped as /*after figuring out missing household heads, change this to work.&region._HH2*/
select JUNG_NO
	, count(*) as jung_hh_size
	, sum(INC_TOT) as INC_TOT
	, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
	, max(CNT_ID_HHHI_FD) as CNT_ID_HHHI_FD
/*	, case when sum(case when head_or_dependent=1 then 1 else . end)>0 then 1 else . end as nhis_head_present*/
from store.seoul_2018_hhval
group by JUNG_NO;
quit;

proc sql;
select jung_hh_size, count(*) as count
from work.jung_grouped group by jung_hh_size;
select CNT_ID_HHHI_FD, count(*) as count
from work.jung_grouped group by CNT_ID_HHHI_FD;
quit;



/*check beloiw from here*/
proc sql;
create table jung_head as
select jung_no
	, max(INDI_DSCM_NO) as INDI_DSCM_NO
from store.seoul_2018_hhval
where head_or_dependent=" 세대주"
group by jung_no;
quit;

proc sql;
alter table jung_grouped add INDI_DSCM_NO char ;
update jung_grouped as a
	set INDI_DSCM_NO=(
		select b.INDI_DSCM_NO 
		from jung_head
		where a.jung_no=b.jung_no
);
quit;
/* check up to here */

/*proc sql;*/
/*create table new_hh_id as*/
/*select INDI_DSCM_NO*/
/*	, HHRR_HEAD_INDI_DSCM_NO*/
/*	, JUNG_NO*/


proc sql;
select case 
	when hh_size>15 then "16+" 
	when hh_size>10 <=15 then "11-15"
	when hh_size<=10 then put(hh_size, 6.)
	else "unkown" end as hh_size_grouped, count(*) as count
	, mean(inc_tot/hh_size) as inc_tot_per_person
from store.seoul_hh group by hh_size_grouped;
quit;
