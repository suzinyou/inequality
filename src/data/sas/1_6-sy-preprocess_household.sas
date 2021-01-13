/*OPTIONS NONOTES NOSOURCE NODATE NOCENTER LABEL NONUMBER LS=200 PS=MAX;*/
options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname STORE '/userdata07/room285/data_out/data_store';

/* 1. Generate household level data -----------------------------------------*/
/* 1.1 Household income data + equivalized income (total, wage, business) ---*/
%macro generate_hh_dataset(region);
%if &region=SEOUL or &region=seoul %then
	%do;
		%let extra_reg_col=, sigungu;
	%end;
%else
	%do;
		%let extra_reg_col=;
	%end;

proc sql;
	create table WORK.TMP_HH as
	select STD_YYYY
		, HHRR_HEAD_INDI_DSCM_NO
		, count(*) as HH_SIZE
		, sum(INC_TOT) as INC_TOT
		, sum(INC_WAGE) as INC_WAGE
		, sum(INC_BUS) as INC_BUS
		, sum(INC_INT) as INC_INT
		, sum(INC_DIVID) as INC_DIVID
		, sum(INC_FIN) as INC_FIN
		, sum(INC_OTHR) as INC_OTHR
		, sum(INC_PNSN_NATL) as INC_PNSN_NATL
		, sum(INC_PNSN_OCCUP) as INC_PNSN_OCCUP
		, sum(INC_PNSN) as INC_PNSN
		, sum(INC_MAIN) as INC_MAIN
		, sum(PROP_TXBS_HS) as PROP_TXBS_HS
		, sum(PROP_TXBS_LND) as PROP_TXBS_LND
		, sum(PROP_TXBS_BLDG) as PROP_TXBS_BLDG
		, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
		, sum(PROP_TXBS_SHIP) as PROP_TXBS_SHIP
		, case when sum(
			case when INC_WAGE+INC_BUS > 0 then 1 else 0 end
		) >= 1 then 1 else . end 
		as is_working_any
	from STORE.&region
	group by STD_YYYY, HHRR_HEAD_INDI_DSCM_NO;
quit;

proc sql;
	create table WORK.TMP_HH_DEM as
	select STD_YYYY
		, HHRR_HEAD_INDI_DSCM_NO
		, SEX_TYPE
		, age
		, JUNG_NO
		, GAIBJA_TYPE
		, case 
			when GAIBJA_TYPE in ("1", "2") then "지역" 
			when GAIBJA_TYPE in ("5", "6") then "직장" 
			when GAIBJA_TYPE in ("7", "8") then "의료"
			else '' end 
		as gaibja_type_major
		, sido
		&extra_reg_col
		, case when CMPR_DSB_GRADE='' 
			or CMPR_DSB_GRADE="00" then . else 1 end as has_disability
		, case when inc_wage+inc_bus > 0 then 1 else . end as is_working_head
	from STORE.&region
	where HHRR_HEAD_INDI_DSCM_NO eq INDI_DSCM_NO;
quit;

proc sql;
	create table STORE.&region._HH as
	select a.*
		, b.SEX_TYPE
		, b.age
		, b.JUNG_NO
		, b.GAIBJA_TYPE
		, b.gaibja_type_major
		, b.sido
		&extra_reg_col
		, b.has_disability
		, b.is_working_head
		, case
			when a.hh_size=. then ''
			when a.hh_size < 5 then put(hh_size, 2.)
			when a.hh_size >= 5 then "5+"
			else '' end
			as hh_size_group
		, case
			when b.age=. then ''
			when b.age < 20 then "0-19"
			when b.age >= 20 and age < 30 then "20-29"
			when b.age >= 30 and age < 40 then "30-39"
			when b.age >= 40 and age < 50 then "40-49"
			when b.age >= 50 and age < 60 then "50-59"
			when b.age >= 60 and age < 70 then "60-69"
			when b.age >= 70 then "70+"
			else '' end
			as age_group 
	from WORK.TMP_HH as a
	inner join WORK.TMP_HH_DEM as b
	/* inner join to drop any HH Head IDs that don't exist as individual IDs */
	on a.STD_YYYY=b.STD_YYYY
		and a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO;
quit;
%mend generate_hh_dataset;


%macro revise_household(region);
/* 가구 재편: 주민등록1인가구 중 세대원은 세대주 가구로 편입*/
/* 1. 1인 세대원 가구의 증번호 파악*/
PROC SQL;
create table single_dependent_hh as
select STD_YYYY, HHRR_HEAD_INDI_DSCM_NO, JUNG_NO
from store.&region._HH 
where HH_SIZE=1
	and (GAIBJA_TYPE="2" or GAIBJA_TYPE="6" or GAIBJA_TYPE="8");
quit;

/* 2. single_dependent_hh의 증번호마다 소득이 제일 높은 세대주의 가구주ID 파악
		- 특정 증번호의 세대주를 못 찾는다면, 서울에 없다는 이야기
		- 따라서 분석에서 제외 */
/* 2.1  1인가구이며 세대원인 사람들의 건보세대주 전부 불러오기 (+그 세대주의 가구주)*/
proc sql;
create table single_dependent_hh_jung AS
select a.STD_YYYY
	, a.HHRR_HEAD_INDI_DSCM_NO
	, a.JUNG_NO
	, a.inc_tot
from store.&region as a
inner join work.single_dependent_hh as b
on a.STD_YYYY=b.STD_YYYY and a.JUNG_NO=b.JUNG_NO
where a.GAIBJA_TYPE="1" or a.GAIBJA_TYPE="5" or a.GAIBJA_TYPE="7";
quit;

/* 2.2  증번호마다 (세대주가 여럿일 경우 있으므로) 최대소득 파악 */
proc sql;
create table single_dependent_hh_jung_max as
select a.*, b.max_inc_tot
from work.single_dependent_hh_jung as a
left join (
	select STD_YYYY, JUNG_NO, max(inc_tot) as max_inc_tot
	from work.single_dependent_hh_jung 
	group by STD_YYYY, JUNG_NO
) as b 
on a.STD_YYYY=b.STD_YYYY and a.JUNG_NO=b.JUNG_NO;
quit;

/* 2.3  최대소득을 가진 세대주의 가구주를 새 가구주로 결정(증번호|-->새 가구주id) */
proc sql;
create table jumin_head_of_nhis_head as
select STD_YYYY, JUNG_NO
	, max(HHRR_HEAD_INDI_DSCM_NO) as HHRR_HEAD_INDI_DSCM_NO
from single_dependent_hh_jung_max
where inc_tot=max_inc_tot
group by STD_YYYY, JUNG_NO;
quit;

/* 3. 기존 가구 데이터에 새로운 가구개념 기준 ID 컬럼 만들기
		- IF 1인가구 세대원: 위에서 구한 가구주 ID 입력
		- ELSE 기존 가구주 ID 입력*/
proc sql;
create table &region._hh as
select a.*
	, case 
		when a.hh_size=1 and (GAIBJA_TYPE="2" or GAIBJA_TYPE="6" or GAIBJA_TYPE="8") then
			b.HHRR_HEAD_INDI_DSCM_NO
		else a.HHRR_HEAD_INDI_DSCM_NO end 
	as new_hh_id
from store.&region._hh as a
left join jumin_head_of_nhis_head as b
on a.STD_YYYY=b.STD_YYYY and a.JUNG_NO=b.JUNG_NO;

/* 기존 주민등록가구ID |--> 재편된 가구 ID map table 저장*/
create table store.&region._hh2_map as
select STD_YYYY, HHRR_HEAD_INDI_DSCM_NO, new_hh_id
from &region._hh;
quit;

/* 4. 새로운 가구 개념 기준 가구원수, 가구 소득 등 파악*/
proc sql;
create table &region._hh_new as
select STD_YYYY
	, new_hh_id
	, sum(hh_size) as HH_SIZE
	, sum(INC_TOT) as INC_TOT
	, sum(INC_WAGE) as INC_WAGE
	, sum(INC_BUS) as INC_BUS
	, sum(INC_INT) as INC_INT
	, sum(INC_DIVID) as INC_DIVID
	, sum(INC_FIN) as INC_FIN
	, sum(INC_OTHR) as INC_OTHR
	, sum(INC_PNSN_NATL) as INC_PNSN_NATL
	, sum(INC_PNSN_OCCUP) as INC_PNSN_OCCUP
	, sum(INC_PNSN) as INC_PNSN
	, sum(INC_MAIN) as INC_MAIN
	, sum(PROP_TXBS_HS) as PROP_TXBS_HS
	, sum(PROP_TXBS_LND) as PROP_TXBS_LND
	, sum(PROP_TXBS_BLDG) as PROP_TXBS_BLDG
	, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
	, sum(PROP_TXBS_SHIP) as PROP_TXBS_SHIP
	, case when sum(case when INC_WAGE+INC_BUS > 0 then 1 else 0 end)>=1 then 1 else . end as is_working_any
from &region._hh
where new_hh_id
group by STD_YYYY, new_hh_id;
quit;

/* 5. 가구주 개인 정보 파악 (가구구주 개인정보가 있는 경우만 남을 것)*/
proc sql;
create table work.tmp_hh_dem as
select a.STD_YYYY
	, b.new_hh_id
	, a.SEX_TYPE
	, a.age
	, a.JUNG_NO
	, a.GAIBJA_TYPE
	, case 
		when a.GAIBJA_TYPE in ("1", "2") then "지역" 
		when a.GAIBJA_TYPE in ("5", "6") then "직장" 
		when a.GAIBJA_TYPE in ("7", "8") then "의료"
		else '' end 
	as gaibja_type_major
	, a.sido
	, a.sigungu
	, case when a.CMPR_DSB_GRADE='' 
		or a.CMPR_DSB_GRADE="00" then . else 1 end as has_disability
	, case when a.inc_wage+a.inc_bus > 0 then 1 else . end as is_working_head
from store.&region as a
left join &region._HH as b
on a.STD_YYYY=b.STD_YYYY and a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO
where b.new_hh_id eq a.INDI_DSCM_NO;
quit;

proc sql;
	create table STORE.&region._HH2 as
	select a.STD_YYYY
		, a.new_hh_id as HHRR_HEAD_INDI_DSCM_NO
		, a.HH_SIZE
		, a.INC_TOT
		, a.INC_WAGE
		, a.INC_BUS
		, a.INC_INT
		, a.INC_DIVID
		, a.INC_FIN
		, a.INC_OTHR
		, a.INC_PNSN_NATL
		, a.INC_PNSN_OCCUP
		, a.INC_PNSN
		, a.INC_MAIN
		, a.PROP_TXBS_HS
		, a.PROP_TXBS_LND
		, a.PROP_TXBS_BLDG
		, a.PROP_TXBS_TOT
		, a.PROP_TXBS_SHIP
		, b.SEX_TYPE
		, b.age
		, b.GAIBJA_TYPE
		, b.gaibja_type_major
		, b.sido
		, b.sigungu
		, b.has_disability
		, b.is_working_head
		, a.is_working_any
		, case
			when a.hh_size=. then ''
			when a.hh_size < 5 then put(hh_size, 2.)
			when a.hh_size >= 5 then "5+"
			else '' end
			as hh_size_group
		, case
			when b.age=. then ''
			when b.age < 20 then "0-19"
			when b.age >= 20 and age < 30 then "20-29"
			when b.age >= 30 and age < 40 then "30-39"
			when b.age >= 40 and age < 50 then "40-49"
			when b.age >= 50 and age < 60 then "50-59"
			when b.age >= 60 and age < 70 then "60-69"
			when b.age >= 70 then "70+"
			else '' end
			as age_group
	from WORK.&region._hh_new as a
	inner join WORK.TMP_HH_DEM as b
	/* inner join to drop any HH Head IDs that don't exist as individual IDs */
	on a.STD_YYYY=b.STD_YYYY and a.new_hh_id=b.new_hh_id
	where a.HH_SIZE <= 10;
quit;
%mend;

/*%generate_hh_dataset(KR);*/
/*%generate_hh_dataset(SEOUL);*/
/*%generate_hh_dataset(SEOULPANEL);*/

/*%revise_household(SEOULPANEL);*/
%revise_household(SEOUL);

/* Filter out households that have more than 10 ppl per household */
/*data STORE.SEOUL_HH1;*/
/*set STORE.SEOUL_HH;*/
/*where hh_size <= 10;*/
/*run;*/
/*data STORE.KR_HH1;*/
/*set STORE.KR_HH;*/
/*where hh_size <= 10;*/
/*run;*/

/* 2. CREATE EQUIVALIZED INCOME/TAXBASE TABLE ---------------------------*/
%macro generate_eq_dataset(region, hhtype);
%if &region=SEOUL or &region=seoul or &region=SEOULPANEL or &region=seoulpanel %then %do;
	%let extra_reg_col=, b.sigungu;
	%end;
%else %do;
	%let extra_reg_col=;
	%end;

%let outsuffix=%sysfunc(tranwrd(%sysfunc(tranwrd(&hhtype,hh,eq)),HH,EQ));

%let dname=&region;
%if &hhtype=HH1 or &hhtype=hh1 %then %do;
	%let id=HHRR_HEAD_INDI_DSCM_NO;	
	%end;
%else %if &hhtype=HH2 or &hhtype=hh2 %then %do;
	%let id=new_hh_id;
	proc sql;
	create table store.&region.2 as
	select a.*, b.new_hh_id
	from store.&region as a
	left join store.&region._HH2_MAP as b
	on a.STD_YYYY=b.STD_YYYY 
		and a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO;
	quit;
	%let dname=&region.2;
	%end;
%else %do;
	%put "Unknown hhtype &hhtype.";
	%abort cancel;
	%end;

proc sql;
create table store.&region._&outsuffix as
select a.STD_YYYY
	, a.INDI_DSCM_NO
	, a.&id
	, b.hh_size
	, b.SEX_TYPE
	, a.age as indi_age
	, a.gaibja_type as indi_gaibja_type
	, a.inc_tot as indi_inc_tot
	, a.prop_txbs_tot as indi_prop_txbs_tot
	, a.cnt_id_hhhi_fd
	, b.age
	, b.GAIBJA_TYPE
	, b.gaibja_type_major
	, b.sido
	&extra_reg_col
	, b.has_disability
	, b.is_working_head
	, b.is_working_any
	, b.hh_size_group
	, b.age_group
	, b.INC_TOT/sqrt(b.HH_SIZE) as INC_TOT
	, b.INC_BUS/sqrt(b.HH_SIZE) as INC_BUS
	, b.INC_WAGE/sqrt(b.HH_SIZE) as INC_WAGE
	, b.INC_FIN/sqrt(b.HH_SIZE) as INC_FIN
	, b.INC_PNSN/sqrt(b.HH_SIZE) as INC_PNSN
	, b.INC_OTHR/sqrt(b.HH_SIZE) as INC_OTHR
	, b.INC_MAIN/sqrt(b.HH_SIZE) as INC_MAIN
	, b.PROP_TXBS_TOT/sqrt(b.HH_SIZE) as PROP_TXBS_TOT
	, b.PROP_TXBS_HS/sqrt(b.HH_SIZE) as PROP_TXBS_HS
	, b.PROP_TXBS_LND/sqrt(b.HH_SIZE) as PROP_TXBS_LND
	, b.PROP_TXBS_BLDG/sqrt(b.HH_SIZE) as PROP_TXBS_BLDG
from STORE.&dname as a
inner join STORE.&region._&hhtype as b
on a.STD_YYYY = b.STD_YYYY
	and a.&id = b.HHRR_HEAD_INDI_DSCM_NO;
quit;
%mend;
/*%generate_eq_dataset(SEOUL, HH1);*/
/*%generate_eq_dataset(KR, HH1);*/

/* Sanity check */
/*proc sql;*/
/*select count(*) as count, nmiss(new_hh_id) as missing_hh*/
/*from store.SEOULPANEL2 where STD_YYYY="2018";quit; */

/*%generate_eq_dataset(SEOULPANEL, HH2);*/
%generate_eq_dataset(SEOUL, HH2);

/* Seoul household 1% sample -----------------------------------*/
/*%macro sample_1percent(dname, outname);*/
/*proc surveyselect data=&dname*/
/*	method=SRS*/
/*	seed=170011*/
/*	rate=0.01*/
/*	out=&outname;*/
/*STRATA STD_YYYY;*/
/*run;*/
/*%mend sample_1percent;*/
/**/
/*%sample_1percent(store.SEOUL_HH1, store.SEOUL_HH1SMPL);*/
/*%generate_eq_dataset(SEOUL, hhtype=HH1SMPL);*/
/*%sample_1percent(store.SEOUL_HH2, store.SEOUL_HH2SMPL);*/
/*%generate_eq_dataset(SEOUL, hhtype=HH2SMPL);*/
