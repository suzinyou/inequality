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

%generate_hh_dataset(SEOUL);
/*%generate_hh_dataset(KR);*/

/* Filter out households that have more than 10 ppl per household */
data STORE.SEOUL_HH1;
set STORE.SEOUL_HH;
where hh_size <= 10;
run;
/*data STORE.KR_HH1;*/
/*set STORE.KR_HH;*/
/*where hh_size <= 10;*/
/*run;*/

/* 2. CREATE EQUIVALIZED INCOME/TAXBASE TABLE ---------------------------*/
%macro generate_eq_dataset(region, hhtype);
%if &region=SEOUL or &region=seoul %then
	%do;
		%let extra_reg_col=, b.sigungu;
	%end;
%else
	%do;
		%let extra_reg_col=;
	%end;

%let outsuffix=%sysfunc(tranwrd(%sysfunc(tranwrd(&hhtype,hh,eq)),HH,EQ));

proc sql;
create table store.&region._&outsuffix as
select a.STD_YYYY
	, a.INDI_DSCM_NO
	, a.HHRR_HEAD_INDI_DSCM_NO
	, b.hh_size
	, b.SEX_TYPE
	, a.age as indi_age
	, a.inc_tot as indi_inc_tot
	, a.prop_txbs_tot as indi_prop_txbs_tot
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
from STORE.&region as a
inner join STORE.&region._&hhtype as b
on a.STD_YYYY = b.STD_YYYY
	and a.HHRR_HEAD_INDI_DSCM_NO = b.HHRR_HEAD_INDI_DSCM_NO;
quit;
%mend;
%generate_eq_dataset(SEOUL, HH1);
%generate_eq_dataset(SEOUL, HH2);
%generate_eq_dataset(KR, HH1);
/*%generate_eq_dataset(KRPANEL);*/


/* SPLIT SEOUL_HH AND SEOUL_EQ BY YEAR ------------------------------*/
/* (SOME OPERATIONS CAN ONLY HANDLE 1 YR AT A TIME)*/
/*%macro split_by_year(prefix);*/
/*%do year=2003 %to 2018;*/
/*	proc sql;*/
/*		create table STORE.&prefix._&year as*/
/*		select * */
/*		from STORE.&prefix*/
/*		where STD_YYYY = "&year";*/
/*	quit;*/
/*%end;*/
/*%mend split_by_year;*/
/*%split_by_year(SEOUL_HH);*/
/*%split_by_year(SEOUL_EQ);*/


/* Seoul household 1% sample -----------------------------------*/
%macro sample_1percent(dname, outname);
proc surveyselect data=&dname
	method=SRS
	seed=170011
	rate=0.01
	out=&outname;
STRATA STD_YYYY;
run;
%mend sample_1percent;

%sample_1percent(store.SEOUL_HH1, store.SEOUL_HH1SMPL);
%generate_eq_dataset(SEOUL, hhtype=HH1SMPL);
%sample_1percent(store.SEOUL_HH2, store.SEOUL_HH2SMPL);
%generate_eq_dataset(SEOUL, hhtype=HH2SMPL);
