/*OPTIONS NONOTES NOSOURCE NODATE NOCENTER LABEL NONUMBER LS=200 PS=MAX;*/
options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';

/* 1. Generate household level data -----------------------------------------*/
/* 1.1 Household income data + equivalized income (total, wage, business) ---*/
%macro generate_hh_dataset(region);
%if &region=seoul %then
	%do;
		%let extra_reg_col=, max(sigungu) as SIGUNGU;
	%end;
%else
	%do;
		%let extra_reg_col='';
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
		, max(sido) as SIDO
		&extra_reg_col
		, (case
			when sum(case when GAIBJA_TYPE = "7" then 1 else 0 end) > 0 then 1 
			else 0 end) as HH_GAIBJA_TYPE_7
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
	from STORE.&region
	where HHRR_HEAD_INDI_DSCM_NO eq INDI_DSCM_NO;
quit;

proc sql;
	create table STORE.&region._HH as
	select a.*
		, b.SEX_TYPE
		, b.age
		, b.JUNG_NO
	from WORK.TMP_HH as a
	inner join WORK.TMP_HH_DEM as b
	/* inner join to drop any HH Head IDs that don't exist as individual IDs */
	on a.STD_YYYY=b.STD_YYYY
		and a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO;
quit;
%mend generate_hh_dataset;

/* 1.2 Generate household dataset that mimics survey household units */
%macro generate_hh2_dataset(region);
proc sql;
create table store.&region._HH2 as /*after figuring out missing household heads, change this to work.&region._HH2*/
select STD_YYYY
	, JUNG_NO
	, sum(HH_SIZE) as HH_SIZE
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
	, max(age) as age  /* Is this ok? */
from STORE.&region._HH
where JUNG_NO  /* drop null */
group by STD_YYYY, JUNG_NO;
%mend generate_hh2_dataset;

/* Seoul takes long -- check KR first --*/
/*%generate_hh_dataset(SEOUL);*/
/*%generate_hh_dataset(KR);*/
/*%generate_hh2_dataset(SEOUL);*/
/*%generate_hh2_dataset(KR);*/

/* 1.2  가구 유형에 따른 평균, 중위 소득 */
%macro compute_stat_by_hh_gaib_type;
%let vnames = inc_tot inc_wage inc_bus;
%do i=1 %to %sysfunc(countw(&vnames));
	%let vname = %scan(&vnames, &i);
	proc sql;
	create table tmp_gaibja_&i as
	select STD_YYYY
		, HH_GAIBJA_TYPE
		, count(*) as COUNT
		, put("&vname", $32.) as VAR
		, mean(&vname) AS MEAN
		, median(&vname) AS MEDIAN
	from STORE.SEOUL_HH
	WHERE HHRR_HEAD_INDI_DSCM_NO NE .
	group by HH_GAIBJA_TYPE, STD_YYYY;
	QUIT;
%end;
data OUT.SEOUL_HH_GAIBJATYPE;
set work.tmp_gaibja_:;
run;
%mend;
%compute_stat_by_hh_gaib_type;

proc export data=OUT.SEOUL_HH_GAIBJATYPE
	outfile="/userdata07/room285/data_out/data_out/seoul_hh_gaibjatype.csv"
	replace;
run;

/* 2. CREATE EQUIVALIZED INCOME/TAXBASE TABLE ---------------------------*/
%macro generate_eq_dataset(region);
proc sql;
create table STORE.&region._EQ as
select a.STD_YYYY as STD_YYYY
	, b.INC_TOT/sqrt(b.HH_SIZE) as INC_TOT
	, b.INC_WAGE/sqrt(b.HH_SIZE) as INC_WAGE
	, b.INC_BUS/sqrt(b.HH_SIZE) as INC_BUS
	, b.PROP_TXBS_HS/sqrt(b.HH_SIZE) as PROP_TXBS_HS
	, b.PROP_TXBS_TOT/sqrt(b.HH_SIZE) as PROP_TXBS_TOT
from STORE.&region as a
left join STORE.&region._HH as b
on a.STD_YYYY = b.STD_YYYY
	and a.HHRR_HEAD_INDI_DSCM_NO = b.HHRR_HEAD_INDI_DSCM_NO;
quit;
%mend;
/*%%generate_eq_dataset(SEOUL);*/
%generate_eq_dataset(KR);
/*%generate_eq_dataset(KRPANEL);*/


/* SPLIT SEOUL_HH AND SEOUL_EQ BY YEAR ------------------------------*/
/* (SOME OPERATIONS CAN ONLY HANDLE 1 YR AT A TIME)*/
%macro split_by_year(prefix);
%do year=2003 %to 2018;
	proc sql;
		create table STORE.&prefix._&year as
		select * 
		from STORE.&prefix
		where STD_YYYY = "&year";
	quit;
%end;
%mend split_by_year;
%split_by_year(SEOUL_HH);
%split_by_year(SEOUL_EQ);

/* ------------------------FOR REFERENCE ONLY ------------------*/
/* 1.2 Household head's demographic info */
proc sql;
	create table DATA.household_dem as
	select HHRR_HEAD_INDI_DSCM_NO_MY, 
		SEX_TYPE as hh_head_sex_type, 
		GAIBJA_TYPE as hh_gaibja_type,
		EMP_Y as emp_y,
		sido as sido,
		sigungu as sigungu,
		dong as dong
	from DATA.all
	where HHRR_HEAD_INDI_DSCM_NO eq INDI_DSCM_NO;
quit;
run;


/* 1.3 Merge income and demographic info */
proc sql;
	create table DATA.household as select *
	from DATA.household_inc as a 
	left join DATA.household_dem as b 
	on a.HHRR_HEAD_INDI_DSCM_NO_MY = b.HHRR_HEAD_INDI_DSCM_NO_MY;
quit;
run;

/* 1.4 Check household size*/ 
proc univariate data=DATA.household_inc noprint;
	var household_size;
	histogram household_size;
run;

/*---------------------------------------------------------------------------*/
/* 2. Generate city-wide, yearly income summary -----------------------------*/
sasfile DATA.hosuehold_inc load;

/* 2.1 Basic stat yearly, Seoul only ----------------------------------------*/
proc means data=DATA.household_inc noprint;
	where hh_inc_tot ne 0; /* add age constraint too? */
	class STD_YYYY;
	var hh_inc_tot eq_inc_tot hh_inc_wage eq_inc_wage hh_inc_bus eq_inc_bus;
	output out=DATA.hh_inc_yearly_basic mean= min= max= median=/autoname;
run;

