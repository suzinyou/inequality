/*OPTIONS NONOTES NOSOURCE NODATE NOCENTER LABEL NONUMBER LS=200 PS=MAX;*/
options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';

/* 1. Generate household level data -----------------------------------------*/
/* 1.1 Household income data + equivalized income (total, wage, business) ---*/

proc sql;
	create table STORE.SEOUL_HH as
	select 
		STD_YYYY
		, HHRR_HEAD_INDI_DSCM_NO
		, count(*) as HH_SIZE
		, sum(INC_TOT) as INC_TOT
		, sum(INC_WAGE) as INC_WAGE
		, sum(INC_BUS) as INC_BUS
		, sum(PROP_TXBS_HS) as PROP_TXBS_HS
		, sum(PROP_TXBS_LND) as PROP_TXBS_LND
		, sum(PROP_TXBS_BLDG) as PROP_TXBS_BLDG
		, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
		, max(sido) as SIDO
		, max(sigungu) as SIGUNGU
		/* 가구 유형*/
		, (case
			when sum(case when GAIBJA_TYPE = "5" or GAIBJA_TYPE = "6" then 1 else 0 end) > 0 then 1 
			when sum(case when GAIBJA_TYPE = "1" or GAIBJA_TYPE = "2" then 1 else 0 end) > 0 then 2 
			when sum(case when GAIBJA_TYPE = "7" or GAIBJA_TYPE = "8" then 1 else 0 end) > 0 then 3
		else 4 end) as HH_GAIBJA_TYPE
		/*sum(PROP_TXBS_HS)/sqrt(count(*)) as hh_prop_txbs_hs,*/
	from STORE.SEOUL
	group by STD_YYYY, HHRR_HEAD_INDI_DSCM_NO;
quit;

/* 1.2  가구 유형에 따른 평균, 중위 소득 */
%macro compute_stat_by_hh_gaib_type;
%let vnames = inc_tot inc_wage inc_bus;
%do i=1 %to %sysfunc(countw(&vnames));
	proc sql;
	create table tmp_gaibja_&i as
	select STD_YYYY
		, HH_GAIBJA_TYPE
		, count(*) as COUNT
		, put("&vname", $32.) as vname
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

sasfile DATA.all close;

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

