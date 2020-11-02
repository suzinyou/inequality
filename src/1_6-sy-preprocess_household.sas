/*OPTIONS NONOTES NOSOURCE NODATE NOCENTER LABEL NONUMBER LS=200 PS=MAX;*/
options DATAmbolgen nosqlremerge;

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
		, sum(INC_TOT) as HH_INC_TOT
		, sum(INC_WAGE) as HH_INC_WAGE
		, sum(INC_BUS) as HH_INC_BUS
		, sum(PROP_TXBS_HS) as HH_PROP_TXBS_HS
		, sum(PROP_TXBS_LND) as HH_PROP_TXBS_LND
		, sum(PROP_TXBS_BLDG) as HH_PROP_TXBS_BLDG
		, sum(PROP_TXBS_TOT) as PROP_TXBS_BLDG
		/* 가구 유형*/
		, (case
			when sum(case when GAIBJA_TYPE = "5" or GAIBJA_TYPE = "6" then 1 else 0 end) > 0 then 1 
			when sum(case when GAIBJA_TYPE = "1" or GAIBJA_TYPE = "2" then 1 else 0 end) > 0 then 1 
			when sum(case when GAIBJA_TYPE = "7" or GAIBJA_TYPE = "8" then 1 else 0 end) > 0 then 1 
		else 4 end) as HH_GAIBJA_TYPE
		/*sum(PROP_TXBS_HS)/sqrt(count(*)) as hh_prop_txbs_hs,*/
	from STORE.SEOUL
	group by STD_YYYY, HHRR_HEAD_INDI_DSCM_NO;
quit;

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

/* 2.2 100 percentiles of total income --------------------------------------*/
/* 2.2.1 Rank, into 100 groups */
proc sort data=DATA.household_inc;
	by STD_YYYY;
run;

OPTIONS NOSQLREMERGE;

sasfile DATA.household_inc close;

/* 2.2.2 Compute lower&upper bounds and mean (and sum, for income share) for 
         each percentile */
%MACRO get_rank_boundaries(income_ranked_dataset, inc_var, rank_var, output_dataset);
	proc sql;
		create table DATA.&output_dataset as
		select STD_YYYY, &rank_var, 
			min(&inc_var) as p_min, 
			max(&inc_var) as p_max,
			mean(&inc_var) as p_mean,
			sum(&inc_var) as p_sum,
			count(*) as size
		from &income_ranked_dataset
		group by STD_YYYY, &rank_var order by STD_YYYY, &rank_var;
		quit;
	run;
%MEND get_rank_boundaries;

sasfile DATA.household_inc_ranked load;

%get_rank_boundaries(DATA.household_inc_ranked, hh_inc_tot, hh_rank, hh_inc_boundary_p100);
%get_rank_boundaries(DATA.household_inc_ranked, eq_inc_tot, eq_rank, eq_inc_boundary_p100);

/* 2.3 Get TOP 1%'s deciles -------------------------------------------------*/
proc rank data=DATA.household_inc_ranked 
groups=10 out=_hh_inc_ranked_top1pct ties=low;
	where hh_rank eq 99;
	var hh_inc_tot;
	by STD_YYYY;
	ranks hh_rank;  /* replace */
run;

proc rank data=DATA.household_inc_ranked
groups=10 out=_eq_inc_ranked_top1pct ties=low;
	where eq_rank eq 99;
	var eq_inc_tot;
	by STD_YYYY;
	ranks eq_rank;
run;

sasfile DATA.household_inc_ranked close;

%get_rank_boundaries(work._hh_inc_ranked_top1pct, hh_inc_tot, hh_rank, hh_inc_boundary_top1pct_10);
%get_rank_boundaries(work._eq_inc_ranked_top1pct, eq_inc_tot, eq_rank, eq_inc_boundary_top1pct_10);

/*---------------------------------------------------------------------------*/

/* ods html file='C:\Users\Suzin\workspace\inequality\data\processed\household income.html'; */

/* proc tabulate data=DATA.all out=DATA.household_inc; */
/*   class STD_YYYY HHRR_HEAD_INDI_DSCM_NO_MY; */
/*   var INC_TOT; */
/*   tables STD_YYYY, HHRR_HEAD_INDI_DSCM_NO_MY, INC_TOT*(N sum); */
/* run; */
/*  */
/*  */
/* proc report data=mydata nowd out=DATA.repout; */
/*   column region payout payout=paysum; */
/*   define region / group style(column)=Header; */
/*   define payout / n 'Count Indicator'; */
/*   define paysum / sum 'Payout Sum'; */
/* run; */

/* ods html close; */
