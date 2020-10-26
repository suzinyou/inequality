OPTIONS NONOTES NOSOURCE NODATE NOCENTER LABEL NONUMBER LS=200 PS=MAX;

/* 1. Generate household level data -----------------------------------------*/
sasfile SY.all load;

/* 1.1 Household income data + equivalized income (total, wage, business) ---*/
proc sql;
	create table SY.household_inc as
	select 
		a.STD_YYYY, 
		a.HHRR_HEAD_INDI_DSCM_NO_MY, 
		count(*) as household_size, 
		sum(a.INC_TOT) as hh_inc_tot, 
		sum(a.INC_TOT)/sqrt(count(*)) as eq_inc_tot,
		sum(a.INC_WAGE) as hh_inc_wage, 
		sum(a.INC_WAGE)/sqrt(count(*)) as eq_inc_wage,
		sum(a.INC_BUS) as hh_inc_bus, 
		sum(a.INC_BUS)/sqrt(count(*)) as eq_inc_bus
	from SY.all as a 
	group by a.STD_YYYY, a.HHRR_HEAD_INDI_DSCM_NO_MY;
quit;
run;

/* 1.2 Household head's demographic info */
proc sql;
	create table SY.household_dem as
	select HHRR_HEAD_INDI_DSCM_NO_MY, 
		SEX_TYPE as hh_head_sex_type, 
		GAIBJA_TYPE as hh_gaibja_type,
		RVSN_ADDR_CD as hh_rvsn_addr_cd
	from SY.all
	where HHRR_HEAD_INDI_DSCM_NO_MY eq INDI_DSCM_N;
quit;
run;

sasfile SY.all close;

/* 1.3 Merge income and demographic info */
proc sql;
	create table SY.household as select *
	from SY.household_inc as a 
	left join SY.household_dem as b 
	on a.HHRR_HEAD_INDI_DSCM_NO_MY = b.HHRR_HEAD_INDI_DSCM_NO_MY;
quit;
run;

/* 1.4 Check household size*/ 
proc univariate data=SY.household_inc noprint;
	var household_size;
	histogram household_size;
run;

/*---------------------------------------------------------------------------*/
/* 2. Generate city-wide, yearly income summary -----------------------------*/
sasfile SY.hosuehold_inc load;

/* 2.1 Basic stat yearly, Seoul only ----------------------------------------*/
proc means data=SY.household_inc noprint;
	where hh_inc_tot ne 0; /* add age constraint too? */
	class STD_YYYY;
	var hh_inc_tot eq_inc_tot hh_inc_wage eq_inc_wage hh_inc_bus eq_inc_bus;
	output out=SY.hh_inc_yearly_basic mean= min= max= median=/autoname;
run;

/* 2.2 100 percentiles of total income --------------------------------------*/
/* 2.2.1 Rank, into 100 groups */
proc sort data=SY.household_inc;
	by STD_YYYY;
run;

proc rank data=SY.household_inc groups=100 out=SY.household_inc_ranked ties=low;
	var hh_inc_tot eq_inc_tot;
	by STD_YYYY;
	ranks hh_rank eq_rank;
run;

sasfile SY.household_inc close;

/* 2.2.2 Compute lower&upper bounds and mean (and sum, for income share) for 
         each percentile */
%MACRO get_rank_boundaries(income_ranked_dataset, inc_var, rank_var, output_dataset);
	proc sql;
		create table SY.&output_dataset as
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

sasfile SY.household_inc_ranked load;

%get_rank_boundaries(SY.household_inc_ranked, hh_inc_tot, hh_rank, hh_inc_boundary_p100);
%get_rank_boundaries(SY.household_inc_ranked, eq_inc_tot, eq_rank, eq_inc_boundary_p100);

/* 2.3 Get TOP 1%'s deciles -------------------------------------------------*/
proc rank data=SY.household_inc_ranked 
groups=10 out=_hh_inc_ranked_top1pct ties=low;
	where hh_rank eq 99;
	var hh_inc_tot;
	by STD_YYYY;
	ranks hh_rank;  /* replace */
run;

proc rank data=SY.household_inc_ranked
groups=10 out=_eq_inc_ranked_top1pct ties=low;
	where eq_rank eq 99;
	var eq_inc_tot;
	by STD_YYYY;
	ranks eq_rank;
run;

sasfile SY.household_inc_ranked close;

%get_rank_boundaries(work._hh_inc_ranked_top1pct, hh_inc_tot, hh_rank, hh_inc_boundary_top1pct_10);
%get_rank_boundaries(work._eq_inc_ranked_top1pct, eq_inc_tot, eq_rank, eq_inc_boundary_top1pct_10);

/*---------------------------------------------------------------------------*/

/* ods html file='C:\Users\Suzin\workspace\inequality\data\processed\household income.html'; */

/* proc tabulate data=SY.all out=SY.household_inc; */
/*   class STD_YYYY HHRR_HEAD_INDI_DSCM_NO_MY; */
/*   var INC_TOT; */
/*   tables STD_YYYY, HHRR_HEAD_INDI_DSCM_NO_MY, INC_TOT*(N sum); */
/* run; */
/*  */
/*  */
/* proc report data=mydata nowd out=SY.repout; */
/*   column region payout payout=paysum; */
/*   define region / group style(column)=Header; */
/*   define payout / n 'Count Indicator'; */
/*   define paysum / sum 'Payout Sum'; */
/* run; */

/* ods html close; */
