OPTIONS nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname out '/userdata07/room285/data_out/data_out';


%macro household_interim(bfc, out);
proc sql;
	create table work.hh_inc_2018_smpl as
	select 
        max(a.STD_YYYY) as STD_YYYY,
		max(a.HHRR_HEAD_INDI_DSCM_NO) as HHRR_HEAD_INDI_DSCM_NO, 
		count(*) as household_size, 
		sum(a.INC_TOT) as hh_inc_tot, 
		sum(a.INC_TOT)/sqrt(count(*)) as eq_inc_tot,
		sum(a.INC_WAGE) as hh_inc_wage, 
		sum(a.INC_WAGE)/sqrt(count(*)) as eq_inc_wage,
		sum(a.INC_BUS) as hh_inc_bus, 
		sum(a.INC_BUS)/sqrt(count(*)) as eq_inc_bus
	from data.BFC_2018_SMPL as a 
	group by a.HHRR_HEAD_INDI_DSCM_NO;
quit;
run;
%mend household_interim;

%macro mean_median_inc(bfc, out);
proc sql;
	create table work.tmp as
	select input(std_yyyy,best.) as std_yyyy, inc_tot, inc_wage, inc_bus
	from &bfc
	having input(STD_YYYY,best4.) - input(BYEAR,best4.) ge 15;
run;
quit;
proc sql;
	create table &out as
	select
		max(std_yyyy) as std_yyyy,
		mean(inc_tot) as indi_inc_tot_mean,
		median(inc_tot) as indi_inc_tot_median,
		max(inc_tot) as indi_inc_tot_max,
		min(inc_tot) as indi_inc_tot_min,
		std(inc_tot) as indi_inc_tot_std,
		mean(inc_bus) as indi_inc_bus_mean,
		median(inc_bus) as indi_inc_bus_median,
		max(inc_bus) as indi_inc_bus_max,
		min(inc_bus) as indi_inc_bus_min,
		std(inc_bus) as indi_inc_bus_std,
		mean(inc_wage) as indi_inc_wage_mean,
		median(inc_wage) as indi_inc_wage_median,
		max(inc_wage) as indi_inc_wage_max,
		min(inc_wage) as indi_inc_wage_min,
		std(inc_wage) as indi_inc_wage_std
	from work.tmp;
quit;
run;
%mend mean_median_inc;

%mean_median_inc(data.bfc_2012, work.mean_median_inc_2012)
%mean_median_inc(data.bfc_2011, work.mean_median_inc_2011)
%mean_median_inc(data.bfc_2010, work.mean_median_inc_2010)
%mean_median_inc(data.bfc_2009, work.mean_median_inc_2009)
%mean_median_inc(data.bfc_2008, work.mean_median_inc_2008)
%mean_median_inc(data.bfc_2007, work.mean_median_inc_2007)
%mean_median_inc(data.bfc_2006, work.mean_median_inc_2006)
%mean_median_inc(data.bfc_2005, work.mean_median_inc_2005)
%mean_median_inc(data.bfc_2004, work.mean_median_inc_2004)
%mean_median_inc(data.bfc_2003, work.mean_median_inc_2003)
%mean_median_inc(data.bfc_2002, work.mean_median_inc_2002)

data out.mean_median_inc_seoul;
	set work.mean_median_inc_2002-work.mean_median_inc_2018;
run;
/*data out.mean_median_inc_seoul;*/
/*	set data.bfc_2002-data.bfc_2018;*/
/*	by memname;*/
/*	if first.memname;*/
/*	call execute(cats('%mean_median_inc(',memname,',mean_median_inc_',memname,')');*/
/*run;*/

/*proc sql;*/
/*	create table work.bfc_2018_smpl_inc from*/
/*	select std_yyyy, inc_tot, inc_bus, inc_wage */
/*	from data.bfc_2018_smpl*/
/*	order by inc_tot;*/
/*quit;*/
/*run;*/


/* 2.2.2 Compute lower&upper bounds and mean (and sum, for income share) for 
         each percentile */
%MACRO get_rank_boundaries(income_ranked_dataset, inc_var, rank_var, output_dataset);
	proc sql;
		create table work.&output_dataset as
		select STD_YYYY, &rank_var, 
			min(&inc_var) as p_min, 
			max(&inc_var) as p_max,
			mean(&inc_var) as p_mean,
			sum(&inc_var) as p_sum,
			count(*) as size
		from &income_ranked_dataset
		group by &rank_var;
		quit;
	run;
%MEND get_rank_boundaries;
