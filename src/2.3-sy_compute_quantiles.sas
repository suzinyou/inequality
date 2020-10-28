OPTIONS NOSQLREMERGE symbolgen mprint mlogic source2;

%macro individual_income(bfc_input, out);
proc sql;
	create table tmp as
	select * 
	from &bfc_input 
	having STD_YYYY - BYEAR ge 15;
quit;
run;

proc sql;
	create table &out as
	select 
		mean(inc_tot) as mean_inc_tot,
		median(inc_tot) as median_inc_tot
	from tmp;
quit;
run;
%mend individual_income;

/*%individual_income(sy.csv2018, sy.csv2018_indi);*/


%macro get_quantiles(input, income);
	%let centile_output = &input._indi_centile_&income.;
	%let militile_output = &input._indi_toppct_&income.;
	proc rank data=&input groups=100 out=tmp ties=low;
		var &income;
		ranks rnk;
	run;
	proc sql;
	create table &centile_output as
		select
			min(rnk) as rank,
			count(*) as freq,
			min(&income) as rank_min,
			max(&income) as rank_max,
			sum(&income) as rank_sum
		from tmp group by rnk;
	quit;
	run;
	proc rank data=tmp groups=10 out=tmp2 ties=low;
		where rnk eq 99;
		var &income;
		ranks rnk;
	run;
	proc sql;
	create table &militile_output as
		select
			min(rnk) as rank,
			count(*) as freq,
			min(&income) as rank_min,
			max(&income) as rank_max,
			sum(&income) as rank_sum
		from tmp2 group by rnk;
	quit;
	run;
%mend get_quantiles;

%get_quantiles(sy.csv2018, inc_tot);

/*proc rank data=sy.csv2018 groups=100 out=tmp ties=low;*/
/*		var inc_tot;*/
/*		ranks rnk;*/
/*run;*/
/**/
/*%let income = inc_tot;*/
/*proc sql;*/
/*	create table indi_centile_inc_tot_csv2018 as*/
/*		select*/
/*			min(rnk) as rank,*/
/*			count(*) as freq,*/
/*			min(&income) as rank_min,*/
/*			max(&income) as rank_max,*/
/*			sum(&income) as rank_sum*/
/*		from tmp group by rnk;*/
/*	quit;*/
/*	run;*/
/**/
/*proc rank data=tmp groups=10 out=tmp2 ties=low;*/
/*		where rnk eq 99;*/
/*		var &income;*/
/*		ranks rnk;*/
/*	run;*/
/**/
/*proc sql;*/
/*	create table indi_militile_inc_tot_csv2018 as*/
/*		select*/
/*			min(rnk) as rank,*/
/*			count(*) as freq,*/
/*			min(&income) as rank_min,*/
/*			max(&income) as rank_max,*/
/*			sum(&income) as rank_sum*/
/*		from tmp2 group by rnk;*/
/*	quit;*/
/*	run;*/
/**/
/*proc rank data=sy.csv2018 groups=100 out=sy.csv2018_ranked ties=low;*/
/*	var inc_tot byear;*/
/*	ranks rank_tot rank_byear;*/
/*run;*/
/**/
/*proc sql;*/
/*	create table sy.csv2018_centile_range as*/
/*	select*/
/*		min(rank_tot) as rank_tot,*/
/*		count(*) as rank_tot_count,*/
/*		min(inc_tot) as inc_tot_centile_min,*/
/*		max(inc_tot) as inc_tot_centile_max,*/
/*		mean(inc_tot) as inc_tot_centile_mean,*/
/*		std(inc_tot) as inc_tot_centile_std*/
/*	from sy.csv2018_ranked group by rank_tot;*/
/*quit;*/
/*run;*/
