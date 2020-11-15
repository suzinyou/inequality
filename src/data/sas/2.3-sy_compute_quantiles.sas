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
