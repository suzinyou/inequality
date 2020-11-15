options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';


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


/* 개인 기준 100분위, 상위 1% 1000분위 */
%macro get_quantiles(i, region, year, var);
%let input=store.&region._&year;
%let centile_output = work.&region._centile_&year&i;
%let militile_output = work.&region._top1p_&year&i;
/*100분위 */
proc rank data=&input groups=100 out=tmp ties=low;
	var &var;
	ranks rnk;
run; /*TODO: add "by STD_YYYY" and use OUT.BFC_&region ?!*/

proc sql;
create table &centile_output as
	select
		&year as std_yyyy
		, put("&var", $32.) as var
		, min(rnk) as rank
		, count(*) as freq
		, min(&var) as rank_min
		, max(&var) as rank_max
		, mean(&var) as rank_mean
		, sum(&var) as rank_sum
	from tmp group by rnk;
quit;
run;

proc rank data=tmp groups=1000 out=tmp2 ties=low;
	where rnk eq 99;
	var &var;
	ranks rnk2;
run;

proc sql;
create table &militile_output as
	select
		&year as std_yyyy
		, put("&var", $32.) as var
		, min(rnk2) as rank
		, count(*) as freq
		, min(&var) as rank_min
		, max(&var) as rank_max
		, mean(&var) as rank_mean
		, sum(&var) as rank_sum
	from tmp2 group by rnk2;
quit;
run;
%mend get_quantiles;

%macro get_quantiles_for_all_vars(region);
%let vnames = inc_tot inc_wage prop_txbs_hs;
%local i vname;
%do i=1 %to %sysfunc(countw(&vnames));
	%do year=2003 %to 2018;
		%let vname=%scan(&vnames, &i);
		%get_quantiles(&i, &region, &year, &vname);
	%end;
%end;
data out.&region._centile;
	set work.&region._centile_:;
run;
data out.&region._top1p;
	set work.&region._top1p_:;
run;
%mend get_quantiles_for_all_vars;

%get_quantiles_for_all_vars(seoul);

proc export data=OUT.SEOUL_CENTILE
	outfile="/userdata07/room285/data_out/data_out/seoul_indi_centile.csv"
	replace;
run;

proc export data=OUT.SEOUL_TOP1P
	outfile="/userdata07/room285/data_out/data_out/seoul_indi_top1p_1000tile.csv"
	replace;
run;

%get_quantiles_for_all_vars(seoul_hh);

proc export data=OUT.SEOUL_HH_CENTILE
	outfile="/userdata07/room285/data_out/data_out/seoul_hh_centile.csv"
	replace;
run;

proc export data=OUT.SEOUL_HH_TOP1P
	outfile="/userdata07/room285/data_out/data_out/seoul_hh_top1p_1000tile.csv"
	replace;
run;
