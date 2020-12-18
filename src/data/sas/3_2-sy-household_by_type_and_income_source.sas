/*OPTIONS NONOTES NOSOURCE NODATE NOCENTER LABEL NONUMBER LS=200 PS=MAX;*/
/* 가구 유형마다 소득원천마다 평균값! */
options symbolgen nosqlremerge;

libname OUT '/userdata07/room285/data_out/output-household';
libname STORE '/userdata07/room285/data_out/data_store';

/* CODE */
%macro count_and_mean_group(dname, group_var, filter, savenamei, year_lb, year_ub);
%let where_expr=where input(STD_YYYY, 4.) between &year_lb and &year_ub;
%if &filter=single %then %do;
	%let where_expr=&where_expr. and hh_size=1;
	%end;


%if &group_var=working %then %do;
	%let group_select=case when is_working_head=1 then "head" when is_working_head=. and is_working_any=1 then "dependent" else "none" end as group length=16;
	%let groupby=STD_YYYY, group;
	%end;
%else %if &group_var=has_property %then %do;
	%let group_select=case when prop_txbs_tot>0 then "유재산" else "무재산" end as group length=16;
	%let groupby=STD_YYYY, group;
	%end;
%else %do;
	%let group_select=put(cats(&group_var), $16.) as group length=16;
	%let groupby=STD_YYYY, &group_var;
	%end;
proc sql;
create table &savenamei as
select "&filter" as filter length=32
	, STD_YYYY
	, "&group_var" as group_var length=32
	, &group_select
	, count(*) as count
	, mean(inc_wage) as mean_inc_wage
	, mean(inc_bus) as mean_inc_bus
	, mean(inc_fin) as mean_inc_fin
	, mean(inc_othr) as mean_inc_othr
	, mean(inc_pnsn) as mean_inc_pnsn
	, mean(inc_tot) as mean_inc_tot
	, median(inc_tot) as median_inc_tot
from &dname
&where_expr
group by &groupby;
quit;
%mend count_and_mean_group;

%macro count_and_mean(region, unit, group_vars, filter=none, year_lb=2006, year_ub=2018);
%let dname=store.&region._&unit;
%let savename=inc_src_&region._&unit;

%do i=1 %to %sysfunc(countw(&group_vars));
	%let group_var=%scan(&group_vars,&i);
	%let savenamei=work.&savename._&i;
	/*dname, group_var, where_expr, savenamei*/
	%count_and_mean_group(&dname, &group_var, &filter, &savenamei, &year_lb, &year_ub);
%end;

/* CREATE OUTPUT DATASET */
%if %sysfunc(exist(out.&savename)) %then %do;
	data out.&savename;
	set 
		out.&savename
		work.&savename._:;
	run;
	%end;
%else
%do;
	data out.&savename;
	set work.&savename._:;
	run;
	%end;

/* DELETE TEMP DATASETS */
proc datasets lib=work nolist kill;
quit;
run;

proc export data=out.&savename
	/* CHANGE OUTFILE PATH */
	outfile="/userdata07/room285/data_out/output-household/household.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%mend count_and_mean;


%count_and_mean(seoul, hh2, group_vars=hh_size_group age_group sex_type working gaibja_type_major has_property has_disability,year_lb=2018,year_ub=2018);
%count_and_mean(seoul, hh2, group_vars=age_group, filter=single, year_lb=2018,year_ub=2018);
%count_and_mean(seoul, eq2, group_vars=hh_size_group age_group sex_type working gaibja_type_major has_property has_disability,year_lb=2018,year_ub=2018);
%count_and_mean(seoul, eq2, group_vars=age_group, filter=single, year_lb=2018,year_ub=2018);
/*%count_and_mean(kr, eq1, group_vars=hh_size_group age_group,second_group=,year_lb=2018,year_ub=2018);*/

/*%count_and_mean(seoul, hh1, group_vars=hh_size_group,second_group=,year_lb=2018,year_ub=2018);*/
/*%count_and_mean(seoul, hh2, group_vars=hh_size_group,second_group=,year_lb=2018,year_ub=2018);*/
/*%count_and_mean(seoul, eq1, group_vars=hh_size_group age_group,second_group=,year_lb=2018,year_ub=2018);*/
/*%count_and_mean(seoul, eq2, group_vars=hh_size_group age_group,second_group=,year_lb=2018,year_ub=2018);*/

/*%count_and_mean(seoul, hh2, group_vars=age_group,second_group=,hh_size_cond='1',year_lb=2006,year_ub=2018);*/
/*%count_and_mean(seoul, hh2, group_vars=age_group,second_group=sex_type,hh_size_cond='1',earner=1,year_lb=2018,year_ub=2018);*/
