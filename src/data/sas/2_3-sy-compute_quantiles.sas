options symbolgen nosqlremerge;

libname OUT '/userdata07/room285/data_out/output-quantiles';
libname STORE '/userdata07/room285/data_out/data_store';


/* 100 분위 */
%macro compute_quantiles_var(
	i, region, unit, earner, adult_age, vname, savenamei,
	subregunit, year_lb, year_ub);
/* Use yearly table because using entire data with 
	group by STD_YYYY gives memory error */
%local dname groupby_vars where_expr;

%if &unit=hh or &unit=eq %then %do;
	%let dname=store.&region._&unit;
	%end;
%else %do;
	%let dname=store.&region;
	%end;

/* If sub-region unit is specified */
%if &subregunit~='' %then %do;
	%let groupby_vars=STD_YYYY, &subregunit;
	%end;
%else %do;
	%let groupby_vars=STD_YYYY;
	%end;

/* Set filtering (where) expressions */
%let where_expr=where input(STD_YYYY, 4.) between &year_lb and &year_ub;
%if &unit=adult %then %do;
	%let where_expr=&where_expr. and age >= &adult_age;
	%if &earner=1 %then %do;
		%let where_expr=&where_expr. and &vname > 0;
		%end;
	%end;
%else %if &unit=earner %then %do;
	%let where_expr=&where_expr. and &vname > 0;
	%end;

/*%let centile_output = work.&region._&unit._centile_&year&i;*/
/*%let militile_output = work.&region._&unit._top1p_&year&i;*/

proc sql;
create table tmp_var as
	select &groupby_vars., &vname
from &dname
&where_expr
order by &vname;
quit;

/* 100분위 */
proc rank data=work.tmp_var groups=100 out=tmp ties=low;
	var &vname;
	by STD_YYYY &subregunit;
	ranks rnk;
run;

proc sql;
create table &savenamei as
	select &groupby_vars
		, put("&vname", $32.) as var
		, min(rnk) as rank
		, count(*) as freq
		, min(&vname) as rank_min
		, max(&vname) as rank_max
		, mean(&vname) as rank_mean
		, sum(&vname) as rank_sum
	from tmp group by &groupby_vars., rnk;
quit;
run;

/* 상위 1% 1000분위 (상위 1%를 다시 10개 그룹으로 나눔) */
/*proc rank data=tmp groups=10 out=tmp2 ties=low;*/
/*	where rnk eq 99;*/
/*	var &vname;*/
/*	ranks rnk2;*/
/*run;*/
/**/
/*proc sql;*/
/*create table &militile_output as*/
/*	select*/
/*		&year as std_yyyy*/
/*		, put("&vname", $32.) as var*/
/*		, min(rnk2) as rank*/
/*		, count(*) as freq*/
/*		, min(&vname) as rank_min*/
/*		, max(&vname) as rank_max*/
/*		, mean(&vname) as rank_mean*/
/*		, sum(&vname) as rank_sum*/
/*	from tmp2 group by rnk2;*/
/*quit;*/
/*run;*/
%mend compute_quantiles_var;


%macro compute_quantiles(
	region /* KR or SEOUL or ~PANEL*/,
	unit /* adult, earner, capita, hh or eq*/,
	vnames /* list of variable names, space separated */,
	subregunit='' /* sido for KR, sigungu for SEOUL*/,
	earner=0 /* whether or not to filter only earners*/,
	adult_age=. /* age lower bound for adults, if unit=adult*/,
	year_lb=2003 /* year lowerbound */,
	year_ub=2018 /* year upperbound */
);
/* Compute mean and median income or prop_txbs according to unit */
%local i vname;

/* Set savename */
%if &subregunit='' %then %do;
	%let savename=&region._&unit;
	%end;
%else %do;
	%let savename=&region._&subregunit._&unit;
	%end;

%if &unit=adult and &adult_age=. %then %do;
	%abort cancel;
	%end;
%else %if &unit=adult %then %do;
	%let savename=&savename.&adult_age;
	%if &earner=1 %then %do;
		%let savename=&savename._earner;
		%end;
	%end;

/* Loop through variables to compute mean and median */
%do i=1 %to %sysfunc(countw(&vnames));
	%let vname = %scan(&vnames, &i);
	%let savenamei=work.&savename._&i;
	%compute_quantiles_var(
		&i, &region, &unit, &earner, &adult_age, &vname, &savenamei,
		&subregunit, year_lb=&year_lb, year_ub=&year_ub);
%end;

/* CREATE OUTPUT DATASET */
%if %sysfunc(exist(out.&savename)) %then %do;
	data out.&savename;
	set 
		out.&savename
		work.&savename._:;
	run;
	%end;
%else %do;
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
	outfile="/userdata07/room285/data_out/output-quantiles/quantiles.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%mend compute_quantiles;

/*%compute_quantiles(KR, adult, vnames=inc_tot, adult_age=20);*/
/*%compute_quantiles(KR, adult, vnames=inc_wage inc_bus, adult_age=20, earner=1);*/
/*%compute_quantiles(SEOUL, adult, vnames=inc_tot, adult_age=20, year_lb=2006, year_ub=2018);*/
/*%compute_quantiles(SEOUL, adult, vnames=inc_wage inc_bus, adult_age=20, earner=1, year_lb=2006, year_ub=2018);*/

/*--------------------------RUN COMPLETE UP TO HERE-------------------------------*/

%compute_quantiles(SEOUL, adult, vnames=inc_tot prop_txbs_tot, adult_age=20, subregunit=sigungu, earner=0, year_lb=2006, year_ub=2018);
%compute_quantiles(SEOUL, adult, vnames=prop_txbs_hs prop_txbs_lnd propt_txbs_bldg, adult_age=20, subregunit=sigungu, earner=1, year_lb=2006, year_ub=2018);
