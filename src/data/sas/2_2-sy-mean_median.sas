options symbolgen nosqlremerge;

/* CHANGE OUTPUT FOLDER NAME */
libname OUT '/userdata07/room285/data_out/output-mean_median';
libname STORE '/userdata07/room285/data_out/data_store';

%macro make_dataset_popcnt(
	region /* KR or SEOUL or ~PANEL */, 
	subregunit /* subregion unit: "sido" or "sigungu" */
);
/* Make population count table */
/* (to compute fraction of earners/adults) */

proc sql;
	create table store.popcnt_&region as
	select STD_YYYY
		, "&region" as region
		, count(*) as num_indi
		, count(distinct(HHRR_HEAD_INDI_DSCM_NO)) as num_hh
	from store.&region
	group by STD_YYYY;
quit;

proc sql;
	create table store.popcnt_&region._&subregunit as
	select STD_YYYY
		, "&region" as region
		, &subregunit
		, count(*) as num_indi
		, count(distinct(HHRR_HEAD_INDI_DSCM_NO)) as num_hh
	from store.&region
	group by STD_YYYY, &subregunit;
quit;
%mend make_dataset_popcnt;

%macro compute_var_mean_median(i, region, unit, earner, adult_age, var, savename, subregunit, year_lb, year_ub);
/* Compute mean and median of given variable accoridng to given unit */
%local dname;

%if &unit=hh or &unit=eq %then %do;
	%let dname=store.&region._&unit;
	%end;
%else %do;
	%let dname=store.&region;
	%end;

/* If sub-region unit is specified */
%if &subregunit~='' %then %do;
	%let groupby_vars=STD_YYYY, &subregunit;
	%let popcnt_dname=store.popcnt_&region._&subregunit;
	%let popcnt_join_on=a.STD_YYYY=b.STD_YYYY and a.&subregunit.=b.&subregunit;
	%end;
%else %do;
	%let groupby_vars=STD_YYYY;
	%let popcnt_dname=store.popcnt_&region;
	%let popcnt_join_on=a.STD_YYYY=b.STD_YYYY;
	%end;

/* Set filtering (where) expressions */
%let where_expr=where input(STD_YYYY, 4.) between &year_lb and &year_ub;
%if &unit=adult %then %do;
	%let where_expr=&where_expr. and age >= &adult_age;
	%if &earner=1 %then %do;
		%let where_expr=&where_expr. and &var > 0;
		%end;
	%end;
%else %if &unit=earner %then %do;
	%let where_expr=&where_expr. and &var > 0;
	%end;

%if &unit=hh or &unit=hh2 or &unit=eq %then %do;
	proc sql;
	create table &savename as
	select "&var" as var length=32
		, a.*
		, b.num_hh as num_hh
	from (
		select &groupby_vars
			, mean(&var) as mean
			, median(&var) as median
		from &dname
			&where_expr
			group by &groupby_vars) as a
	left join &popcnt_dname as b
	on &popcnt_join_on;
	quit;
	%end;
%else %do;
	proc sql;
	create table &savename as
	select "&var" as var length=32
		, a.*
		, b.num_indi as num_indi
		, a.count / b.num_indi as frac_earners
	from (
		select &groupby_vars
			, count(&var) as count
			, mean(&var) as mean
			, median(&var) as median
		from &dname
			&where_expr
			group by &groupby_vars) as a
	left join &popcnt_dname as b
	on &popcnt_join_on;
	quit;
	%end;
%mend compute_var_mean_median;

%macro compute_mean_median(
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
	%compute_var_mean_median(
		&i, &region, &unit, &earner, &adult_age, &vname, &savenamei,
		&subregunit, year_lb=&year_lb, year_ub=&year_ub);
%end;

data out.&savename;
set work.&savename._:;

proc export data=out.&savename
	/* CHANGE OUTFILE PATH */
	outfile="/userdata07/room285/data_out/output-mean_median/mean_median.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%mend compute_mean_median;

/* TO DO: recalculate seoul_smpl after sampling based on HHs -------*/
/*%make_dataset_popcnt(seoul_smpl, sigungu);*/
/*%make_dataset_popcnt(seoul, sigungu);*/
/*%make_dataset_popcnt(kr, sido);*/

/*%compute_mean_median(KR, adult, vnames=inc_tot, adult_age=20);*/
/*%compute_mean_median(KR, adult, vnames=inc_tot, adult_age=15);*/
/*%compute_mean_median(KR, adult, vnames=inc_wage inc_bus, earner=1, adult_age=20);*/
/*%compute_mean_median(KR, unit=capita, vnames=inc_tot);*/
/*%compute_mean_median(KR, earner, vnames=inc_wage inc_bus inc_fin inc_othr inc_pnsn);*/
/*%compute_mean_median(KR, adult, vnames=inc_tot, subregunit=sido, adult_age=20);*/
/*%compute_mean_median(SEOUL_SMPL, adult, vnames=inc_tot, adult_age=20, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL_SMPL, adult, vnames=inc_tot, adult_age=15, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL_SMPL, adult, vnames=inc_wage inc_bus, earner=1, adult_age=20, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL_SMPL, unit=capita, vnames=inc_tot, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL_SMPL, earner, vnames=inc_wage inc_bus inc_fin inc_othr inc_pnsn, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL_SMPL, adult, vnames=inc_tot, subregunit=sigungu, adult_age=20, year_lb=2006, year_ub=2018);*/

/*%compute_mean_median(KR, hh, vnames=inc_tot inc_wage inc_bus inc_fin inc_othr inc_pnsn);*/
/*%compute_mean_median(SEOUL, hh, subregunit=sigungu, vnames=inc_tot, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, hh, vnames=inc_tot inc_wage inc_bus inc_fin inc_othr inc_pnsn, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(KR, eq, vnames=inc_tot);*/
/*%compute_mean_median(SEOUL, eq, vnames=inc_tot);*/

/*--------------------------RUN COMPLETE UP TO HERE-------------------------------*/

%compute_mean_median(SEOUL, adult, vnames=inc_tot, adult_age=20, year_lb=2006, year_ub=2018);
%compute_mean_median(SEOUL, adult, vnames=inc_tot, adult_age=15, year_lb=2006, year_ub=2018);
%compute_mean_median(SEOUL, adult, vnames=inc_wage inc_bus, earner=1, adult_age=20, year_lb=2006, year_ub=2018);
%compute_mean_median(SEOUL, capita, vnames=inc_tot, year_lb=2006, year_ub=2018);
%compute_mean_median(SEOUL, earner, vnames=inc_wage inc_bus inc_fin inc_othr inc_pnsn, year_lb=2006, year_ub=2018);
%compute_mean_median(SEOUL, adult, vnames=inc_tot, subregunit=sigungu, adult_age=20, year_lb=2006, year_ub=2018);
