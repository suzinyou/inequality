options symbolgen nosqlremerge;

/* CHANGE OUTPUT FOLDER NAME */
libname OUT '/userdata07/room285/data_out/output-mean_median';
libname STORE '/userdata07/room285/data_out/data_store';

%macro make_dataset_popcnt(
	region /* KR or SEOUL or ~PANEL */,
	suffix /* _HH1, _HH2, etc*/,
	subregunit /* subregion unit: "sido" or "sigungu" */
);
%if %sysevalf(%superq(suffix)=,boolean) or &suffix=_smpl %then %do;
	%let select=count(*) as num_indi;
	%end;
%else %do;
	%let select=count(*) as num_hh, sum(hh_size) as num_indi;
	%end;

/* Make population count table */
/* (to compute fraction of earners/adults) */
proc sql;
	create table store.popcnt_&region.&suffix as
	select STD_YYYY
		, "&region" as region
		, &select
	from store.&region.&suffix
	group by STD_YYYY;
quit;

proc sql;
	create table store.popcnt_&region.&suffix._&subregunit as
	select STD_YYYY
		, "&region" as region
		, &subregunit
		, &select
	from store.&region.&suffix
	group by STD_YYYY, &subregunit;
quit;
%mend make_dataset_popcnt;

%macro compute_var_mean_median(i, region, unit, earner, adult_age, var, savename, subregunit, year_lb, year_ub);
/* Compute mean and median of given variable accoridng to given unit */
%local dname;

/* Set base dataset name */
%let unit_prefix=%sysfunc(substr(&unit,1,2));
%if "&unit_prefix"="hh" or "&unit_prefix"="eq" %then %do;
	%let dname=&region._&unit;
	%end;
%else %do;
	%let dname=&region;
	%end;

/* Select appropriate population count reference */
%if &subregunit~='' %then %do;
	%let groupby_vars=STD_YYYY, &subregunit;
	%let popcnt_dname=store.popcnt_&dname._&subregunit;
	%let popcnt_join_on=a.STD_YYYY=b.STD_YYYY and a.&subregunit.=b.&subregunit;
	%end;
%else %do;
	%let groupby_vars=STD_YYYY;
	%let popcnt_dname=store.popcnt_&dname;
	%if &unit_prefix=eq %then %do;
		%let popcnt_dname=%sysfunc(tranwrd(store.popcnt_&dname,eq,hh));
		%end;
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

%if &unit_prefix=hh or &unit_prefix=eq %then %do;
	proc sql;
	create table &savename as
	select "&var" as var length=32
		, a.*
		, b.num_hh
		, b.num_indi
	from (
		select &groupby_vars
			, mean(&var) as mean
			, median(&var) as median
		from store.&dname
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
		, b.num_indi
		, a.count / b.num_indi as frac_earners
	from (
		select &groupby_vars
			, count(&var) as count
			, mean(&var) as mean
			, median(&var) as median
		from store.&dname
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
	outfile="/userdata07/room285/data_out/output-mean_median/mean_median.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%mend compute_mean_median;


%macro compute_age_group_income(region); 
%local dname savename;
%let dname = store.&region;
%let savename = &region._agegroup;
proc sql;
create table out.&savename as
select STD_YYYY
	, (case when age < 15 then "0-14"
		when age >= 15 and age < 20 then "15-19"
		when age >= 20 and age < 25 then "20-24"
		when age >= 25 and age < 30 then "25-29"
		when age >= 30 and age < 35 then "30-34"
		when age >= 35 and age < 40 then "35-39"
		when age >= 40 and age < 45 then "40-44"
		when age >= 45 and age < 50 then "45-49"
		when age >= 50 and age < 55 then "50-54"
		when age >= 55 and age < 60 then "55-59"
		when age >= 60 and age < 65 then "60-64"
		when age >= 65 and age < 70 then "65-69"
		when age >= 70 and age < 75 then "70-74"
		when age >= 75 and age < 80 then "75-79"
		when age >= 80 and age < 85 then "80-84"
		else "85+" end) as age_group
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
    , median(inc_tot) as median_inc_tot
    , mean(inc_wage) as mean_inc_wage
    , median(inc_wage) as median_inc_wage
	, mean(inc_bus) as mean_inc_bus
    , median(inc_bus) as median_inc_bus
    , mean(prop_txbs_hs) as mean_inc_bus
    , median(prop_txbs_hs) as median_inc_bus
from &dname
group by STD_YYYY, age_group;
quit;

proc export data=out.&savename
	/* TODO: CHANGE OUTFILE PATH TO mean_median.xlsx? */
	outfile="/userdata07/room285/data_out/output-mean_median/mean_median_agegroup.xlsx" 
	DBMS=xlsx
	append;
	sheet="&savename";
run;
%mend;

/**/
/*%make_dataset_popcnt(seoul,_smpl,sigungu);*/
/*%make_dataset_popcnt(seoul,,sigungu);*/
/*%make_dataset_popcnt(kr,,sido);*/
/**/

%make_dataset_popcnt(seoul,_eq1,sigungu);
/*%make_dataset_popcnt(kr,_hh1,sido);*/
%make_dataset_popcnt(seoul,_eq2,sigungu);
/* TODO: re do potentially, after adjusting max hh_size !!!!!!!!!!!!!!!!!!!! */

/*--------------------------RUN COMPLETE FROM HERE-------------------------------*/

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

/*%compute_mean_median(SEOUL, adult, vnames=inc_tot, adult_age=20, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, adult, vnames=inc_tot, adult_age=15, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, adult, vnames=inc_wage inc_bus, earner=1, adult_age=20, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, capita, vnames=inc_tot, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, earner, vnames=inc_wage inc_bus inc_fin inc_othr inc_pnsn, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, adult, vnames=inc_tot, subregunit=sigungu, adult_age=20, year_lb=2006, year_ub=2018);*/

/*%compute_age_group_income(kr);*/

/*%compute_mean_median(KR, adult, vnames=prop_txbs_tot, adult_age=20, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(KR, adult, vnames=prop_txbs_hs prop_txbs_lnd prop_txbs_bldg, earner=1, adult_age=20, year_lb=2006, year_ub=2018);*/
/**/
/*%compute_mean_median(SEOUL, adult, vnames=prop_txbs_tot, adult_age=20, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, adult, vnames=prop_txbs_hs prop_txbs_lnd prop_txbs_bldg, earner=1, adult_age=20, year_lb=2006, year_ub=2018);*/
/**/
/*%compute_mean_median(SEOUL, adult, vnames=prop_txbs_tot, subregunit=sigungu, adult_age=20, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, adult, vnames=prop_txbs_hs prop_txbs_lnd prop_txbs_bldg, subregunit=sigungu, earner=1, adult_age=20, year_lb=2006, year_ub=2018);*/

/* 201214 추가: 총소득을 성인 소득자 기준으로! + 25개구별 근로소득, 사업소득도 확인 */
/*%compute_mean_median(KR, adult, vnames=inc_tot, earner=1, adult_age=20);*/
/*%compute_mean_median(SEOUL, adult, vnames=inc_tot, earner=1, adult_age=20, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, adult, vnames=inc_tot inc_wage inc_bus, subregunit=sigungu, earner=1, adult_age=20, year_lb=2006, year_ub=2018);*/

/*--------------------------RUN COMPLETE UP TO HERE-------------------------------*/

/* Run following after determining hh_size maximum */

/*%compute_mean_median(KR, hh1, vnames=inc_tot inc_wage inc_bus inc_fin inc_othr inc_pnsn);*/
/**/
/*%compute_mean_median(SEOUL, hh1, subregunit=sigungu, vnames=inc_tot, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, hh1, vnames=inc_tot inc_wage inc_bus inc_fin inc_othr inc_pnsn, year_lb=2006, year_ub=2018);*/
/**/
/*%compute_mean_median(SEOUL, hh22, subregunit=sigungu, vnames=inc_tot, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(SEOUL, hh22, vnames=inc_tot inc_wage inc_bus inc_fin inc_othr inc_pnsn, year_lb=2006, year_ub=2018);*/
/*%compute_mean_median(KR, eq, vnames=inc_tot);*/
/*%compute_mean_median(SEOUL, eq1smpl, vnames=inc_tot);*/
/*%compute_mean_median(SEOUL, eq22smpl, vnames=inc_tot);*/

%compute_mean_median(seoul, eq2, vnames=inc_tot, year_lb=2006, year_ub=2018);
%compute_mean_median(seoul, eq1, vnames=inc_tot, year_lb=2006, year_ub=2018);
