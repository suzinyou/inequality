options symbolgen nosqlremerge;

libname OUT '/userdata07/room285/data_out/output-demographics';
libname STORE '/userdata07/room285/data_out/data_store';

/* demographic info */

/* 1. Individuals -----------------------------------------------------------*/
%macro make_dataset_indi_dem(region);
%let savename=&region._indi;

proc sql;
create table work.&savename as
select STD_YYYY
	, SEX_TYPE
	, case
		when age < 20 then "0-19"
		when age >= 20 and age < 30 then "20-29"
		when age >= 30 and age < 40 then "30-39"
		when age >= 40 and age < 50 then "40-49"
		when age >= 50 and age < 60 then "50-59"
		when age >= 60 and age < 70 then "60-69"
		else "70+" end
		as age_group
	, inc_tot
	, prop_txbs_tot
from store.&region
where STD_YYYY in ("2006", "2010", "2014", "2018");
quit;

proc sql;
create table work.&savename.1 as
select STD_YYYY
	, SEX_TYPE
	, age_group
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
	, mean(prop_txbs_tot) as mean_prop_txbs_tot
from work.&savename
group by STD_YYYY, SEX_TYPE, age_group;

create table work.&savename.2 as
select STD_YYYY
	, age_group
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
	, mean(prop_txbs_tot) as mean_prop_txbs_tot
from work.&savename
group by STD_YYYY, age_group;

create table work.&savename.3 as
select STD_YYYY
	, SEX_TYPE
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
	, mean(prop_txbs_tot) as mean_prop_txbs_tot
from work.&savename
group by STD_YYYY, SEX_TYPE;
quit;

proc sql;
create table out.&savename as
select * from work.&savename.1
outer union corr select * from work.&savename.2
outer union corr select * from work.&savename.3;
quit;

proc export data=out.&savename
	/* CHANGE OUTFILE PATH */
	outfile="/userdata07/room285/data_out/output-demographics/demographics.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%mend make_dataset_indi_dem;

/* 2. Households */
%macro make_dataset_hh_dem(region, unit);
%let savename=&region._&unit;

proc sql;
create table work.&savename as
select STD_YYYY
	, case
		when hh_size=. then ''
		when hh_size < 5 then put(hh_size, 2.)
		when hh_size >= 5 then "5+"
		else '' end
		as hh_size_group
	, case
		when age=. then ''
		when age < 20 then "0-19"
		when age >= 20 and age < 30 then "20-29"
		when age >= 30 and age < 40 then "30-39"
		when age >= 40 and age < 50 then "40-49"
		when age >= 50 and age < 60 then "50-59"
		when age >= 60 and age < 70 then "60-69"
		when age >= 70 then "70+"
		else '' end
		as age_group
	, inc_tot
	, prop_txbs_tot
from store.&region._&unit
where STD_YYYY in ("2006", "2010", "2014", "2018");
quit;

proc sql;
create table work.&savename.1 as
select STD_YYYY
	, hh_size_group
	, age_group
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
	, mean(prop_txbs_tot) as mean_prop_txbs_tot
from work.&savename
group by STD_YYYY, hh_size_group, age_group;

create table work.&savename.2 as
select STD_YYYY
	, age_group
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
	, mean(prop_txbs_tot) as mean_prop_txbs_tot
from work.&savename
group by STD_YYYY, age_group;

create table work.&savename.3 as
select STD_YYYY
	, hh_size_group
	, count(*) as count
	, mean(inc_tot) as mean_inc_tot
	, mean(prop_txbs_tot) as mean_prop_txbs_tot
from work.&savename
group by STD_YYYY, hh_size_group;
quit;

proc sql;
create table out.&savename as
select * from work.&savename.1
outer union corr select * from work.&savename.2
outer union corr select * from work.&savename.3;
quit;

proc export data=out.&savename
	/* CHANGE OUTFILE PATH */
	outfile="/userdata07/room285/data_out/output-demographics/demographics.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;

%mend make_dataset_hh_dem;

%make_dataset_indi_dem(seoul);
%make_dataset_indi_dem(kr);
%make_dataset_hh_dem(seoul, hh);
%make_dataset_hh_dem(kr, hh);
%make_dataset_hh_dem(seoul, hh2);
%make_dataset_hh_dem(kr, hh2);
