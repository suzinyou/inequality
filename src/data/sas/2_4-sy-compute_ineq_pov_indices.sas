options symbolgen nosqlremerge;

libname OUT '/userdata07/room285/data_out/output-indices';
libname MMS '/userdata07/room285/data_out/output-mean_median';
libname QTS '/userdata07/room285/data_out/output-quantiles';
libname STORE '/userdata07/room285/data_out/data_store';

/*&i, &region, &unit, &indices, &earner, &adult_age, &vname,
		&subregunit, &savename, subreg='', year_lb=&year_lb, year_ub=&year_ub*/
%MACRO compute_indices_var(
	i, region, unit, indices, earner, adult_age, vname, subregunit, savename, 
	year_lb, year_ub,
	subreg=''
);
%local dname where_conditions groupby_vars groupby_vars_sas groupby_join_on groupby_join_vars;
%local median_exists;

	/* Uncomment to debug */
	/*%let i=999;*/
	/*%let region=seoul;*/
	/*%let unit=adult;*/
	/*%let adult_age=20;*/
	/*%let earner=1;*/
	/*%let year=2014;	*/
	/*%let vname=inc_wage;*/
	/*%let subregunit=sigungu;*/
	/*%let savename=woohoo;*/

%if &unit=hh or &unit=eq %then %do;
	%let dname=store.&region._&unit;
	%end;
%else %do;
	%let dname=store.&region;
	%end;

/* Set filtering (where) expressions */
/*   filter year (can only handle 1 year at a time) */
%let where_conditions=(input(STD_YYYY, 4.)>=&year_lb and input(STD_YYYY, 4.)<=&year_ub);
/*   filter earners and/or adults*/
%if &unit=adult %then %do;
	%let where_conditions=&where_conditions. and age >= &adult_age;
	%end;
%if &unit=earner or &earner=1 %then %do;
	%let where_conditions=&where_conditions. and &vname > 0;
	%end;

/* If sub-region unit is specified */
%let groupby_vars=STD_YYYY;
%let groupby_join_on=a.STD_YYYY=b.STD_YYYY;
%let groupby_join_vars=a.STD_YYYY;
%if &subregunit~='' %then %do;
	%let groupby_vars=&groupby_vars., &subregunit.;
	%let groupby_join_on=&groupby_join_on. and a.&subregunit.=b.&subregunit;
	%let groupby_join_vars=&groupby_join_vars., a.&subregunit;
	%end;

/* For DATA step, make a space separated list of groupby variables (without commas) */
%let groupby_vars_sas=%sysfunc(tranwrd(%quote(&groupby_vars),%str(,),%str()));
%let sorted=0;

/* FILTER VARIABLE */
data work.tmp;
set &dname(where=(&where_conditions));
keep &groupby_vars_sas &vname;
run;

%do i=1 %to %sysfunc(countw(&indices));
	%let index=%scan(&indices,&i);
	%if &sorted=0 and (&index=gini or &index=iqsr) %then %do;
		/* SORT VARIABLE */
		proc sort data=work.tmp;
		 by &groupby_vars_sas &vname;
		run;

		/* GET THE TOTAL(i.e. SUM) & count OF THE VARIABLE */
		proc sql;
		create table work.tmp2 as
		select &groupby_vars. 
			, sum(&vname) as tot
			, count(*) as N 
		from work.tmp
		group by &groupby_vars;
		quit;

		proc sql;
		alter table work.tmp
		add tot num
			, N num;
		update work.tmp as a
		set tot=(select tot
				from work.tmp2 as b
				where &groupby_join_on)
			, N=(select N
				from work.tmp2 as b
				where &groupby_join_on);
		quit;

		/* COMPUTE INDEX AND CUMULATIVE SUMS */
		%if &subregunit~='' %then %do;
			data work.tmp;
			set work.tmp;
			count + 1;				/*individual count (1, 2, 3, ...)*/
			by STD_YYYY &subregunit &vname;
			if first.STD_YYYY or first.&subregunit
				then count= 1;
			cumpopfrac=count/N;		/*cumulative population fraction*/
			varfrac=&vname/tot;		/*individual income/capital share*/
			if first.&vname 		/*cumulative income/capital sum (not share)*/
				then cumvar=&vname;
				else cumvar+&vname;
			run;
			%end;
		%else %do;
			data work.tmp;
			set work.tmp;
			count + 1;				/*individual count (1, 2, 3, ...)*/
			by STD_YYYY &vname;
			if first.STD_YYYY
				then count=1;
			cumpopfrac=count/N;		/*cumulative population fraction*/
			varfrac=&vname/tot;		/*individual income/capital share*/
			if first.&vname 		/*cumulative income/capital sum (not share)*/
				then cumvar=&vname;
				else cumvar+&vname;
			run;
			%end;
		%let sorted=1;
		%end;

	%if &index=gini %then %do;
		/*1. Gini-------------------------------------------------------*/
		proc sql;
		create table tmp_gini as
		select &groupby_vars
			, (2 * sum(count * &vname) / (max(N)*max(tot))) - ((max(N) + 1)/max(N)) as Gini
		from tmp
		group by &groupby_vars.;
		quit;
		%end;
	%else %if &index=iqsr %then %do;
		/*2. Income quintile share ratio------------------------------*/
		/* Maybe get Top quintile share and bottom quintile share from quantiles if it exists*/

		/*	- Compute top quintile share */
		proc sql;
		create table tmp_top_qs as
		select &groupby_vars
			, sum(varfrac) as tqs
		from tmp 
		where cumpopfrac > 0.8
		group by &groupby_vars.;
		quit;

		/*	- Compute bottom quintile share */
		proc sql;
		create table tmp_bot_qs as
		select &groupby_vars
			, sum(varfrac) as bqs
		from tmp 
		where cumpopfrac <= 0.2
		group by &groupby_vars.;
		quit;

		proc sql;
		create table tmp_iqsr as
		select &groupby_join_vars., a.tqs/b.bqs as iqsr
		from tmp_top_qs as a
		left join tmp_bot_qs as b
		on &groupby_join_on;
		quit;
		%end;
	%else %if &index=rpr %then %do;
		/*3. Relative poverty rate------------------------------------*/
/*		%let median_exists=%sysfunc(exist(MMS.&savename));*/
/*		%if &median_exists=1 %then %do;*/
/*			proc sql;*/
/*			create table work.tmp_median as*/
/*			select &groupby_vars*/
/*				, median*/
/*			from MMS.&savename*/
/*			where var="&vname";*/
/*			quit;*/
/*			proc sql;*/
/*			select nobs > 0 into :median_exists*/
/*			from sashelp.vtable*/
/*			where libname="WORK" and memname="TMP_MEDIAN";*/
/*			quit;*/
/*			%end;*/
/*		%if &median_exists eq 0 %then %do;*/
			proc sql;
			create table tmp_median as
			select &groupby_vars
				, median(&vname) as median
			from tmp
			group by &groupby_vars.;
			quit;
/*			%end;*/

		proc sql;
		create table tmp_rpr as
		select &groupby_join_vars
			, count(*) / max(a.N) as rpr
		from (
			select a.*, b.median
			from tmp as a
			left join tmp_median as b
			on &groupby_join_on
		)
		where &vname <= median / 2
		group by &groupby_vars.;
		quit;
		%end;
	
	/* Insert or create */
	%if %sysfunc(exist(work.&savename)) %then %do;
		proc sql;
		alter table work.&savename
		add &index num;
		update work.&savename as a
		set &index=(
			select &index
			from tmp_&index as b 
			where &groupby_join_on.);
		quit;
		%end;
	%else %do;
		proc sql;
		create table work.&savename as
			select "&vname" as var, a.*
		from work.tmp_&index as a;
		quit;
		%end;
%end;

/*5. Insert results into results table-----------------------*/
proc sql;
insert into out.&savename
select * from work.&savename.;
quit;

/* DELETE TEMP DATASETS */
proc datasets lib=work nolist kill;
quit;
run;

%MEND compute_indices_var;

%macro compute_indices(
	region /* KR or SEOUL or ~PANEL*/,
	unit /* adult, earner, capita, hh or eq*/,
	vnames /* list of variable names, space separated */,
	indices /* select from {gini iqsr rpr} */,
	subregunit='' /* sido for KR, sigungu for SEOUL*/,
	earner=0 /* whether or not to filter only earners*/,
	adult_age=. /* age lower bound for adults, if unit=adult*/,
	year_lb=2003 /* year lowerbound */,
	year_ub=2018 /* year upperbound */
);
/* Compute mean and median income or prop_txbs according to unit */
%local i vname savename subreg_coldef;

/* Set savename */
%if &subregunit='' %then %do;
	%let savename=&region._&unit;
	%let subreg_coldef='';
	%end;
%else %do;
	%let savename=&region._&subregunit._&unit;
	%let subreg_coldef=, &subregunit char(3);
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

%if not %sysfunc(exist(out.&savename)) %then %do;
	/*Create an empty table into which we'll accumulate yearly results*/
	proc sql;
	create table out.&savename (
		var char(32)
		, STD_YYYY char(8)
		&subreg_coldef
		, gini num label='Gini'
		, iqsr num label='5��������'
		, rpr num label='�������'
	);
	quit;
	%end;

/* Compute indices for each variable */
%do i=1 %to %sysfunc(countw(&vnames));
	%let vname = %scan(&vnames, &i);
	%compute_indices_var(
		&i, &region, &unit, &indices, &earner, &adult_age, &vname,
		&subregunit, &savename, subreg='', year_lb=&year_lb, year_ub=&year_ub);
%end;

proc export data=out.&savename
	/* CHANGE OUTFILE PATH */
	outfile="/userdata07/room285/data_out/output-indices/indices.xlsx"
	DBMS=xlsx
	replace;
	sheet="&savename";
run;
%mend compute_indices;

/*%compute_indices(region=seoul_smpl, unit=adult, vnames=inc_tot prop_txbs_tot, indices=gini iqsr rpr, subregunit='', earner=0, adult_age=20, year_lb=2006, year_ub=2011);*/
/*%compute_indices(region=seoul_smpl, unit=adult, vnames=inc_tot prop_txbs_tot, indices=gini iqsr rpr, subregunit='', earner=0, adult_age=20, year_lb=2012, year_ub=2018);*/

/* ���� 20�� �̻� (�Ѽҵ�, ������ǥ) --------------------------------*/
/*%compute_indices(*/
/*	region=kr, */
/*	unit=adult, */
/*	vnames=inc_tot prop_txbs_tot, */
/*	indices=gini iqsr rpr, */
/*	subregunit='', */
/*	earner=0, */
/*	adult_age=20, */
/*	year_lb=2006, year_ub=2018);*/

/* ���� 20�� �̻� �ҵ��� ---------------------------------------------*/
/*%compute_indices(*/
/*	region=kr, */
/*	unit=adult, */
/*	vnames=inc_wage inc_bus prop_txbs_hs prop_txbs_lnd prop_txbs_bldg, */
/*	indices=gini iqsr rpr, */
/*	subregunit='', */
/*	earner=1, */
/*	adult_age=20, */
/*	year_lb=2006, year_ub=2018);*/

/* ���� 20�� �̻� (������ǥ) ---------------------------------------*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=prop_txbs_tot, indices=gini iqsr rpr, subregunit='', earner=0, adult_age=20, year_lb=2006, year_ub=2009);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=prop_txbs_tot, indices=gini iqsr rpr, subregunit='', earner=0, adult_age=20, year_lb=2010, year_ub=2013);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=prop_txbs_tot, indices=gini iqsr rpr, subregunit='', earner=0, adult_age=20, year_lb=2014, year_ub=2018);*/

/* ���� 20�� �̻� �ҵ��� ---------------------------------------------*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=inc_wage inc_bus prop_txbs_hs prop_txbs_lnd prop_txbs_bldg, */
/*	indices=gini iqsr rpr, */
/*	subregunit='', earner=1, adult_age=20, year_lb=2006, year_ub=2009);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=inc_wage inc_bus prop_txbs_hs prop_txbs_lnd prop_txbs_bldg, */
/*	indices=gini iqsr rpr,*/
/*	subregunit='', earner=1, adult_age=20, year_lb=2010, year_ub=2013);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=inc_wage inc_bus prop_txbs_hs prop_txbs_lnd prop_txbs_bldg, */
/*	indices=gini iqsr rpr, */
/*	subregunit='', earner=1, adult_age=20, year_lb=2014, year_ub=2018);*/

/* 25������ 20�� �̻� (�Ѽҵ�, ����꼼��ǥ) --------------------------*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=inc_tot prop_txbs_tot, */
/*	indices=gini iqsr rpr, */
/*	subregunit=sigungu, earner=0, adult_age=20, year_lb=2006, year_ub=2009);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=inc_tot prop_txbs_tot, */
/*	indices=gini iqsr rpr,*/
/*	subregunit=sigungu, earner=0, adult_age=20, year_lb=2010, year_ub=2013);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=inc_tot prop_txbs_tot, */
/*	indices=gini iqsr rpr, */
/*	subregunit=sigungu, earner=0, adult_age=20, year_lb=2014, year_ub=2018);*/

/* 25������ 20�� �̻� �ҵ��� -----------------------------------------*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=prop_txbs_hs prop_txbs_lnd prop_txbs_bldg, */
/*	indices=gini iqsr rpr, */
/*	subregunit=sigungu, earner=1, adult_age=20, year_lb=2006, year_ub=2009);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=prop_txbs_hs prop_txbs_lnd prop_txbs_bldg, */
/*	indices=gini iqsr rpr,*/
/*	subregunit=sigungu, earner=1, adult_age=20, year_lb=2010, year_ub=2013);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=prop_txbs_hs prop_txbs_lnd prop_txbs_bldg, */
/*	indices=gini iqsr rpr, */
/*	subregunit=sigungu, earner=1, adult_age=20, year_lb=2014, year_ub=2018);*/

/* ����&���� 15�� �̻� �Ѽҵ� ---------------------------------------------*/
/*%compute_indices(*/
/*	region=kr, unit=adult, */
/*	vnames=inc_tot, */
/*	indices=gini iqsr rpr, */
/*	subregunit='', earner=0, adult_age=15, year_lb=2006, year_ub=2018);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=inc_tot, */
/*	indices=gini iqsr rpr, */
/*	subregunit='', earner=0, adult_age=15, year_lb=2006, year_ub=2009);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=inc_tot, */
/*	indices=gini iqsr rpr, */
/*	subregunit='', earner=0, adult_age=15, year_lb=2010, year_ub=2013);*/
/*%compute_indices(*/
/*	region=seoul, unit=adult, */
/*	vnames=inc_tot, */
/*	indices=gini iqsr rpr, */
/*	subregunit='', earner=0, adult_age=15, year_lb=2014, year_ub=2018);*/

/*-------------------------RUN COMPLETE UP TO THIS LINE ------------------------*/
