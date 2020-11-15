options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';

%MACRO compute_indices(year, region, unit, vname);
	%local dname tot N idcol mean_val median_val gini iqsr median_val rpr;

	/* Uncomment to debug */
/*	%let unit = hh;*/
/*	%let year=2014;*/
/*	%let region=seoul;*/
/*	%let vname=inc_wage;*/
/*	%let dname = store.&region._&unit._&year;*/

	%if &unit=indi %then 
		%do;
			%let dname=store.&region._&year;
		%end;
	%else  
		%do;
			%let dname=store.&region._&unit._&year;
		%end;
	%put &dname;

	/* SORT BY THE VARIABLE */
	proc sql;
	create table work.tmp as
	select STD_YYYY
		, &vname
	from &dname
	where not missing(&vname) /* EXCLUDE INDIVIDUALS WITH MISSING VALUES*/
	order by &vname;
	quit;

	/* GET THE TOTAL(i.e. SUM) OF THE VARIABLE */
	proc sql;
	select sum(&vname) into :tot from work.tmp;
	quit;

	/* GET THE NUMBER OF OBSERVATIONS OF THE VARIABLE */
	proc sql;
	select count(*) into :N from work.tmp;
	quit;

	/* COMPUTE INDEX AND CUMULATIVE SUMS */
	data work.tmp;
	set work.tmp;
	index=_n_;					/*individual index (1, 2, 3, ...)*/
	cumpopfrac=index/&N;		/*cumulative population fraction*/
	varfrac=&vname/&tot;		/*individual income/capital share*/
	if first.&vname 			/*cumulative income/capital sum (not share)*/
		then cumvar=&vname;
		else cumvar+&vname;
	run;

	/*1. Gini-------------------------------------------------------*/
	proc sql;
	select (2 * sum(index * &vname) / (&N*&tot)) - ((&N + 1)/&N) into :Gini
	from tmp;
	quit;

	/*2. Income quintile share ratio------------------------------*/
	/*	- Compute top quintile share */
	proc sql;
	select sum(varfrac) into :top_quintile_share
	from tmp 
	where cumpopfrac > 0.8;
	quit;

	/*	- Compute bottom quintile share */
	proc sql;
	select sum(varfrac) into :bottom_quintile_share
	from tmp 
	where cumpopfrac <= 0.2;
	quit;

	%let iqsr = %sysevalf(&top_quintile_share/&bottom_quintile_share);

	/*3. Relative poverty rate------------------------------------*/
	proc sql;
	select median(&vname) into :median_val
	from tmp;
	select count(*) / &N into :rpr
	from tmp
	where &vname < (&median_val / 2);
	quit;

	/*4. Mean over median--------------------------------------*/
	proc sql;
	select mean(&vname) into :mean_val
	from tmp;
	quit;

	%let mean_over_median = %sysevalf(&mean_val/&median_val);

	/*5. Insert results into results table-----------------------*/
	proc sql;
	insert into out.&region._&unit._indices
	values ("&year", "&vname", &Gini, &iqsr, &rpr, &median_val, &mean_val, &mean_over_median);
	quit;
%MEND compute_indices;

%macro compute_indices_all(region, unit);
	/*Create an empty table into which we'll accumulate yearly results*/
/*	proc sql;*/
/*	create table out.&region._&unit._indices (*/
/*		std_yyyy char(4)*/
/*		, var char(32)*/
/*		, gini num*/
/*		, iqsr num*/
/*		, rpr num*/
/*		, median num*/
/*		, mean num*/
/*		, mean_over_median num*/
/*	);*/
/*	quit;*/

	/* Set variables of interest */
	%let vnames=inc_wage inc_bus inc_tot prop_txbs_hs prop_txbs_tot;

	/* Compute indices for each year, for each variable */
	%do year=2012 %to 2018;
		%do i=1 %to %sysfunc(countw(&vnames));
			%let vname = %scan(&vnames, &i);
			%compute_indices(&year, &region, &unit, &vname);
		%end;
	%end;

	/* Save to csv */
	proc export data=out.&region._&unit._indices
		outfile="/userdata07/room285/data_out/data_out/&region._&unit._indices.csv"
		replace;
	run;
%mend compute_indices_all;

/* Execute on Seoul, with unit=individuals */
/*%compute_indices_all(seoul, indi);*/

/* Execute on Seoul, with unit=households*/
%compute_indices_all(seoul, hh);

/* Execute on Seoul, with unit=equivalized*/
/*%compute_indices_all(seoul, eq);*/
