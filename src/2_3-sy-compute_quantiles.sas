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


%macro get_quantiles(year, income);
	%let input=data.bfc_&year.;
	%let centile_output = out.bfc_&year._indi_centile_&income.;
	%let militile_output = out.bfc_&year._indi_toppct_&income.;
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
	proc rank data=tmp groups=1000 out=tmp2 ties=low;
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

%get_quantiles(2018, inc_tot);
%get_quantiles(2017, inc_tot);
%get_quantiles(2016, inc_tot);
%get_quantiles(2015, inc_tot);
%get_quantiles(2014, inc_tot);
%get_quantiles(2013, inc_tot);
%get_quantiles(2012, inc_tot);
%get_quantiles(2011, inc_tot);
%get_quantiles(2010, inc_tot);
%get_quantiles(2009, inc_tot);
%get_quantiles(2008, inc_tot);
%get_quantiles(2007, inc_tot);
%get_quantiles(2006, inc_tot);
%get_quantiles(2005, inc_tot);
%get_quantiles(2004, inc_tot);
%get_quantiles(2003, inc_tot);
%get_quantiles(2002, inc_tot);
/*%get_quantiles(data.BFC_2017, inc_tot);*/
/*%get_quantiles(data.BFC_2016, inc_tot);*/
/*%get_quantiles(data.BFC_2015, inc_tot);*/
/*%get_quantiles(data.BFC_2014, inc_tot);*/
/*%get_quantiles(data.BFC_2013, inc_tot);*/
/*%get_quantiles(data.BFC_2012, inc_tot);*/
/*%get_quantiles(data.BFC_2011, inc_tot);*/
/*%get_quantiles(data.BFC_2010, inc_tot);*/
/*%get_quantiles(data.BFC_2009, inc_tot);*/
/*%get_quantiles(data.BFC_2008, inc_tot);*/
/*%get_quantiles(data.BFC_2007, inc_tot);*/
/*%get_quantiles(data.BFC_2006, inc_tot);*/
/*%get_quantiles(data.BFC_2005, inc_tot);*/
/*%get_quantiles(data.BFC_2004, inc_tot);*/
/*%get_quantiles(data.BFC_2003, inc_tot);*/
/*%get_quantiles(data.BFC_2002, inc_tot);*/


/* combining*/
/*combine quantile data into one output!*/
%macro add_year(input, year);
proc sql;
	create table &input as
	select *, &year as std_yyyy
	from &input;
quit;
run;
%mend add_year;

data years;
	do year = 2002 to 2018;
	output;
	end;
run;

/**/
/*data _null_;*/
/*	set years;*/
/*	call execute(cats('%add_year(OUT.BFC_',year,'_INDI_CENTILE_INC_TOT,',year,');'));*/
/*run;*/
/**/
/*data _null_;*/
/*	set years;*/
/*	call execute(cats('%add_year(OUT.bfc_',year,'_INDI_TOPPCT_INC_TOT,',year,');'));*/
/*run;*/

data centile_names;
	do year = 2002 to 2018;
		name = cats("OUT.BFC_",year,"._INDI_CENTILE_INC_TOT");
	output;
	end;
run;

data toppct_names;
	do year = 2002 to 2018;
		name = cats("OUT.BFC_",year,"._INDI_TOPPCT_INC_TOT");
	output;
	end;
run;


/* CHANGE NAME OF DATASET */
%macro change_dataset_name;
%do year = 2002 %to 2018;
	proc datasets lib=DATA;
	CHANGE BFC_&year. = BFC_SEOUL_&year.;
	
	proc datasets lib=DATA;
	CHANGE BFC_&year._SMPL = BFC_SMPL_&year.;
%end;
%mend change_dataset_name;

/* UNION BFC's: using proc append to save memory/time */
%macro union_bfc(name);
%local year;
%do year=2002 %to 2018;
   %if &year=2002 %then %do;
   		data work.bfc_&name;
			set data.bfc_&name._&year;
	%end;
	%else %do;
		proc append base=work.bfc_&name data=data.bfc_&name._&year;
	%end;
%end;
%mend union_bfc;

proc export data=OUT.BFC_SEOUL_INDI_TOPPCT_INC_TOT
	outfile="/userdata07/room285/data_out/data_out/bfc_seoul_indi_toppct_inc_tot.csv"
	replace;
run;

proc export data=OUT.BFC_SEOUL_INDI_CENTILE_INC_TOT
	outfile="/userdata07/room285/data_out/data_out/bfc_seoul_indi_centile_inc_tot.csv"
	replace;
run;
