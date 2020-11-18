options symbolgen nosqlremerge;

libname DATA '/userdata07/room285/data_source/user_data';
libname OUT '/userdata07/room285/data_out/data_out';
libname STORE '/userdata07/room285/data_out/data_store';


/* 100����, ���� 1% 1000���� */
%macro get_quantiles(i, region, unit, year, var);
/* Use yearly table because using entire data with 
	group by STD_YYYY gives memory error */
%let centile_output = work.&region._&unit._centile_&year&i;
%let militile_output = work.&region._&unit._top1p_&year&i;

%if &unit=indi %then
	%do;
		%let input=store.&region._&year;
		/* FILTER AGE > 20 */
		%let age_lb_incl=20;
		/*TODO: ���� ����? 20�� �̻� �Ǵ� 19�� �̻� ��*/
		%if &var=inc_tot or &var=prop_txbs_tot %then 
			/* For totals, fill null with 0 and count them in*/
			%do;
				proc sql;
				create table tmp_var as
				select sum(&var, 0) as &var
				from &input
				where age >= &age_lb_incl;
				quit;
			%end;
		%else  
			%do;
				proc sql;
				create table tmp_var as
				select &var
				from &input
				where age >= &age_lb_incl;
				quit;
			%end;
	%end;
%else /* household-level or equivalized income */
	%do;
		%let input=store.&region._&unit._&year;
		%if &var=inc_tot or &var=prop_txbs_tot %then 
			/* For totals, fill null with 0 and count them in*/
			%do;
				proc sql;
				create table tmp_var as
				select sum(&var, 0) as &var
				from &input;
				quit;
			%end;
		%else  
			%do;
				proc sql;
				create table tmp_var as
				select &var
				from &input;
				quit;
			%end;
	%end;

/* 100���� */
proc rank data=work.tmp_var groups=100 out=tmp ties=low;
	var &var;
	ranks rnk;
run;

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

/* ���� 1% 1000���� (���� 1%�� �ٽ� 10�� �׷����� ����) */
proc rank data=tmp groups=10 out=tmp2 ties=low;
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

%macro get_quantiles_for_all_vars(region, unit);
%let vnames = inc_tot inc_wage prop_txbs_hs;
%local i vname;
%do i=1 %to %sysfunc(countw(&vnames));
	%do year=2003 %to 2018;
		%let vname=%scan(&vnames, &i);
		%get_quantiles(&i, &region, &unit, &year, &vname);
	%end;
%end;
data out.&region._&unit._centile;
	set work.&region._&unit._centile_:;
run;
data out.&region._&unit._top1p;
	set work.&region._&unit._top1p_:;
run;
%mend get_quantiles_for_all_vars;

/*%get_quantiles_for_all_vars(seoul, indi);*/
/**/
/*proc export data=OUT.SEOUL_CENTILE*/
/*	outfile="/userdata07/room285/data_out/data_out/seoul_indi_centile.csv"*/
/*	replace;*/
/*run;*/
/**/
/*proc export data=OUT.SEOUL_TOP1P*/
/*	outfile="/userdata07/room285/data_out/data_out/seoul_indi_top1p_1000tile.csv"*/
/*	replace;*/
/*run;*/

%get_quantiles_for_all_vars(seoul, hh);

proc export data=OUT.SEOUL_HH_CENTILE
	outfile="/userdata07/room285/data_out/data_out/seoul_hh_centile.csv"
	replace;
run;

proc export data=OUT.SEOUL_HH_TOP1P
	outfile="/userdata07/room285/data_out/data_out/seoul_hh_top1p_1000tile.csv"
	replace;
run;

%get_quantiles_for_all_vars(seoul, eq);

proc export data=OUT.SEOUL_EQ_CENTILE
	outfile="/userdata07/room285/data_out/data_out/seoul_eq_centile.csv"
	replace;
run;

proc export data=OUT.SEOUL_EQ_TOP1P
	outfile="/userdata07/room285/data_out/data_out/seoul_eq_top1p_1000tile.csv"
	replace;
run;
