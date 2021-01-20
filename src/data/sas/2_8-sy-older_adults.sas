/*OPTIONS NONOTES NOSOURCE NODATE NOCENTER LABEL NONUMBER LS=200 PS=MAX;*/
options symbolgen nosqlremerge;

libname OUT '/userdata07/room285/data_out/output-older_adults';
libname STORE '/userdata07/room285/data_out/data_store';


%macro new_sheet(name=);
ods excel options(sheet_interval="TABLE" sheet_name=&name);
ods select none; data _null_; dcl odsout obj(); run; ods select all;
ods excel options(sheet_interval="NONE" sheet_name=&name);
ods select none; data _null_; dcl odsout obj(); run; ods select all;
%mend new_sheet;


%macro create_older_adult_datasets;
proc sql;
create table tmp_hh as
select std_yyyy
	, new_hh_id
	, case when count(*)=1 then "독거"
		when count(*)=2 and sum(fmly_line="0O")=1 and sum(age >= 62)=2 then " 노인부부"	
		else "  성인동거" end as older_adult_hh_type
	, sum(inc_tot - inc_pnsn) as inc_market
	, sum(inc_pnsn) as inc_pnsn
from store.seoulpanel2
where std_yyyy in ("2006", "2018") and sido="11"
group by std_yyyy, new_hh_id;
quit;

proc sql;
create table out.older_adults_seoul_eq2 as
select a.std_yyyy
	, a.INDI_DSCM_NO
	, case when b.age < 70 then "  62~69"
		when b.age >= 70 and b.age < 80 then " 70~79"
		else "80+" end as old_age_group
	, b.sex_type as sex_type
	, case when a.INDI_DSCM_NO=a.new_hh_id then " 가구주" 
		else "가구원" end as head_or_dependent
	, c.older_adult_hh_type
	, b.inc_wage
	, b.inc_bus
	, b.inc_wage + b.inc_bus as inc_labor
	, b.inc_tot - b.inc_pnsn as inc_market
	, b.inc_pnsn_natl
	, b.INC_PNSN_OCCUP
	, b.inc_pnsn
	, b.inc_tot
	, a.inc_tot as eq_inc_tot
	, c.inc_market / sqrt(a.hh_size) as eq_inc_market
	, c.inc_pnsn / sqrt(a.hh_size) as eq_inc_pnsn
from store.seoul_eq2 as a
inner join store.seoul as b
	on a.std_yyyy=b.std_yyyy and a.indi_dscm_no=b.indi_dscm_no
inner join tmp_hh as c
	on a.std_yyyy=c.std_yyyy and a.new_hh_id=c.new_hh_id
where a.std_yyyy in ("2006", "2018") and b.age >= 62;
quit;

%let regions=seoul kr;
%do i=1 %to %sysfunc(countw(&regions));
	%let region=%scan(&regions, &i);
	/* Individual income by income source -----------------------------------*/
	proc sql;
	create table out.older_adults_&region as
	select std_yyyy
		, INDI_DSCM_NO
		, case when age < 70 then "  62~69"
			when age >= 70 and age < 80 then " 70~79"
			else "80+" end as old_age_group
		, sex_type
		, inc_wage
		, inc_bus
		, inc_wage + inc_bus as inc_labor
		, inc_tot - inc_pnsn as inc_market
		, inc_pnsn_natl
		, INC_PNSN_OCCUP
		, inc_pnsn
		, inc_tot
	from store.&region
	where std_yyyy in ("2006", "2018") and age >= 62;
	quit;

	/* Equivalized (주민등록) income by source & by household type --------------*/
	proc sql;
	create table tmp_hh1_&region as
	select std_yyyy
		, HHRR_HEAD_INDI_DSCM_NO
		, case when count(*)=1 then "  독거"
			when count(*)=2 and sum(fmly_line="0O")=1 and sum(age >= 62)=2 then " 노인부부"	
			else "성인동거" end as older_adult_hh_type
	from store.&region
	where std_yyyy in ("2006", "2018")
	group by std_yyyy, HHRR_HEAD_INDI_DSCM_NO;
	quit;

	proc sql;
	create table out.older_adults_&region._eq1 as
	select a.std_yyyy
		, a.indi_dscm_no
		, b.older_adult_hh_type
		, a.hhrr_head_indi_dscm_no
		, a.inc_tot - a.inc_pnsn as inc_market
		, a.inc_pnsn as inc_pnsn
		, a.inc_tot as inc_tot
	from store.&region._eq1 as a
	inner join tmp_hh1_&region as b
		on a.std_yyyy=b.std_yyyy and a.hhrr_head_indi_dscm_no=b.hhrr_head_indi_dscm_no
	where a.std_yyyy in ("2006", "2018") and a.indi_age >= 62;
	%end;
%mend;

%macro append_to_older_adults_xlsx(dname, sheet_name);
proc export data=out.&dname
	outfile="/userdata07/room285/data_out/output-older_adults/older_adults.xlsx"
	DBMS=xlsx
	replace;
	sheet=&sheet_name;
run;
%mend;
%macro demographics;
proc sql;
create table out.older_adults_demogr as 
select a.*, b.count as num_indi
from (select std_yyyy
		, old_age_group
		, sex_type
		, count(*) as count
		, sum(head_or_dependent=" 가구주") as count_hh_head
		, sum(older_adult_hh_type="독거") as hh_type_single
		, sum(older_adult_hh_type=" 노인부부") as hh_type_couple
		, sum(older_adult_hh_type="  성인동거") as hh_type_adult_dep
		, sum(inc_pnsn_natl > 0) as inc_pnsn_natl_earner
		, sum(inc_pnsn_occup > 0) as inc_pnsn_occup_earner
	from out.older_adults_seoul_eq2
	group by std_yyyy, old_age_group, sex_type
) as a
left join (
	select std_yyyy, count(*) as count 
	from store.seoul_eq2 
	group by std_yyyy
) as b
	 on a.std_yyyy=b.std_yyyy;
quit;
%mend;

%macro individual_income_by_source;
%let regions=seoul kr;
%let vnames=inc_wage inc_bus inc_labor inc_market inc_pnsn inc_tot;
%let savename=indi_income_by_source;
/*Create an empty table into which we'll accumulate results*/
proc sql;
create table out.&savename (
	region char(32)
	, var char(32)
	, std_yyyy char(8)
	, old_age_group char(8)
	, sex_type char(4)
	, count num
	, frac_earners num
	, mean num
	, median num
);
quit;

%do i=1 %to %sysfunc(countw(&regions));
	%let region=%scan(&regions, &i);
	proc sql;
	create table yearly_pop_&region as
	select std_yyyy
		, case when age < 70 then "  62~69"
			when age >= 70 and age < 80 then " 70~79"
			else "80+" end as old_age_group
		, sex_type
		, count(*) as count 
	from store.&region
	where std_yyyy in ("2006", "2018") and age >=62
	group by std_yyyy, old_age_group, sex_type;
	quit;

	%do j=1 %to %sysfunc(countw(&vnames));
		%let vname=%scan(&vnames, &j);
		proc sql;
		insert into out.&savename
		select a.region, a.var, a.std_yyyy, a.old_age_group, a.sex_type
			, a.count
			, a.count / b.count as frac_earners
			, a.mean
			, a.median 
		from (
			select "&region" as region
				, "&vname" as var
				, std_yyyy
				, old_age_group
				, sex_type
				, count(*) as count
				, mean(&vname) as mean
				, median(&vname) as median
			from out.older_adults_&region
			where &vname > 0
			group by std_yyyy, old_age_group, sex_type
		) as a
		left join yearly_pop_&region as b
			on a.std_yyyy=b.std_yyyy and 
				a.old_age_group=b.old_age_group and
				a.sex_type=b.sex_type;
		quit;
		%end;

	proc sql;
	insert into out.&savename
	select "&region" as region
		, "inc_tot(all)" as var
		, std_yyyy
		, old_age_group
		, sex_type
		, count(*) as count
		, 1 as frac_earners
		, mean(&vname) as mean
		, median(&vname) as median
	from out.older_adults_&region
	group by std_yyyy, old_age_group, sex_type;
	quit;
	%end;
%mend;
%macro income_by_pnsn_status_var(region,earner_type,pnsn_type,vname);
%let where_expr_pop=;
%if &pnsn_type="  국민연금" %then %do;
	%let where_expr=inc_pnsn_natl> 0;
	%end;
%else %if &pnsn_type=" 직역연금" %then %do;
	%let where_expr=inc_pnsn_occup> 0;
	%end;
%else %if &pnsn_type="비수급자" %then %do;
	%let where_expr=inc_pnsn=0;
	%end;
%else %do;
	%put "Unknown pnsn_type=&pnsn_type";
	%abort cancel;
	%end;

%if &earner_type=" 시장소득자" %then %do;
	%let where_expr_pop=where &where_expr;
	%let where_expr=&where_expr and &vname > 0;
	%end;

proc sql;
insert into out.income_by_pnsn_status
select a.region, a.earner_type, a.std_yyyy, a.pnsn_type, a.count
	, a.count / b.count as frac_earners
	, a.mean, a.median
from (select "&region" as region
		, &earner_type as earner_type
		, std_yyyy
		, &pnsn_type as pnsn_type
		, count(*) as count
		, mean(&vname) as mean
		, median(&vname) as median
	from out.older_adults_&region
	where &where_expr
	group by std_yyyy) as a
left join (
	select std_yyyy, count(*) as count
	from out.older_adults_&region
	&where_expr_pop
	group by std_yyyy
) as b
	on a.std_yyyy=b.std_yyyy;
%mend income_by_pnsn_status_var;
%macro income_by_pnsn_status;
%let savename=income_by_pnsn_status;
proc sql;
create table out.&savename (
	region char(32)
	, earner_type char(32)
	, std_yyyy char(8)
	, pnsn_type char(32)
	, count num
	, frac_earners num
	, mean num
	, median num
);
quit;
%let regions=seoul kr;
%do i=1 %to %sysfunc(countw(&regions));
	%let region=%scan(&regions, &i);
	%income_by_pnsn_status_var(&region,"  연금","  국민연금",inc_pnsn_natl);
	%income_by_pnsn_status_var(&region,"  연금"," 직역연금",inc_pnsn_occup);
	%income_by_pnsn_status_var(&region,"  연금","비수급자",inc_pnsn);
	%income_by_pnsn_status_var(&region," 시장소득자" ,"  국민연금",inc_market);
	%income_by_pnsn_status_var(&region," 시장소득자" ," 직역연금",inc_market);
	%income_by_pnsn_status_var(&region," 시장소득자" ,"비수급자",inc_market);
	%income_by_pnsn_status_var(&region,"총소득" ,"  국민연금",inc_tot);
	%income_by_pnsn_status_var(&region,"총소득" ," 직역연금",inc_tot);
	%income_by_pnsn_status_var(&region,"총소득" ,"비수급자",inc_tot);

	proc sort data=out.&savename;
	by region std_yyyy earner_type pnsn_type;
	run;
	%end;
	
%mend income_by_pnsn_status;

%macro eq_inc_by_hh_type;
%let savename=eq_inc_by_hh_type;
proc sql;
create table out.&savename (
	region char(32)
	, std_yyyy char(8)
	, older_adult_hh_type char(32)
	, mean_inc_market num
	, median_inc_market num
	, mean_inc_pnsn num
	, median_inc_pnsn num
	, rpr_inc_market num
	, rpr_inc_tot num
);
%let regions=seoul kr;
%do i=1 %to %sysfunc(countw(&regions));
	%let region=%scan(&regions, &i);
	proc sql;
	create table pop_median_inc as
	select std_yyyy
		, median(inc_tot - inc_pnsn) as median_inc_market
		, median(inc_tot) as median_inc_tot
	from store.&region._eq1
	where std_yyyy in ("2006", "2018")
	group by std_yyyy;
	quit;

	proc sql;
	insert into out.&savename
	select "&region" as region
		, a.std_yyyy
		, cat(" ",a.older_adult_hh_type) as older_adult_hh_type
		, mean(a.inc_market) as mean_inc_market
		, median(a.inc_market) as median_inc_market
		, mean(a.inc_pnsn) as mean_inc_pnsn
		, median(a.inc_pnsn) as median_inc_pnsn
		, sum(a.inc_market <= b.median_inc_market/2) / count(*) as rpr_inc_market
		, sum(a.inc_tot <= b.median_inc_tot/2) / count(*) as rpr_inc_tot
	from out.older_adults_&region._eq1 as a
	left join pop_median_inc as b
		on a.std_yyyy=b.std_yyyy
	group by a.std_yyyy, a.older_adult_hh_type;
	quit;

	proc sql;
	insert into out.&savename
	select "&region" as region
		, a.std_yyyy
		, "전체" as older_adult_hh_type
		, mean(a.inc_market) as mean_inc_market
		, median(a.inc_market) as median_inc_market
		, mean(a.inc_pnsn) as mean_inc_pnsn
		, median(a.inc_pnsn) as median_inc_pnsn
		, sum(a.inc_market <= b.median_inc_market/2) / count(*) as rpr_inc_market
		, sum(a.inc_tot <= b.median_inc_tot/2) / count(*) as rpr_inc_tot
	from out.older_adults_&region._eq1 as a
	left join pop_median_inc as b
		on a.std_yyyy=b.std_yyyy
	group by a.std_yyyy;
	%end;

proc sort data=out.&savename;
by region std_yyyy older_adult_hh_type;
run;
%mend;

/*%create_older_adult_datasets;*/
/*%demographics;*/
%individual_income_by_source;
/*%income_by_pnsn_status;*/
/*%eq_inc_by_hh_type;*/

%append_to_older_adults_xlsx(older_adults_demogr,"1) older_adults_demogr(seoul)");
%append_to_older_adults_xlsx(indi_income_by_source,"2) indi_income_by_src");
%append_to_older_adults_xlsx(income_by_pnsn_status,"3) income_by_pnsn_status");
%append_to_older_adults_xlsx(eq_inc_by_hh_type,"4) eq_inc_by_hh_type");

/*proc tabulate data=out.older_adults;*/
/*class old_age_group sex_type head_or_dependent older_adult_hh_type;*/
/*by std_yyyy;*/
/*table (old_age_group="노인연령대")*(sex_type="성별")*N, (all head_or_dependent older_adult_hh_type);*/
/*run;*/

%macro tabulate_inc_prop_dist(year, filter=);
proc tabulate data=out.interim_inc_prop_ranked(where=(std_yyyy="&year" &filter));
class rnk_inc rnk_prop;
table (rnk_inc="균등화 총소득 분위" all="계")*(N), (rnk_prop="균등화 총재산세과표 분위" all="계") / nocellmerge;
run;
%mend;

%macro inc_prop_distribution;
proc sql;
create table tmp as
select std_yyyy
	, INDI_DSCM_NO
	, inc_tot
	, prop_txbs_tot
	, hh_size
	, indi_age
from store.seoul_eq2
where std_yyyy in ("2006", "2018")
order by std_yyyy;
quit;

proc rank data=work.tmp groups=10 ties=low out=out.interim_inc_prop_ranked;
var inc_tot prop_txbs_tot;
by std_yyyy;
ranks rnk_inc rnk_prop;
run;

proc sql;
update out.interim_inc_prop_ranked
	set rnk_inc = rnk_inc + 1
		, rnk_prop = rnk_prop + 1;
quit;

ods excel file="/userdata07/room285/data_out/output-older_adults/inc_prop_cross_dist.xlsx"
	options(sheet_interval='none');

%new_sheet(name='2006년 전체/62세이상/1인가구');
%tabulate_inc_prop_dist(2006);
%tabulate_inc_prop_dist(2006, filter=and indi_age>=62);
%tabulate_inc_prop_dist(2006, filter=and hh_size=1);

%new_sheet(name='2018년 전체/62세이상/1인가구');
%tabulate_inc_prop_dist(2018);
%tabulate_inc_prop_dist(2018, filter=and indi_age>=62);
%tabulate_inc_prop_dist(2018, filter=and hh_size=1);
ods excel close;
%mend;
/**/
/*%inc_prop_distribution;*/
