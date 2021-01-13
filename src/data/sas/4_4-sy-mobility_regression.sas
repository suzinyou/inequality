/* Social Mobility - 4. Regression analysis*/
options symbolgen nosqlremerge;

libname OUT '/userdata07/room285/data_out/output-social_mobility';
libname STORE '/userdata07/room285/data_out/data_store';

%macro create_dataset;
/* Individual variables from 2018 */
/* 	store.seoulpanel2 = store.seoulpanel과 동일하지만 재편된 가구 id도 붙어있는 데이터셋*/
/* 	HHRR_HEAD_INDI_DSCM_NO = 주민등록세대주*/
/* 	new_hh_id = 재편가구주*/
/*proc sql;*/
/*create table seoul_indi_18 as*/
/*select INDI_DSCM_NO*/
/*	, HHRR_HEAD_INDI_DSCM_NO */
/*	, new_hh_id*/
/*	, inc_wage+inc_bus as inc_labor_18*/
/*	, case when sex_type="2" then " 여성" when sex_type="1" then "남성" else "" end as is_female*/
/*	, case when foreigner_y="Y" then " 외국인" else "내국인" end as is_foreigner*/
/*	, case when age>=29 then cat("   ", put(age, 2.)) else "28" end as age_category*/
/*	, case when cmpr_dsb_grade="" then "없음" else cat("      ", cmpr_dsb_grade) end as cmpr_dsb_grade_filled*/
/*	, min(12, sum_wrk_m) as sum_wrk_m_clipped*/
/*	, min(12, hi_cnt_m) as hi_cnt_m_clipped*/
/*	, case when firm_scl_enter_nop_id<5 then "1~5"*/
/*		when firm_scl_enter_nop_id between 5 and 29 then "    5~29"*/
/*		when firm_scl_enter_nop_id between 30 and 99 then "   30~99"*/
/*		when firm_scl_enter_nop_id between 100 and 299 then "  100~299"*/
/*		else " 300+" end as firm_size_group*/
/*	, gaibja_type*/
/*	, case when inc_wage+inc_bus=0 then 0 else inc_wage / (inc_wage+inc_bus) end as frac_wage*/
/*	, case when sigungu in ("110", "140") then "     중앙"*/
/*		when sigungu in ("170", "440", "410") then "    마포서대문용산"*/
/*		when sigungu in ("650", "680", "710") then "   강남3구"*/
/*		when sigungu in ("305", "320", "350") then "  동북3구"*/
/*		when sigungu in ("530", "545", "620") then " 관구금"*/
/*		else "기타" end as gu_group*/
/*from store.SEOULPANEL2 */
/**/
/*where age >= 28 and age <= 32 and STD_YYYY="2018" and sido="11";*/
/*quit;*/

/* Household variables from 2018 */
/* HHRR_HEAD_INDI_DSCM_NO = 재편가구 id */
/*proc sql;*/
/*create table seoul_hh_18 as*/
/*select HHRR_HEAD_INDI_DSCM_NO */
/*	, hh_size*/
/*	, prop_txbs_tot as hh_prop_txbs_tot_18*/
/*from store.seoulpanel_hh2*/
/*where STD_YYYY="2018";*/
/*quit;*/

/* Individual(household) variables from 2006 */
/*proc sql;*/
/*create table seoul_indi_06 as*/
/*select INDI_DSCM_NO*/
/*	, inc_tot * hh_size**2 as hh_inc_tot_06*/
/*	, prop_txbs_tot * hh_size**2 as hh_prop_txbs_tot_06*/
/*	, case when sido="11" and input(sigungu, 3.) <=440 then "    서울강북"*/
/*		when sido="11" and input(sigungu, 3.) > 440 then "   서울강남"*/
/*		when sido="28" then "  인천"*/
/*		when sido="41" then " 경기"*/
/*		else "기타" end as region_06*/
/*from store.seoulpanel_eq2*/
/*where STD_YYYY="2006";*/
/*quit;*/

proc sql;
create table store.mobility_data as
select a.*
	, b.hh_size
	, b.hh_prop_txbs_tot_18
	, c.hh_inc_tot_06
	, c.hh_prop_txbs_tot_06
	, c.region_06
	, case when inc_labor_18=0 then 0 else log(inc_labor_18) end as log_inc_labor_18
	, case when hh_prop_txbs_tot_18=0 then 0 else log(hh_prop_txbs_tot_18) end as log_hh_prop_txbs_tot_18
	, case when hh_inc_tot_06=0 then 0 else log(hh_inc_tot_06) end as log_hh_inc_tot_06
	, case when hh_prop_txbs_tot_06=0 then 0 else log(hh_prop_txbs_tot_06) end as log_hh_prop_txbs_tot_06
from seoul_indi_18 as a
inner join seoul_hh_18 as b
	on a.new_hh_id=b.HHRR_HEAD_INDI_DSCM_NO
inner join seoul_indi_06 as c
	on a.INDI_DSCM_NO=c.INDI_DSCM_NO;
quit;
%mend create_dataset;

/*%create_dataset;*/

%macro print_descriptive_stats;
title "";
/*proc means data=store.mobility_data;*/
/*	var sum_wrk_m_clipped hi_cnt_m_clipped frac_wage hh_size log_hh_inc_tot_06 log_hh_prop_txbs_tot_06;*/
/*run;*/

%let cat_vars=is_female is_foreigner age_category cmpr_dsb_grade_filled firm_size_group gaibja_type gu_group region_06;
%do i=1 %to %sysfunc(countw(&cat_vars));
	%let cat=%scan(&cat_vars, &i);
	proc freq data=store.mobility_data;
		table &cat;
	run;
	%end;

proc univariate data=store.mobility_data;
	var sum_wrk_m_clipped hi_cnt_m_clipped frac_wage hh_size log_hh_inc_tot_06 log_hh_prop_txbs_tot_06;
	ods output BasicMeasures=varinfo;
run;

proc means data=store.mobility_data;
	var sum_wrk_m_clipped hi_cnt_m_clipped frac_wage hh_size log_hh_inc_tot_06 log_hh_prop_txbs_tot_06;
	by is_female;
run;

proc means data=store.mobility_data;
	var sum_wrk_m_clipped hi_cnt_m_clipped frac_wage hh_size log_hh_inc_tot_06 log_hh_prop_txbs_tot_06;
	by region_06;
run;
%mend;

%print_descriptive_stats;

/*proc qlim data=store.mobility_data plots=(predicted residual expected);*/
/*	class is_female is_foreigner age_category cmpr_dsb_grade_filled firm_size_group gaibja_type gu_group region_06;*/
/*	model log_inc_labor_18 = is_female is_foreigner age_category cmpr_dsb_grade_filled firm_size_group gaibja_type gu_group region_06 sum_wrk_m_clipped hi_cnt_m_clipped frac_wage hh_size log_hh_inc_tot_06 log_hh_prop_txbs_tot_06;*/
/*	endogenous log_inc_labor_18 ~ censored(lb=0);*/
/*	output out=out.reg_out_qlim predicted;*/
/*run;*/

/*ods graphics on;*/
/*ods trace on;*/
/*proc corr data=out.reg_out_qlim nosimple PLOTS(MAXPOINTS=none)=(SCATTER MATRIX(histogram));*/
/*	var log_inc_labor_18 p_log_inc_labor_18;*/
/*run;*/
/**/
/*proc sgplot data=out.reg_out_qlim;*/
/*scatter x=log_inc_labor_18 y=P_log_inc_labor_18 / markerattrs=(symbol=CircleFilled) transparency=0.995;*/
/*run;*/

/*proc glm data=store.mobility_data(where=(log_inc_labor_18>0)) plots(maxpoints=none)=(diagnostics residuals);*/
/*	class is_female is_foreigner age_category cmpr_dsb_grade_filled firm_size_group gaibja_type gu_group region_06;*/
/*	model log_inc_labor_18 = is_female is_foreigner age_category cmpr_dsb_grade_filled firm_size_group gaibja_type gu_group region_06 sum_wrk_m_clipped hi_cnt_m_clipped frac_wage hh_size log_hh_prop_txbs_tot_18 log_hh_inc_tot_06 log_hh_prop_txbs_tot_06 / solution;*/
/*run;*/
