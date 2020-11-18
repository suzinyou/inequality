options symbolgen nosqlremerge;

/*libname DATA '/userdata07/room285/data_source/user_data';*/
libname DATA '/userdata07/room285/data_source/user_data/201111ADD';
libname STORE '/userdata07/room285/data_out/data_store';

/* CHANGE NAME OF DATASET */
%macro change_dataset_name;
%do year = 2002 %to 2019;
	proc datasets lib=DATA;
	CHANGE BFC_&year. = BFC_SEOUL_&year.;
	
	proc datasets lib=DATA;
	CHANGE BFC_&year._SMPL_PANEL = BFC_SMPL_PANEL_&year.; 
%end;
%mend change_dataset_name;

%macro generate_features(region);
/* Compute features */
/* 
	[O] 주민등록주소 시도/시군구/동읍면 분리 
	[O] 사업장주소 시도/시군구/동읍면 분리
	[O] 나이 계산
*/
%if &region=seoul %then %do;
	proc sql;
		alter table STORE.&region
	    	add sido char(2)
				, sigungu char(3)
				, dong char(3)
				, firm_sido char(2)
				, firm_sigungu char(3)
				, firm_dong char(3)
				, age num;
			update STORE.&region
			set sido=substr(RVSN_ADDR_CD, 1, 2)
				, sigungu= substr(RVSN_ADDR_CD, 3, 3)
				, dong= substr(RVSN_ADDR_CD, 6, 3)
				, dong= substr(RVSN_ADDR_CD, 6, 3)
		        , firm_sido= (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 1, 2) end)
				, firm_sigungu= (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 3, 3) end)
		    	, firm_dong= (case when RVSN_FIRM_ADDR_CD is null then '' else substr(RVSN_FIRM_ADDR_CD, 6, 3) end)
				, age= input(STD_YYYY,4.) - input(BYEAR,4.);
	    quit;
	run;
%end;
%else %do;
	proc sql;
		alter table STORE.&region
	    	add sido char(2)
				, age num;
			update STORE.&region
			set sido=substr(RVSN_ADDR_CD, 1, 2)
				, age=input(STD_YYYY,4.) - input(BYEAR,4.);
	    quit;
	run;
%end;
%mend generate_features;

%macro union_bfc(region);
/* UNION BFC's: using proc append to save memory/time */
%if &region=smpl_panel %then %do;
	%let savename=krpanel; %end;
%else %if &region=smpl_xsection %then %do;
	%let savename=kr; %end;
%else %do;
	%let savename=&region; %end;

%local year;
%do year=2002 %to 2019;
	%if (&region=smpl_panel and &year=2002)
		or (&region=smpl_xsection and &year=2003) %then %do;
   		data store.&savename;
			set data.bfc_&year._&region;
	%end;
	%else %if &region=seoul and &year=2002 %then %do;
		data store.&savename;
			set data.bfc_&year;
		%end;
	%else %if &region=smpl_panel 
		or (&region=smpl_xsection and (&year=2010 or &year=2019)) %then %do;
		proc append base=store.&savename data=data.bfc_&year._&region;
	%end;
	%else %if &region=seoul %then %do;
		proc append base=store.&savename data=data.bfc_&year;
	%end;
%end;
%mend union_bfc;

%union_bfc(seoul);
/*%union_bfc(smpl_xsection);*/
/*%union_bfc(smpl_panel);*/
%generate_features(seoul);
/*%generate_features(kr);*/
/*%generate_features(krpanel);*/


proc sql;
	create table STORE.SEOUL as
	select * 
	from STORE.SEOUL
	where sido = "11";
quit;


/*%filter_regions;*/

/*%union_bfc(seoul);*/
/*%union_bfc(smpl);*/

/*%macro add_seoul_smpl_to_rest;*/
/*%local year;*/
/*%do year=2002 %to 2018;*/
/*proc sql;*/
/*	create table tmp as*/
/*	select HHRR_HEAD_INDI_DSCM_NO*/
/*	from STORE.SEOUL_&year*/
/*	where HHRR_HEAD_INDI_DSCM_NO = INDI_DSCM_NO;*/
/*quit;*/
/**/
/*proc sql;*/
/*	select count(*)  into :nhhh from tmp; /*Number of HouseHold Heads*/*/
/*quit;*/
/**/
/*proc sql;*/
/*	select round(&nhhh * 0.05, 1.0) into :num_sample */
/*	from tmp;*/
/*quit;*/
/**/
/*proc sql outobs=&num_sample;*/
/*	create table tmp2 as*/
/*	select **/
/*	from tmp*/
/*	order by ranuni(0);*/
/*quit;*/
/**/
/*/*샘플된 가구만 filter해서 KR에 병합*/*/
/*proc sql;*/
/*	create table STORE.KR_&year as*/
/*		select * */
/*		from STORE.KR_&year */
/*		union all*/
/*		(select **/
/*		from STORE.SEOUL_&year*/
/*		where HHRR_HEAD_INDI_DSCM_NO in */
/*			(select HHRR_HEAD_INDI_DSCM_NO from tmp2));*/
/*	quit;*/
/*run;*/
/*%mend;*/
