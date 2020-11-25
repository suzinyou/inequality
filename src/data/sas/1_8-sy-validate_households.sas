options symbolgen nosqlremerge;

/* CHANGE OUTPUT FOLDER NAME */
libname OUT '/userdata07/room285/data_out/output-household_validation';
libname STORE '/userdata07/room285/data_out/data_store';

/* GROUP BY NHIS JUNG_NO*/
proc sql;
create table STORE.SEOUL_HH_NHIS as
	select STD_YYYY
		, JUNG_NO
		, count(*) as HH_SIZE
		, sum(INC_TOT) as INC_TOT
		, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
	from STORE.SEOUL
	group by STD_YYYY, JUNG_NO;
quit;

/* GROUP BY JUMIN NO. (���� ���� ���� ������ ���� ��������?)*/
proc sql;
create table STORE.SEOUL_HH_JUMIN as
	select STD_YYYY
		, HHRR_HEAD_INDI_DSCM_NO
		, count(*) as HH_SIZE
		, sum(INC_TOT) as INC_TOT
		, sum(PROP_TXBS_TOT) as PROP_TXBS_TOT
	from STORE.SEOUL
	group by STD_YYYY, HHRR_HEAD_INDI_DSCM_NO;
quit;

/* MAKE BASE DATASET FOR HOUSEHOLD VALIDATION */
proc sql;
create table STORE.SEOUL_HHVAL as
	select a.STD_YYYY
		, a.INDI_DSCM_NO
		, a.HHRR_HEAD_INDI_DSCM_NO
		, a.JUNG_NO
		, a.age
		, a.INC_TOT
		, a.PROP_TXBS_TOT
		, a.GAIBJA_TYPE
		, b.hh_size as hh_size_jumin
		, c.hh_size as hh_size_nhis
	from store.SEOUL as a
	left join store.SEOUL_HH_JUMIN as b
		on a.STD_YYYY=b.STD_YYYY and a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO
	left join store.SEOUL_HH_NHIS as c
		on a.STD_YYYY=c.STD_YYYY and a.JUNG_NO=c.JUNG_NO;
quit;

/* GET HOUSEHOLD HEAD'S GAIBJA_TYPE*/
proc sql;
create table STORE.SEOUL_HHVAL as
	select a.*
		, b.HH_GAIBJA_TYPE_JUMIN
	from STORE.SEOUL_HHVAL as a
	left join (
		select STD_YYYY
			, HHRR_HEAD_INDI_DSCM_NO
			, GAIBJA_TYPE as HH_GAIBJA_TYPE_JUMIN
		from STORE.SEOUL
		where HHRR_HEAD_INDI_DSCM_NO eq INDI_DSCM_NO
	) as b
	on a.STD_YYYY=b.STD_YYYY and a.HHRR_HEAD_INDI_DSCM_NO=b.HHRR_HEAD_INDI_DSCM_NO;
quit;

/* COMPUTE CATEGORIES FOR CROSS TABULATION */
proc sql;
alter table STORE.SEOUL_HHVAL
	add hh_size_jumin_group1 char(32)
		,hh_size_nhis_group1 char(32)
		, hh_size_jumin_group2 char(32)
		, hh_size_nhis_group2 char(32)
		, gaibja_type_major char(32)
		, head_or_dependent char(32)
		, hh_type_jumin char(32);
	update STORE.SEOUL_HHVAL
	set	hh_size_jumin_group1=(case
			when hh_size_jumin=. then ''
			when hh_size_jumin=1 then "1"
			when hh_size_jumin=2 or hh_size_jumin=3 then "2-3"
			else "4+" end)
		, hh_size_nhis_group1=(case
			when hh_size_nhis=. then ''
			when hh_size_nhis=1 then "1"
			when hh_size_nhis=2 or hh_size_nhis=3 then "2-3"
			else "4+" end)
		, hh_size_jumin_group2=(case
			when hh_size_jumin=. then ''
			when hh_size_jumin=1 then "1"
			else "2+" end)
		, hh_size_nhis_group2=(case
			when hh_size_nhis=. then ''
			when hh_size_nhis=1 then "1"
			else "2+" end)
		, gaibja_type_major=(case 
			when GAIBJA_TYPE="1" or GAIBJA_TYPE="2" then "  ����"
			when GAIBJA_TYPE="5" or GAIBJA_TYPE="6" then " ����"
			when GAIBJA_TYPE="7" or GAIBJA_TYPE="8" then "�Ƿ�"
			else '' end)
		, head_or_dependent=(case
			when GAIBJA_TYPE='' then ''
			when GAIBJA_TYPE="1" or GAIBJA_TYPE="5" or GAIBJA_TYPE="7" then " ������"
			else "�����" end)
		, hh_type_jumin=(case
			when hh_size_jumin=1 and age<30 then "   1�� û��"
			when hh_size_jumin=1 and age>=30 and age <55 then "  1�� 30-54��"
			when hh_size_jumin=1 and age>=55 then " 1�� 55���̻�"
			WHEN hh_size_jumin>=2 then "2�� �̻�"
			else '' end);
quit;
run;

/*---------------------------------------------------------------------------*/
/* �ֹε�� ���������� �ǰ������� ������� ����								   */
/*---------------------------------------------------------------------------*/
proc freq data=STORE.SEOUL_HHVAL(where=(STD_YYYY="2018"));
title "1-1. 2018�� ����, �ֹε�� ���������� �ǰ������� ������� ����";
tables hh_size_jumin_group1*hh_size_nhis_group1 ;
run;

proc freq data=STORE.SEOUL_HHVAL(where=(STD_YYYY="2018" and age<30));
title "1-2. 2018�� ���� 29�� ����, �ֹε�� ���������� �ǰ������� ������� ����";
tables hh_size_jumin_group1*hh_size_nhis_group1 ;
run;

proc freq data=STORE.SEOUL_HHVAL(where=(STD_YYYY="2018" and age<30 and inc_tot=0 and prop_txbs_tot=0));
title "1-3. 2018�� ���� 29�� ���� ���ҵ� �����, �ֹε�� ���������� �ǰ������� ������� ����";
tables hh_size_jumin_group1*hh_size_nhis_group1 ;
run;

proc freq data=STORE.SEOUL_HHVAL(where=(STD_YYYY="2018" and age>=55));
title "1-4. 2018�� ���� 55�� �̻�, �ֹε�� ���������� �ǰ������� ������� ����";
tables hh_size_jumin_group1*hh_size_nhis_group1 ;
run;

proc freq data=STORE.SEOUL_HHVAL(where=(STD_YYYY="2018" and age>=55 and inc_tot=0));
title "1-5. 2018�� ���� 55�� �̻� ���ҵ�, �ֹε�� ���������� �ǰ������� ������� ����";
tables hh_size_jumin_group1*hh_size_nhis_group1 ;
run;

/*---------------------------------------------------------------------------*/
/* �ֹε�� ���������� �ǰ������� ���� ������ 							   */
/*---------------------------------------------------------------------------*/
