*******************20200907�����ڷ�Ȯ�ο�******;

***���̺귯�������ϰ�***;
LIBNAME KMH 'D:\KMH';

**���� ���� ��ĭ��***;
**N, Y �� ��Ʈ������ �� ��찡 �ִ� ��... ���ں�ȯ�� �ʿ� ����***;

Data KMH.Sample_data_20200902_1; set KMH.Sample_data_20200902;
run; quit;

**�ϴ� �������� �ֿ� Ȯ��, �̵̽� ������ ī�װ���***;

ODS HTML;
proc freq data=KMH.Sample_data_20200902_1;
table STD_YYYY -- MCBNF_PTTN_CD/missing;
run; quit;

***��հ� �ֿ� Ȯ��***;
ODS HTML;
proc means data=KMH.Sample_data_20200902_1;
var SEX_TYPE;
run; quit;

ODS HTML;
proc means data=KMH.Sample_data_20200902_1;
var BYEAR;
run; quit;

***keep, drop ��***;

Data KMH.Sample_data_20200902_1; set KMH.Sample_data_20200902;
keep SEX_TYPE; 
drop SEX_TYPE;
rename SEX_TYPE=SEX_TYPEgaguwon;

IF 20<=AGE<= 29 THEN AGEG=1;
ELSE IF 30<=AGE<= 39 THEN AGEG=2;
ELSE IF 40<=AGE<= 49 THEN AGEG=3;
ELSE IF 50<=AGE<= 59 THEN AGEG=4;
ELSE IF 60<=AGE<= 69 THEN AGEG=5;
ELSE IF 70<=AGE<= 79 THEN AGEG=6;
ELSE IF 80<=AGE THEN AGEG=7;/*80�� �̻�*/
ELSE DELETE; 

label SEX_TYPE='����' STD_YYYY='���ؿ���';

run; quit;

***Ư�������� ��� ������ ����***;

Data KMH.Sample_data_20200902_2; set KMH.Sample_data_20200902; IF  SEX_TYPE=1;
run; quit;

***������ ���� ��ɹ��� ����***;

PROC SQL;
CREATE TABLE A2 AS
SELECT PB010, PB150, EDU3G_1, AGEG, COUNT(PB030)  FROM STD.udb_csk17p_20190412_1/*�����͹ٲ�*/
GROUP BY PB010, PB150, EDU3G_1, AGEG  
ORDER BY PB010, PB150, EDU3G_1, AGEG  ; QUIT;


/*Weight �ο�*//*ǥ������ ���߾� ������ ����ġ	NEW_WT=������ġ*ǥ����/������ġ�� */
PROC SORT DATA=STD.udb_csk17p_20190412_1/*�����͹ٲ�*/; BY PB010 PB150 AGEG EDU3G_1; RUN;
PROC MEANS DATA=STD.udb_csk17p_20190412_1/*�����͹ٲ�*/ N SUM NOPRINT;
CLASS PB010 PB150 EDU3G_1; 
VAR PB040; 
OUTPUT OUT=WT1 N(PB040)=N_WT SUM(PB040)=SUM_WT;
RUN; 
DATA WT2; SET WT1;
IF _TYPE_=7; *Ÿ�� ��ȣ�� ��� �ٲ���� �� Ȯ���ؾ� ��*;
DROP _TYPE_ _FREQ_; 
RUN;
PROC SORT DATA=STD.udb_csk17p_20190412_1/*�����͹ٲ�*/; BY PB010 PB150 EDU3G_1; RUN;
PROC SORT DATA=WT2; BY PB010 PB150 EDU3G_1; RUN;
DATA STD.PSRH_RW1; 
MERGE STD.udb_csk17p_20190412_1/*�����͹ٲ�*/ WT2; BY PB010 PB150 EDU3G_1; 
NEW_WT=PB040*N_WT/SUM_WT;
RUN; 


/*�������� ���� �ٲٱ�(��������1->��������3)*/
DATA Psrh_rw1; 
SET STD.PSRH_RW1;
	edu=4-EDU3G_1;
RUN;

PROC SORT DATA=Psrh_rw1; BY PB010 PB150 AGEG edu; RUN;
PROC FREQ DATA=Psrh_rw1; BY PB010 PB150 AGEG; 
	TABLES edu/OUTCUM OUT=CUM1; RUN;

DATA EDUSEP; SET CUM1;
	DIFF = CUM_PCT-PERCENT;
	EDUSEP= ((PERCENT/2)+DIFF)/100;  *ridit score;
	KEEP PB010 PB150 AGEG edu EDUSEP; 
RUN;

PROC SORT DATA=Psrh_rw1; BY PB010 PB150 AGEG edu; RUN;
PROC SORT DATA=EDUSEP; BY PB010 PB150 AGEG edu; RUN;
DATA STD.DATA1; MERGE Psrh_rw1 EDUSEP; BY PB010 PB150 AGEG edu; RUN;

ODS HTML;
PROC FREQ DATA=STD.DATA1; TABLES PB010*PB150*AGEG*EDUSEP/LIST; RUN;


/* PSU�� KSTRATA�� 1���� �ο�*/
DATA STD.DATA1;SET STD.DATA1;
	PSU=1; KSTRATA = 1; RUN;
