
/*GINI*/
%let vname = inc_tot;

proc sql;
create table work.tmp_sorted as
select INDI_DSCM_N, &vname
from sy.csv2018
order by &vname;
quit;

proc sql;
select sum(&vname) into :tot from tmp_sorted;
quit;

proc sql;
select count(*) into :N from tmp_sorted;
quit;

data work.tmp_indexed;
set work.tmp_sorted;
by &vname;
index=_n_;
poppct=index/&N;
varpct=&vname/&tot;
run;

proc sql;
create table gini as 
	select "&vname" as var
		, 2 * sum(poppct * varpct) - (&N + 1)/&N as Gini 
from tmp_indexed;
quit;
proc sql;

/*income quintile share ratio*/
proc sql;
select sum(varpct) into :top_quintile_share
from tmp_indexed 
where poppct > 0.8;
quit;

proc sql;
select sum(varpct) into :bottom_quintile_share
from tmp_indexed 
where poppct < 0.2;
quit;

%let iqsr = %sysevalf(&top_quintile_share/&bottom_quintile_share);
%put &iqsr;

/*relative poverty rate*/
proc sql;
select &vname into :median_val
from tmp_indexed
where poppct > 0.5 - 1/&N and poppct < 0.5 + 1/&N;
quit;

proc sql;
create table tmp as 
select sum(poppct) as rpr
from tmp_indexed
where &vname < &median_val / 2;
quit;