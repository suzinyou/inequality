
proc sql;
create table &region._old_age as
select std_yyyy, hhrr_head_indi_dscm_no, sum(indi_age>=62) as num_older_adults
from store.&region._eq1
where std_yyyy in ("2006", "2018")
group by std_yyyy, hhrr_head_indi_dscm_no;
quit;

proc sql;
create table tmp as
select a.std_yyyy, a.hhrr_head_indi_dscm_no
	, a.hh_size
	, a.age
	, a.inc_wage
	, a.inc_bus
	, a.inc_fin+a.inc_othr as inc_fin_othr
	, a.inc_pnsn
	, a.inc_tot
	, b.num_older_adults
from store.&region._hh1 as a
inner join &region._old_age as b /*only want 2006 & 2018*/
	on a.std_yyyy=b.std_yyyy and a.hhrr_head_indi_dscm_no=b.hhrr_head_indi_dscm_no;
quit;

proc rank data=tmp(where=(hh_size>1)) out=tmp_ranked groups=5 ties=low;
var inc_tot;
by std_yyyy;
ranks rnk;
run;

proc sql;
create table x as
select std_yyyy, rnk+1 as rank
	, mean(hh_size)
	, mean(num_older_adults)
	, mean(inc_wage)
	/*...*/
	, mean(inc_tot)
	, median(inc_tot)
from tmp_ranked
group by std_yyyy, rnk;
quit;
/* do the same without grouping by rnk */

proc rank data=tmp(where=(hh_size=1)) out=tmp_ranked groups=5 ties=low;
var inc_tot;
by std_yyyy;
run;

proc sql;
create table x as
select std_yyyy, rnk+1 as rank
	, sum(age>62)/count(*) as frac_older_adults
	, mean(inc_wage)
	/*...*/
	, mean(inc_tot)
	, median(inc_tot)
from tmp_ranked
group by std_yyyy, rnk;
quit;
/* do the same without grouping by rnk */