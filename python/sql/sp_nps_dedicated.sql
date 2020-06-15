USE [Churn_Growth_Dev]
GO
/****** Object:  StoredProcedure [dbo].[sp_nps_dedicated]    Script Date: 4/30/2020 9:50:47 AM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[sp_nps_dedicated]
as

/*
	this script pulls in information on the last two nps_r ratings for an account.
*/

select *,
	row_number() over (partition by account_number order by time_month_key_dt desc) as rn
into #rn
from (
		select distinct b.*
		from [customerretention].[dbo].[base_dedicated] a
		inner join [customerretention].[dbo].[zz_staging_nps_r] b 
		on a.account_number = b.account_number 
			and b.time_month_key_dt between dateadd(month, -11, a.time_month_key_dt) and a.time_month_key_dt
			and lower(b.account_type)='dedicated'
		) a
;

drop table [customerretention].[dbo].[zz_staging_nps_01_dedicated]
select distinct a.account_name
	, a.account_number
	, a.account_type
	, a.time_month_key
	, a.time_month_key_dt
	, a.revenue_segment
	, a.account_segment
	, a.churn_flag
	, b.time_month_key_dt as nps_time_month_key_dt
	, b.survey_score
	, b.survey_rating
	, c.rn
into [customerretention].[dbo].[zz_staging_nps_01_dedicated]
from [customerretention].[dbo].[base_dedicated] a
left join 
(
	select account_number, survey_score, survey_rating, time_month_key, time_month_key_dt
	from [customerretention].[dbo].[zz_staging_nps_r] 
	where lower(account_type)='dedicated'
) b 
on a.account_number = b.account_number 
and b.time_month_key_dt between dateadd(month, -11, a.time_month_key_dt) and a.time_month_key_dt
left join #rn c
on b.account_number = c.account_number
	and b.time_month_key = c.time_month_key
order by a.account_number, a.time_month_key_dt, b.time_month_key_dt
		

	
drop table [customerretention].[dbo].[zz_staging_nps_02_dedicated]
select distinct a.account_name
	, a.account_number
	, a.account_type
	, a.time_month_key
	, a.time_month_key_dt
	, a.revenue_segment
	, a.account_segment
	, a.churn_flag
	, b.survey_score as last_survey_score 
	, b.survey_rating as last_survey_rating
	, datediff(month, b.nps_time_month_key_dt, a.time_month_key_dt) as months_since_last_survey
into [customerretention].[dbo].[zz_staging_nps_02_dedicated]
from [customerretention].[dbo].[base_dedicated] a
		left join (
	select a.*
	from [customerretention].[dbo].[zz_staging_nps_01_dedicated] a
	inner join (
				select account_number,
					time_month_key,
					rn,
					survey_score = min(survey_score)
				from [customerretention].[dbo].[zz_staging_nps_01_dedicated]  
				group by account_number,
					time_month_key,
					rn
				) b
	on a.account_number = b.account_number
		and a.time_month_key = b.time_month_key
		and a.rn = b.rn
		and a.survey_score = b.survey_score
	) b
on a.account_number = b.account_number 
and a.time_month_key = b.time_month_key 
and b.rn = 1




drop table [customerretention].[dbo].[zz_staging_nps_03_dedicated]
select a.account_name
	, a.account_number
	, a.account_type
	, a.time_month_key
	, a.time_month_key_dt
	, a.revenue_segment
	, a.account_segment
	, a.churn_flag
	, b.survey_score as second_last_survey_score 
	, b.survey_rating as second_last_survey_rating
	, datediff(month, b.nps_time_month_key_dt, a.time_month_key_dt) as months_since_second_last_survey
into [customerretention].[dbo].[zz_staging_nps_03_dedicated]
from [customerretention].[dbo].[base_dedicated] a
left join (
	select a.*
	from [customerretention].[dbo].[zz_staging_nps_01_dedicated] a
	inner join (
				select account_number,
					time_month_key,
					rn,
					survey_score = min(survey_score)
				from [customerretention].[dbo].[zz_staging_nps_01_dedicated]  
				group by account_number,
					time_month_key,
					rn
				) b
	on a.account_number = b.account_number
		and a.time_month_key = b.time_month_key
		and a.rn = b.rn
		and a.survey_score = b.survey_score
	) b
on a.account_number = b.account_number 
and a.time_month_key = b.time_month_key 
and b.rn = 2



drop table [customerretention].[dbo].[zz_staging_nps_04_dedicated]
select a.account_name
	, a.account_number
	, a.account_type
	, a.time_month_key
	, a.time_month_key_dt
	, a.revenue_segment
	, a.account_segment
	, a.churn_flag
--	, coalesce(cast(last_survey_score as varchar), '')  as last_survey_score --AW 4/30/2020 removed coalesce to allow nulls
	, last_survey_score
	, coalesce(last_survey_rating, '')  as last_survey_rating
--	, coalesce(cast(second_last_survey_score as varchar), '')  as second_last_survey_score --AW 4/30/2020 removed coalesce to allow nulls
	, second_last_survey_score
	, coalesce(second_last_survey_rating, '')  as second_last_survey_rating
--	, coalesce(last_survey_score - second_last_survey_score, '') as change_in_survey_score --AW 4/30/2020 removed coalesce to allow nulls
	,last_survey_score - second_last_survey_score as change_in_survey_score
	, case when lower(second_last_survey_rating) = 'promoter' and lower(last_survey_rating) = 'promoter' then 1 else 0 end as rating_promoter_to_promoter
	, case when lower(second_last_survey_rating) = 'promoter' and lower(last_survey_rating) = 'passive' then 1 else 0 end as rating_promoter_to_passive
	, case when lower(second_last_survey_rating) = 'promoter' and lower(last_survey_rating) = 'detractor' then 1 else 0 end as rating_promoter_to_detractor
	, case when lower(second_last_survey_rating) = 'passive' and lower(last_survey_rating) = 'promoter' then 1 else 0 end as rating_passive_to_promoter
	, case when lower(second_last_survey_rating) = 'passive' and lower(last_survey_rating) = 'passive' then 1 else 0 end as rating_passive_to_passive
	, case when lower(second_last_survey_rating) = 'passive' and lower(last_survey_rating) = 'detractor' then 1 else 0 end as rating_passive_to_detractor
	, case when lower(second_last_survey_rating) = 'detractor' and lower(last_survey_rating) = 'promoter' then 1 else 0 end as rating_detractor_to_promoter
	, case when lower(second_last_survey_rating) = 'detractor' and lower(last_survey_rating) = 'passive' then 1 else 0 end as rating_detractor_to_passive
	, case when lower(second_last_survey_rating) = 'detractor' and lower(last_survey_rating) = 'detractor' then 1 else 0 end as rating_detractor_to_detractor
into [customerretention].[dbo].[zz_staging_nps_04_dedicated]
from [customerretention].[dbo].[base_dedicated] a
left join [customerretention].[dbo].[zz_staging_nps_02_dedicated] b 
on a.account_number = b.account_number 
	and a.time_month_key = b.time_month_key 
left join [customerretention].[dbo].[zz_staging_nps_03_dedicated] c 
on a.account_number = c.account_number 
	and a.time_month_key = c.time_month_key 
;
