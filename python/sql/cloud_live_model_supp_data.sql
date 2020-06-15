 select	cast(a.[Account_Number] as int) as account_number
 , a.Account_Name
 , d.Account_Team_Name
 , a.time_month_key
 , a.[Total_Invoiced_In_Month]
 , [Keizan_Cloud_AM] as TAM
from
[CustomerRetention].[dbo].[AMT_FINAL_AMT_cloud_live_allvariables] a
left outer join
	(
		select *
		from
		(
			SELECT *,  rowno = row_number() over(partition by account_number order by rec desc)
			from (
				select max([REC_UPDATED]) as rec, Account_Name, cast([Account_Number] as int) as account_number, Account_Team_Name, Account_Manager
				FROM [CustomerRetention].[dbo].[Account_Data_All_DW] with (nolock)
				where [Account_Source_System_Name] in ('HostingMatrix','HostingMatrix_UK')
				group by [Account_Name], Account_Number, Account_Team_Name, Account_Manager
				) c
		) b
		where rowno=1
	) d
on a.Account_Number=d.Account_Number

left join
[480072-EA].[Report_Tables].[dbo].[Historical_Keizan_Account_Facts_Global] T with (nolock)
on A.account_number = cast(t.[Keizan_Cloud_DDI] as int) and a.Time_Month_Key=t.time_month_key

order by Account_Number
