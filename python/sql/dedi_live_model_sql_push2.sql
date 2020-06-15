BULK INSERT [CustomerRetention].[dbo].[Dedicated_Scores] 
FROM 'c:/python scripts/churn_dedi_probabilities.txt' 
WITH (FIELDTERMINATOR = '\t',ROWTERMINATOR = '\n')
