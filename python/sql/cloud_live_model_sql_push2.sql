BULK INSERT [CustomerRetention].[dbo].[cloud_Scores]
FROM 'c:/python scripts/growth_probabilities.txt'
WITH (FIELDTERMINATOR = '\t',ROWTERMINATOR = '\n')
