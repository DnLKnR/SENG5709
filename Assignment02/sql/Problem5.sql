CREATE OR ALTER PROCEDURE sp_Problem5
AS
BEGIN

WITH total_hours_played AS (
	SELECT G2.appid
		   , SUM(CONVERT(DECIMAL, G2.playtime_forever)) as total_playtime
	       , AVG(CONVERT(DECIMAL, G2.playtime_forever)) as avg_playtime
	FROM games_2 G2
	GROUP BY G2.appid
)
SELECT TOP 5 AII.appid, 
			 AII.Title, 
			 THP.total_playtime / THP.avg_playtime as playtime_ratio
FROM app_id_info AII
INNER JOIN total_hours_played THP
		ON THP.appid = AII.appid
WHERE THP.avg_playtime > 0
ORDER BY playtime_ratio DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem5_r3p
AS
BEGIN

WITH total_hours_played AS (
	SELECT G2.appid
		   , SUM(CONVERT(DECIMAL, G2.playtime_forever)) as total_playtime
	       , AVG(CONVERT(DECIMAL, G2.playtime_forever)) as avg_playtime
	FROM games_2_r3p G2
	GROUP BY G2.appid
)
SELECT TOP 5 AII.appid, 
			 AII.Title, 
			 THP.total_playtime / THP.avg_playtime as playtime_ratio
FROM app_id_info_r3p AII
INNER JOIN total_hours_played THP
		ON THP.appid = AII.appid
WHERE THP.avg_playtime > 0
ORDER BY playtime_ratio DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem5_r3pg
AS
BEGIN

WITH total_hours_played AS (
	SELECT G2.appid
		   , SUM(CONVERT(DECIMAL, G2.playtime_forever)) as total_playtime
	       , AVG(CONVERT(DECIMAL, G2.playtime_forever)) as avg_playtime
	FROM games_2_r3pg G2
	GROUP BY G2.appid
)
SELECT TOP 5 AII.appid, 
			 AII.Title, 
			 THP.total_playtime / THP.avg_playtime as playtime_ratio
FROM app_id_info_r3pg AII
INNER JOIN total_hours_played THP
		ON THP.appid = AII.appid
WHERE THP.avg_playtime > 0
ORDER BY playtime_ratio DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem5_r5pg
AS
BEGIN

WITH total_hours_played AS (
	SELECT G2.appid
		   , SUM(CONVERT(DECIMAL, G2.playtime_forever)) as total_playtime
	       , AVG(CONVERT(DECIMAL, G2.playtime_forever)) as avg_playtime
	FROM games_2_r5pg G2
	GROUP BY G2.appid
)
SELECT TOP 5 AII.appid, 
			 AII.Title, 
			 THP.total_playtime / THP.avg_playtime as playtime_ratio
FROM app_id_info_r5pg AII
INNER JOIN total_hours_played THP
		ON THP.appid = AII.appid
WHERE THP.avg_playtime > 0
ORDER BY playtime_ratio DESC

END
GO