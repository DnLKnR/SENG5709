
CREATE OR ALTER PROCEDURE sp_Problem1
AS
BEGIN
-- Question 1 (Average Playtime - Single Game) -- Note: This query returns Top 5
WITH apps_averages AS (
	SELECT G2.appid, 
		   AVG(CONVERT(BIGINT, G2.playtime_forever)) AS avg_playtime
    FROM games_2 G2
	GROUP BY G2.appid
) 
SELECT TOP 5 AA.appid, AII.Title, AA.avg_playtime
FROM apps_averages AA
LEFT JOIN app_id_info AII
		ON AII.appid = AA.appid
ORDER BY AA.avg_playtime DESC
END
GO

CREATE OR ALTER PROCEDURE sp_Problem1_r3p
AS
BEGIN
-- Question 1 (Average Playtime - Single Game) -- Note: This query returns Top 5
WITH apps_averages AS (
	SELECT G2.appid, 
		   AVG(CONVERT(BIGINT, G2.playtime_forever)) AS avg_playtime
    FROM games_2_r3p G2
	GROUP BY G2.appid
) 
SELECT TOP 5 AA.appid, AII.Title, AA.avg_playtime
FROM apps_averages AA
LEFT JOIN app_id_info_r3p AII
		ON AII.appid = AA.appid
ORDER BY AA.avg_playtime DESC
END
GO

CREATE OR ALTER PROCEDURE sp_Problem1_r3pg
AS
BEGIN
-- Question 1 (Average Playtime - Single Game) -- Note: This query returns Top 5
WITH apps_averages AS (
	SELECT G2.appid, 
		   AVG(CONVERT(BIGINT, G2.playtime_forever)) AS avg_playtime
    FROM games_2_r3pg G2
	GROUP BY G2.appid
) 
SELECT TOP 5 AA.appid, AII.Title, AA.avg_playtime
FROM apps_averages AA
LEFT JOIN app_id_info_r3pg AII
		ON AII.appid = AA.appid
ORDER BY AA.avg_playtime DESC
END
GO

CREATE OR ALTER PROCEDURE sp_Problem1_r5pg
AS
BEGIN
-- Question 1 (Average Playtime - Single Game) -- Note: This query returns Top 5
WITH apps_averages AS (
	SELECT G2.appid, 
		   AVG(CONVERT(BIGINT, G2.playtime_forever)) AS avg_playtime
    FROM games_2_r5pg G2
	GROUP BY G2.appid
) 
SELECT TOP 5 AA.appid, AII.Title, AA.avg_playtime
FROM apps_averages AA
LEFT JOIN app_id_info_r5pg AII
		ON AII.appid = AA.appid
ORDER BY AA.avg_playtime DESC
END
GO