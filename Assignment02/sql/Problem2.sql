

CREATE OR ALTER PROCEDURE sp_Problem2
AS
BEGIN

-- Question 2 (Most Users - Single Game) -- Note: This query returns Top 5
WITH apps_player_counts AS (
	SELECT G2.appid, 
		   COUNT(G2.steamid) AS player_count
    FROM games_2 G2
	GROUP BY G2.appid
) 
SELECT TOP 5 APC.appid, AII.Title, APC.player_count
FROM apps_player_counts APC
LEFT JOIN app_id_info AII
		ON AII.appid = APC.appid
ORDER BY APC.player_count DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem2_r3p
AS
BEGIN

-- Question 2 (Most Users - Single Game) -- Note: This query returns Top 5
WITH apps_player_counts AS (
	SELECT G2.appid, 
		   COUNT(G2.steamid) AS player_count
    FROM games_2_r3p G2
	GROUP BY G2.appid
) 
SELECT TOP 5 APC.appid, AII.Title, APC.player_count
FROM apps_player_counts APC
LEFT JOIN app_id_info_r3p AII
		ON AII.appid = APC.appid
ORDER BY APC.player_count DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem2_r3pg
AS
BEGIN

-- Question 2 (Most Users - Single Game) -- Note: This query returns Top 5
WITH apps_player_counts AS (
	SELECT G2.appid, 
		   COUNT(G2.steamid) AS player_count
    FROM games_2_r3pg G2
	GROUP BY G2.appid
) 
SELECT TOP 5 APC.appid, AII.Title, APC.player_count
FROM apps_player_counts APC
LEFT JOIN app_id_info_r3pg AII
		ON AII.appid = APC.appid
ORDER BY APC.player_count DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem2_r5pg
AS
BEGIN

-- Question 2 (Most Users - Single Game) -- Note: This query returns Top 5
WITH apps_player_counts AS (
	SELECT G2.appid, 
		   COUNT(G2.steamid) AS player_count
    FROM games_2_r5pg G2
	GROUP BY G2.appid
) 
SELECT TOP 5 APC.appid, AII.Title, APC.player_count
FROM apps_player_counts APC
LEFT JOIN app_id_info_r5pg AII
		ON AII.appid = APC.appid
ORDER BY APC.player_count DESC

END
GO