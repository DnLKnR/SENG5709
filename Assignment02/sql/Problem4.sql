

CREATE OR ALTER PROCEDURE sp_Problem4
AS
BEGIN

WITH apps_total_user_playtime AS (
	SELECT G2.steamid, 
		   SUM(CONVERT(BIGINT, G2.playtime_forever)) AS total_playtime
    FROM games_2 G2
	GROUP BY G2.steamid
) 
SELECT TOP 5 ATUP.steamid, ATUP.total_playtime, PS.personaname, PS.loccountrycode
FROM apps_total_user_playtime ATUP
LEFT JOIN player_summaries PS
		ON PS.steamid = ATUP.steamid
ORDER BY ATUP.total_playtime DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem4_r3p
AS
BEGIN

WITH apps_total_user_playtime AS (
	SELECT G2.steamid, 
		   SUM(CONVERT(BIGINT, G2.playtime_forever)) AS total_playtime
    FROM games_2_r3p G2
	GROUP BY G2.steamid
) 
SELECT TOP 5 ATUP.steamid, ATUP.total_playtime, PS.personaname, PS.loccountrycode
FROM apps_total_user_playtime ATUP
LEFT JOIN player_summaries_r3p PS
		ON PS.steamid = ATUP.steamid
ORDER BY ATUP.total_playtime DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem4_r3pg
AS
BEGIN

WITH apps_total_user_playtime AS (
	SELECT G2.steamid, 
		   SUM(CONVERT(BIGINT, G2.playtime_forever)) AS total_playtime
    FROM games_2_r3pg G2
	GROUP BY G2.steamid
) 
SELECT TOP 5 ATUP.steamid, ATUP.total_playtime, PS.personaname, PS.loccountrycode
FROM apps_total_user_playtime ATUP
LEFT JOIN player_summaries_r3pg PS
		ON PS.steamid = ATUP.steamid
ORDER BY ATUP.total_playtime DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem4_r5pg
AS
BEGIN

WITH apps_total_user_playtime AS (
	SELECT G2.steamid, 
		   SUM(CONVERT(BIGINT, G2.playtime_forever)) AS total_playtime
    FROM games_2_r5pg G2
	GROUP BY G2.steamid
) 
SELECT TOP 5 ATUP.steamid, ATUP.total_playtime, PS.personaname, PS.loccountrycode
FROM apps_total_user_playtime ATUP
LEFT JOIN player_summaries_r5pg PS
		ON PS.steamid = ATUP.steamid
ORDER BY ATUP.total_playtime DESC

END
GO