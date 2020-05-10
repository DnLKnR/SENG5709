USE steam
GO

CREATE OR ALTER PROCEDURE sp_Problem3
AS
BEGIN

SELECT TOP 5 G2.appid, AII.Title, G2.steamid, PS.personaname, G2.playtime_forever
FROM games_2 G2
LEFT JOIN app_id_info AII
		ON AII.appid = G2.appid
LEFT JOIN player_summaries PS
		ON PS.steamid = G2.steamid
ORDER BY G2.playtime_forever DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem3_r3p
AS
BEGIN

SELECT TOP 5 G2.appid, AII.Title, G2.steamid, PS.personaname, G2.playtime_forever
FROM games_2_r3p G2
LEFT JOIN app_id_info_r3p AII
		ON AII.appid = G2.appid
LEFT JOIN player_summaries_r3p PS
		ON PS.steamid = G2.steamid
ORDER BY G2.playtime_forever DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem3_r3pg
AS
BEGIN

SELECT TOP 5 G2.appid, AII.Title, G2.steamid, PS.personaname, G2.playtime_forever
FROM games_2_r3pg G2
LEFT JOIN app_id_info_r3pg AII
		ON AII.appid = G2.appid
LEFT JOIN player_summaries_r3pg PS
		ON PS.steamid = G2.steamid
ORDER BY G2.playtime_forever DESC

END
GO

CREATE OR ALTER PROCEDURE sp_Problem3_r5pg
AS
BEGIN

SELECT TOP 5 G2.appid, AII.Title, G2.steamid, PS.personaname, G2.playtime_forever
FROM games_2_r5pg G2
LEFT JOIN app_id_info_r5pg AII
		ON AII.appid = G2.appid
LEFT JOIN player_summaries_r5pg PS
		ON PS.steamid = G2.steamid
ORDER BY G2.playtime_forever DESC

END
GO