


SELECT SUM(convert(BIGINT, playtime_forever)) 
FROM games_daily GD
GROUP BY appid


DECLARE @oldest_retrieval_date DATETIME = (SELECT MIN(dateretrieved) FROM games_daily);
DECLARE @newest_retrieval_date DATETIME = (SELECT MAX(dateretrieved) FROM games_daily);


select @oldest_retrieval_date AS oldest, @newest_retrieval_date AS newest

--SELECT COUNT(*) FROM (SELECT DISTINCT steamid, appid FROM games_2) AS t;
--SELECT COUNT(*) FROM games_2;
GO

select avg(playtime_forever) from games_2 where appid = 71310
select * from app_id_info where appid = 71310
select * from app_id_info_old where appid = 71310



-- Question 3 (Max Playtime - Single User, Single Game)
SELECT TOP 5 G2.steamid, AII.Title, G2.playtime_forever
FROM games_2 G2
INNER JOIN app_id_info AII
		ON AII.appid = G2.appid
INNER JOIN player_summaries PS
		ON PS.steamid = G2.steamid
ORDER BY G2.playtime_forever DESC

-- Question 4 (Total Playtime - User)
WITH user_sum AS (
	SELECT G2.steamid, SUM(CONVERT(BIGINT, G2.playtime_forever)) AS total_playtime
    FROM games_2 G2
	WHERE G2.playtime_forever IS NOT NULL
	GROUP BY G2.steamid
) 
SELECT TOP 5 US.steamid, US.total_playtime
FROM user_sum US
INNER JOIN player_summaries PS
		ON PS.steamid = US.steamid
ORDER BY US.total_playtime DESC

-- Question 2 

SELECT *
FROM games_2 G2
GROUP BY appid

select distinct appid from games_2_r3p
except
select appid from app_id_info_r3p


EXEC sp_Problem1_r3pg
EXEC sp_Problem2_r3pg
EXEC sp_Problem3_r3pg
EXEC sp_Problem4_r3pg
EXEC sp_Problem5_r3pg
EXEC sp_Problem6_r3pg
EXEC sp_Problem7_r3pg
EXEC sp_Problem8_r3pg

-- this is my actual account
select * from player_summaries where steamid = 76561198082908441


EXEC sp_Problem6_r3pg


select * from app_id_info_r3pg

select * from player_summaries_r3pg


select count(*) from app_id_info_r3pg
select count(*) from games_1_r3pg
select count(*) from games_2_r3pg
select * from player_summaries_r3pg

SELECT STRING_AGG(COLUMN_NAME, ',')
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'player_summaries_r3pg'
GROUP BY TABLE_SCHEMA