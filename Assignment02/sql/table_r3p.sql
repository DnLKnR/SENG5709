
-- This is the Games2 subset table (random 3% of the original games_2)
SELECT TOP 3 PERCENT *
INTO games_2_r3p
FROM games_2;


SELECT TOP 3 PERCENT G2.*
INTO games_2_r3pg
FROM games_2 G2
INNER JOIN app_id_info AII
		ON AII.appid = G2.appid
INNER JOIN player_summaries PS
		ON PS.steamid = G2.steamid
WHERE AII.Type = 'game'


select distinct Type from app_id_info

select count(*) from app_id_info

select * from player_summaries_r3p


select count(*) from games_2_r3p
select count(*) from games_2_r5pg

-- This is the AppInfo subset table (games that fall into the random 3% of games_2)
SELECT AII.*
INTO app_id_info_r3pg
FROM app_id_info AII
INNER JOIN (SELECT DISTINCT appid FROM games_2_r3pg) R5PG
		ON R5PG.appid = AII.appid
	
-- This is the Player Summaries subset table (players that fall into the random 3% of games_2)
SELECT PS.*
INTO player_summaries_r3pg
FROM player_summaries PS
INNER JOIN (SELECT DISTINCT steamid FROM games_2_r3pg) R5PG
		ON R5PG.steamid = PS.steamid

-- select from G1 that is based on games and players we have in our subsets
SELECT G1.*
INTO games_1_r3pg
FROM games_1 G1
INNER JOIN (SELECT DISTINCT appid FROM app_id_info_r3pg) AII
		ON G1.appid = AII.appid
INNER JOIN (SELECT DISTINCT steamid FROM player_summaries_r3pg) PS
		ON G1.steamid = PS.steamid
		

-- Test should look for 678785 rows total from the CSV
select count(*) from player_summaries_r3p
select count(*) from app_id_info_r3p
select count(*) from games_2_r3p