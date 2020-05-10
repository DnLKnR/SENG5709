USE steam
GO

CREATE OR ALTER PROCEDURE sp_Problem6
AS
BEGIN

-- Question 6 (Average Amount of Users per ESRB) -- Note: This query returns Top 5
SELECT AII.Required_Age, COUNT(G2.steamid) as user_count
FROM app_id_info AII
INNER JOIN games_2 G2
		ON G2.appid = AII.appid
WHERE AII.Type = 'game'
GROUP BY AII.Required_Age

END
GO

CREATE OR ALTER PROCEDURE sp_Problem6_r3p
AS
BEGIN

-- Question 6 (Average Amount of Users per ESRB) -- Note: This query returns Top 5
SELECT AII.Required_Age, COUNT(G2.steamid) as user_count
FROM app_id_info_r3p AII
INNER JOIN games_2_r3p G2
		ON G2.appid = AII.appid
WHERE AII.Type = 'game'
GROUP BY AII.Required_Age

END
GO

CREATE OR ALTER PROCEDURE sp_Problem6_r3pg
AS
BEGIN

-- Question 6 (Average Amount of Users per ESRB) -- Note: This query returns Top 5
SELECT AII.Required_Age, COUNT(G2.steamid) as user_count
FROM app_id_info_r3pg AII
INNER JOIN games_2_r3pg G2
		ON G2.appid = AII.appid
GROUP BY AII.Required_Age

END
GO

CREATE OR ALTER PROCEDURE sp_Problem6_r5pg
AS
BEGIN

-- Question 6 (Average Amount of Users per ESRB) -- Note: This query returns Top 5
SELECT AII.Required_Age, COUNT(G2.steamid) as user_count
FROM app_id_info_r5pg AII
INNER JOIN games_2_r5pg G2
		ON G2.appid = AII.appid
GROUP BY AII.Required_Age

END
GO



CREATE OR ALTER PROCEDURE sp_Problem6_r3p
AS
BEGIN

-- Question 6 (Average Amount of Users per ESRB) -- Note: This query returns Top 5
SELECT AII.Required_Age, COUNT(G2.steamid) as user_count
FROM app_id_info_r3p AII
INNER JOIN games_2_r3p G2
		ON G2.appid = AII.appid
WHERE AII.Type = 'game'
GROUP BY AII.Required_Age


SELECT count(*)
FROM app_id_info_r3p AII
INNER JOIN games_2_r3p G2
		ON G2.appid = AII.appid
WHERE AII.Required_Age = 0
	  AND AII.Type = 'game'

select * from app_id_info_r3p


select distinct appid from 
END
GO