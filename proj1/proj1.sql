-- Before running drop any existing views
DROP VIEW IF EXISTS q0;
DROP VIEW IF EXISTS q1i;
DROP VIEW IF EXISTS q1ii;
DROP VIEW IF EXISTS q1iii;
DROP VIEW IF EXISTS q1iv;
DROP VIEW IF EXISTS q2i;
DROP VIEW IF EXISTS q2ii;
DROP VIEW IF EXISTS q2iii;
DROP VIEW IF EXISTS q3i;
DROP VIEW IF EXISTS q3ii;
DROP VIEW IF EXISTS q3iii;
DROP VIEW IF EXISTS q4i;
DROP VIEW IF EXISTS q4ii;
DROP VIEW IF EXISTS q4iii;
DROP VIEW IF EXISTS q4iv;
DROP VIEW IF EXISTS q4v;

-- Question 0

CREATE VIEW q0(era) AS
SELECT max(era)
FROM pitching ;

-- Question 1i

CREATE VIEW q1i(namefirst, namelast, birthyear) AS
SELECT namefirst,
       namelast,
       birthyear
FROM people
WHERE weight > 300;

-- Question 1ii

CREATE VIEW q1ii(namefirst, namelast, birthyear) AS
SELECT namefirst,
       namelast,
       birthyear
FROM people
WHERE namefirst LIKE '% %'
ORDER BY namefirst,
         namelast;

-- Question 1iii

CREATE VIEW q1iii(birthyear, avgheight, count) AS
SELECT birthyear,
       AVG(height),
       COUNT(*)
FROM people
GROUP BY birthyear
ORDER BY birthyear;

-- Question 1iv

CREATE VIEW q1iv(birthyear, avgheight, count) AS
SELECT *
FROM q1iii
WHERE avgheight > 70;

-- Question 2i

CREATE VIEW q2i(namefirst, namelast, playerid, yearid) AS
SELECT namefirst,
       namelast,
       p.playerid,
       yearid
FROM people AS p,
     halloffame AS h 
ON p.playerid = h.playerid
WHERE inducted = 'Y'
ORDER BY yearid DESC,
         p.playerid;

-- Question 2ii

CREATE VIEW q2ii(namefirst, namelast, playerid, schoolid, yearid) AS
SELECT q.namefirst,
       q.namelast,
       q.playerid,
       s.schoolid,
       q.yearid
FROM q2i AS q,
     schools AS s,
     collegeplaying AS c 
ON s.schoolid = c.schoolid 
AND q.playerid = c.playerid
WHERE schoolstate = 'CA'
ORDER BY q.yearid DESC,
         s.schoolid,
         q.playerid;

-- Question 2iii

CREATE VIEW q2iii(playerid, namefirst, namelast, schoolid) AS
SELECT q.playerid,
       q.namefirst,
       q.namelast,
       c.schoolid
FROM q2i AS q
LEFT OUTER JOIN collegeplaying AS c 
ON q.playerid = c.playerid
ORDER BY q.playerid DESC,
         c.schoolid;

-- Question 3i

CREATE VIEW q3i(playerid, namefirst, namelast, yearid, slg) AS
SELECT 1,
       1,
       1,
       1,
       1 -- replace this line
;

-- Question 3ii

CREATE VIEW q3ii(playerid, namefirst, namelast, lslg) AS
SELECT 1,
       1,
       1,
       1 -- replace this line
;

-- Question 3iii

CREATE VIEW q3iii(namefirst, namelast, lslg) AS
SELECT 1,
       1,
       1 -- replace this line
;

-- Question 4i

CREATE VIEW q4i(yearid, MIN, MAX, AVG) AS
SELECT 1,
       1,
       1,
       1 -- replace this line
;

-- Question 4ii

CREATE VIEW q4ii(binid, low, high, COUNT) AS
SELECT 1,
       1,
       1,
       1 -- replace this line
;

-- Question 4iii

CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff) AS
SELECT 1,
       1,
       1,
       1 -- replace this line
;

-- Question 4iv

CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid) AS
SELECT 1,
       1,
       1,
       1,
       1 -- replace this line
;

-- Question 4v

CREATE VIEW q4v(team, diffavg) AS
SELECT 1,
       1 -- replace this line
;