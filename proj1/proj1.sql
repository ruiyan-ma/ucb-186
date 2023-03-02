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

DROP VIEW IF EXISTS lslg;
DROP VIEW IF EXISTS binfo;
DROP VIEW IF EXISTS bindex;
DROP VIEW IF EXISTS salary_data;
DROP VIEW IF EXISTS maxsal;

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
SELECT p.playerid,
       p.namefirst,
       p.namelast,
       b.yearid,
       (h + h2b + 2 * h3b + 3 * hr + 0.0) / (ab + 0.0) AS slgval
FROM people AS p,
     batting AS b 
ON p.playerid = b.playerid
WHERE b.ab > 50
ORDER BY slgval DESC,
         b.yearid,
         p.playerid
LIMIT 10;

-- Question 3ii

CREATE VIEW lslg(playerid, lslgval) AS
SELECT playerid,
       (SUM(h) + SUM(h2b) + 2 * SUM(h3b) + 3 * SUM(hr) + 0.0) / (SUM(ab) + 0.0)
FROM batting
GROUP BY playerid
HAVING SUM(ab) > 50;


CREATE VIEW q3ii(playerid, namefirst, namelast, lslg) AS
SELECT p.playerid,
       p.namefirst,
       p.namelast,
       l.lslgval
FROM people AS p,
     lslg AS l 
ON p.playerid = l.playerid
ORDER BY l.lslgval DESC,
         p.playerid
LIMIT 10;

-- Question 3iii

CREATE VIEW q3iii(namefirst, namelast, lslg) AS
SELECT p.namefirst,
       p.namelast,
       l.lslgval
FROM people AS p,
     lslg AS l 
ON p.playerid = l.playerid
WHERE l.lslgval >
        (SELECT lslgval
         FROM lslg
         WHERE playerid = 'mayswi01');

-- Question 4i

CREATE VIEW q4i(yearid, min, max, avg) AS
SELECT yearid,
       MIN(salary),
       MAX(salary),
       AVG(salary)
FROM salaries
GROUP BY yearid
ORDER BY yearid;

-- Question 4ii

CREATE VIEW binfo(minval, maxval, width) AS
SELECT MIN(salary),
       MAX(salary),
       (MAX(salary) - MIN(salary)) / 10
FROM salaries
WHERE yearid = 2016;


CREATE VIEW bindex(salary, bidx) AS
SELECT DISTINCT salary,
                CAST ((salary - minval) / width AS INT)
FROM salaries,
     binfo
WHERE yearid = 2016
AND salary < maxval
UNION
SELECT maxval, 9
FROM binfo;


CREATE VIEW q4ii(binid, low, high, count) AS
SELECT bidx,
       minval + bidx * width,
       minval + (bidx + 1) * width,
       COUNT(*)
FROM salaries AS s,
     bindex AS b,
     binfo 
ON s.salary = b.salary
WHERE yearid = 2016
GROUP BY bidx;

-- Question 4iii

CREATE VIEW salary_data(yearid, minsal, maxsal, avgsal) AS
SELECT yearid,
       MIN(salary),
       MAX(salary),
       AVG(salary)
FROM salaries
GROUP BY yearid;


CREATE VIEW q4iii(yearid, mindiff, maxdiff, avgdiff) AS
SELECT s1.yearid,
       s1.minsal - s2.minsal,
       s1.maxsal - s2.maxsal,
       s1.avgsal - s2.avgsal
FROM salary_data AS s1,
     salary_data AS s2 
ON s1.yearid - 1 = s2.yearid
ORDER BY s1.yearid;

-- Question 4iv

CREATE VIEW maxsal(playerid, salary, yearid) AS
SELECT playerid,
       salary,
       yearid
FROM salaries
WHERE (yearid = 2000
       AND salary =
           (SELECT MAX(s1.salary)
            FROM salaries AS s1
            WHERE s1.yearid = 2000))
    OR (yearid = 2001
        AND salary =
            (SELECT MAX(s2.salary)
             FROM salaries AS s2
             WHERE s2.yearid = 2001)) ;


CREATE VIEW q4iv(playerid, namefirst, namelast, salary, yearid) AS
SELECT p.playerid,
       p.namefirst,
       p.namelast,
       ms.salary,
       ms.yearid
FROM people AS p,
     maxsal AS ms
ON p.playerid = ms.playerid;

-- Question 4v

CREATE VIEW q4v(team, diffavg) AS
SELECT a.teamid,
       MAX(s.salary) - MIN(s.salary)
FROM salaries AS s,
     allstarfull AS a
ON s.playerid = a.playerid
AND s.yearid = a.yearid
WHERE a.yearid = 2016
GROUP BY a.teamid;
