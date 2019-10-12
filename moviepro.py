import sqlite3 as lite
import csv
import re
import pandas
from sqlalchemy import create_engine
con = lite.connect('cs1656.sqlite')

with con:
	cur = con.cursor()

	########################################################################		
	### CREATE TABLES ######################################################
	########################################################################		
	# DO NOT MODIFY - START 
	cur.execute('DROP TABLE IF EXISTS Actors')
	cur.execute("CREATE TABLE Actors(aid INT, fname TEXT, lname TEXT, gender CHAR(6), PRIMARY KEY(aid))")

	cur.execute('DROP TABLE IF EXISTS Movies')
	cur.execute("CREATE TABLE Movies(mid INT, title TEXT, year INT, rank REAL, PRIMARY KEY(mid))")

	cur.execute('DROP TABLE IF EXISTS Directors')
	cur.execute("CREATE TABLE Directors(did INT, fname TEXT, lname TEXT, PRIMARY KEY(did))")

	cur.execute('DROP TABLE IF EXISTS Cast')
	cur.execute("CREATE TABLE Cast(aid INT, mid INT, role TEXT)")

	cur.execute('DROP TABLE IF EXISTS Movie_Director')
	cur.execute("CREATE TABLE Movie_Director(did INT, mid INT)")
	# DO NOT MODIFY - END

	########################################################################		
	### READ DATA FROM FILES ###############################################
	########################################################################		
	# actors.csv, cast.csv, directors.csv, movie_dir.csv, movies.csv
	# UPDATE THIS

	engine = create_engine("sqlite:///cs1656.sqlite")
	df1 = pandas.read_csv('actors.csv', header=None)
	df1.columns = ["aid", "fname", "lname", "gender"]
	df1.to_sql('actors', engine, if_exists='append', index=False)

	df2 = pandas.read_csv('cast.csv', header=None)
	df2.columns = ["aid", "mid", "role"]
	df2.to_sql('cast', engine, if_exists='append', index=False)

	df3 = pandas.read_csv('directors.csv', header=None)
	df3.columns = ["did", "fname", "lname"]
	df3.to_sql('directors', engine, if_exists='append', index=False)

	df4 = pandas.read_csv('movie_dir.csv', header=None)
	df4.columns = ["did", "mid"]
	df4.to_sql('movie_director', engine, if_exists='append', index=False)

	df5 = pandas.read_csv('movies.csv', header=None)
	df5.columns = ["mid", "title", "year", "rank"]
	df5.to_sql('movies', engine, if_exists='append', index=False)

	########################################################################
	### INSERT DATA INTO DATABASE ##########################################
	########################################################################
	# UPDATE THIS TO WORK WITH DATA READ IN FROM CSV FILES
	cur.execute("INSERT INTO Actors VALUES(1003, 'Harrison', 'Ford', 'Male')")
	cur.execute("INSERT INTO Actors VALUES(1004, 'Daisy', 'Ridley', 'Female')")

	cur.execute("INSERT INTO Movies VALUES(103, 'Star Wars VII: The Force Awakens', 2015, 8.2)")
	cur.execute("INSERT INTO Movies VALUES(104, 'Rogue One: A Star Wars Story', 2016, 8.0)")

	cur.execute("INSERT INTO Cast VALUES(1003, 101, 'Han Solo')")
	cur.execute("INSERT INTO Cast VALUES(1004, 101, 'Rey')")

	cur.execute("INSERT INTO Directors VALUES(50001, 'J.J.', 'Abrams')")

	cur.execute("INSERT INTO Movie_Director VALUES(50001, 101)")

	con.commit()



	########################################################################
	### QUERY SECTION ######################################################
	########################################################################
	queries = {}

	# DO NOT MODIFY - START
	# DEBUG: all_movies ########################
	queries['all_movies'] = '''
SELECT * FROM Movies
'''
	# DEBUG: all_actors ########################
	queries['all_actors'] = '''
SELECT * FROM Actors
'''
	# DEBUG: all_cast ########################
	queries['all_cast'] = '''
SELECT * FROM Cast
'''
	# DEBUG: all_directors ########################
	queries['all_directors'] = '''
SELECT * FROM Directors
'''
	# DEBUG: all_movie_dir ########################
	queries['all_movie_dir'] = '''
SELECT * FROM Movie_Director
'''
	# DO NOT MODIFY - END

	########################################################################
	### INSERT YOUR QUERIES HERE ###########################################
	########################################################################
	# NOTE: You are allowed to also include other queries here (e.g.,
	# for creating views), that will be executed in alphabetical order.
	# We will grade your program based on the output files q01.csv,
	# q02.csv, ..., q12.csv

	# Q01 ########################
	queries['q01'] = '''
	
	SELECT c.lname, c.fname
	FROM 
	(SELECT actors.lname, actors.fname
	FROM actors NATURAL JOIN cast NATURAL JOIN movies
	WHERE (movies.year >= 1980 AND movies.year <= 1990 )
	) as c NATURAL JOIN actors NATURAL JOIN cast NATURAL JOIN movies
	WHERE movies.year > 2000
	GROUP BY c.lname, c.fname
	ORDER BY c.lname ASC, c.fname ASC
	
	'''

	# Q02 ########################
	queries['q02'] = '''
	SELECT movies.title, movies.year
	FROM 
	(SELECT movies.year, movies.rank
	FROM movies
	WHERE (movies.title == "Rogue One: A Star Wars Story")
	) as c, movies
	WHERE ( movies.year == c.year AND movies.rank >= c.rank)
	GROUP BY movies.title
	ORDER BY movies.title ASC
'''

	# Q03 ########################
	queries['q03'] = '''
	SELECT a.lname, a.fname
	FROM (
	SELECT actors.aid, count(*) , actors.lname , actors.fname
	FROM actors NATURAL JOIN cast NATURAL JOIN movies
	WHERE movies.title LIKE "%Star Wars%"
	GROUP BY actors.aid
	ORDER BY count(*) DESC, actors.lname ASC, actors.fname ASC ) as a
'''

	# Q04 ########################
	queries['q04'] = '''
	SELECT  a.lname, a.fname
	FROM actors as a
	WHERE NOT EXISTS
	(
	SELECT *
	FROM actors as b NATURAL JOIN cast NATURAL JOIN movies
	WHERE movies.year >= 1985 AND a.lname == b.lname AND a.fname == b.fname) 
	ORDER BY a.lname, a.fname
	
'''

	# Q05 ########################
	queries['q05'] = '''

	SELECT  count(*), directors.lname, directors.fname
	FROM  movie_director NATURAL JOIN directors
	GROUP BY directors.lname, directors.fname
	ORDER BY count(*) DESC  
	LIMIT 20

	
'''

	# Q06 ########################
	queries['q06'] = '''
	SELECT movies.title, count(*)
	FROM movies NATURAL JOIN cast
	GROUP BY movies.title
	ORDER BY count(*) DESC
	LIMIT 10
'''

	# Q07 ########################
	queries['q07'] = '''
	SELECT c.title, c.a, b.a
	
	FROM (SELECT movies.title, actors.gender, count(*) as a
	FROM actors NATURAL JOIN cast NATURAL JOIN movies
	WHERE actors.gender == "Female"
	GROUP BY movies.title, actors.gender
	) as c, (SELECT movies.title, actors.gender, count(*) as a
	FROM actors NATURAL JOIN cast NATURAL JOIN movies
	WHERE actors.gender == "Male"
	GROUP BY movies.title, actors.gender) as b
	
	WHERE c.title == b.title AND c.a > b.a
	ORDER BY c.title ASC
	
	

'''

	# Q08 ########################
	queries['q08'] = '''
	SELECT  actors.fname, actors.lname, count(DISTINCT did) as c
	FROM (actors NATURAL JOIN cast NATURAL JOIN movies) JOIN (movie_dir NATURAL JOIN directors)
	WHERE movies.mid = movie_dir.mid 
	GROUP BY actors.lname, actors.fname, did
	HAVING c > 7
	ORDER BY c DESC
	
'''

	# Q09 ########################
	queries['q09'] = '''
	SELECT actors.fname, actors.lname, count(DISTINCT mid) as m
	FROM actors NATURAL JOIN cast NATURAL JOIN movies
	WHERE actors.fname LIKE "T%" 
	GROUP BY actors.lname, actors.fname, movies.year
	HAVING movies.year == MIN(movies.year) 
	ORDER BY m DESC

'''

	# Q10 ########################
	queries['q10'] = '''
	SELECT DISTINCT actors.lname, movies.title
	FROM (actors NATURAL JOIN cast NATURAL JOIN movies) JOIN (movie_dir NATURAL JOIN directors)
	WHERE movies.mid == movie_dir.mid AND directors.lname == actors.lname
	ORDER BY actors.lname ASC
'''

	# Q11 ########################
	queries['q11'] = '''
	WITH RecursiveCTE
	AS (SELECT C.aid, C.mid, 0 as Level
	FROM   CAST C JOIN ACTORS A ON A.aid = C.aid
	WHERE  A.fname = 'Kevin' and A.lname = 'Bacon' UNION ALL
	SELECT c1.aid, c2.mid, R.Level + 1
	FROM   RecursiveCTE R
	JOIN CAST c1 ON c1.mid = R.mid AND R.Level < 2
	JOIN CAST c2 ON c1.aid = c2.aid)
	
	SELECT *
	FROM   ACTORS
	WHERE  aid IN (SELECT aid 
	FROM   RecursiveCTE
	GROUP  BY aid
	HAVING MIN(Level) = 2)  
	

'''

	# Q12 ########################
	queries['q12'] = '''
	SELECT actors.fname, actors.lname, count(DISTINCT mid), AVG(movies.rank) as a
	FROM actors NATURAL JOIN cast NATURAL JOIN movies
	GROUP BY actors.lname, actors.fname
	ORDER BY a DESC
	LIMIT 20
	
'''


	########################################################################
	### SAVE RESULTS TO FILES ##############################################
	########################################################################
	# DO NOT MODIFY - START
	for (qkey, qstring) in sorted(queries.items()):
		try:
			cur.execute(qstring)
			all_rows = cur.fetchall()

			print ("=========== ",qkey," QUERY ======================")
			print (qstring)
			print ("----------- ",qkey," RESULTS --------------------")
			for row in all_rows:
				print (row)
			print (" ")

			save_to_file = (re.search(r'q0\d', qkey) or re.search(r'q1[012]', qkey))
			if (save_to_file):
				with open(qkey+'.csv', 'w') as f:
					writer = csv.writer(f)
					writer.writerows(all_rows)
					f.close()
				print ("----------- ",qkey+".csv"," *SAVED* ----------------\n")

		except lite.Error as e:
			print ("An error occurred:", e.args[0])
	# DO NOT MODIFY - END

