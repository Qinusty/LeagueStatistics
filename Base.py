#!/usr/bin/python

#############################
#https://github.com/Qinusty
##© 2015 Josh Smith
##All Rights Reserved
#############################
import pymysql
from riotwatcher import RiotWatcher
from riotwatcher import EUROPE_WEST
from datetime import datetime
import time

w = RiotWatcher('<API KEY>', default_region=EUROPE_WEST)

####################################################################

### Initialises a connection with the database.
def GetConnection():
	## initialise a connection to the database.
	connection = pymysql.connect(host='localhost',
                             user='user',
                             passwd='pwd',
                             db='db',
                             charset='utf8')
	## return the connection
	return connection

### Grabs the Match history of the summoner passed.
def match_history(summoner, Region):
    ms = w.get_match_history(summoner['id'], region=Region, begin_index=0,end_index=15)
    return ms['matches']

### Adds a user to the users table with the passed values.
def AddUser(SumName, Region):
	## initialise a new connection
	connection = GetConnection()
	try:
		cursor = connection.cursor()
		# Create a new record
		sql = "INSERT INTO `Users` (`Username`, `Region`) VALUES (%s, %s)"
		cursor.execute(sql, (SumName, Region))
		## Commit changes
		connection.commit()
	except pymysql.err.IntegrityError:
		### handle exception
	finally:
		## close the connection
		connection.close()

### Adds a match with the details passed to the Matches table.
def AddMatch(MatchID, UserID, ChampionID, Win, Kills, Assists, Deaths, MatchCreation):
	## initialise a new connection
	connection = GetConnection()
	try:
		cursor = connection.cursor()
		# Create new record
		sql = ("INSERT INTO Matches "
			"(MatchID, UserID, ChampionID, Win, Kills, Assists, Deaths, CreationTime) "
			"Values (%s,%s,%s,%s,%s,%s,%s,%s);")
		cursor.execute(sql, (MatchID, UserID, ChampionID, Win,
						Kills, Assists, Deaths, MatchCreation))
		## Commit changes
		connection.commit()
	except pymysql.err.IntegrityError:
		### handle exception
	finally:
		connection.close()

### Grabs all users from the Users table.
def GetUsers():
	## initialise a new connection
	connection = GetConnection()
	try:
		with connection.cursor() as cursor:
			# Read a single record
			cursor.execute("SELECT * FROM `Users`")
			result = cursor.fetchall()
	finally:
		## close the connection
		connection.close()
	return result

### Checks the Matches table for an idential MatchID to check if this is a new game.
def IsMatchUnique(MatchID):
	## Initialise a new connection
	connection = GetConnection()
	try:
		with connection.cursor() as cursor:
			# Read a single record
			cursor.execute("SELECT 'Win' FROM `Matches` WHERE 'MatchID' = %s", MatchID)
			result = cursor.fetchall()
	finally:
		## Close the connection
		connection.close()
	## Check if any results are returned, if so: the match is not unique.
	if len(result) > 0:
		return False
	else:
		return True

### Main Method for updating the database.
def UpdateHistory(sumName, Region, DatabaseUserID):
	s = w.get_summoner(name=sumName, region=Region)
	ms = match_history(s, Region)
	## Loop through each game checking if it has been stored in the database
	## and if not, add it to the database.
	for game in ms:
		MatchID = game['matchId']
		if IsMatchUnique(MatchID):
			CreationTime = datetime.fromtimestamp(game['matchCreation']//1000)
			kills = game['participants'][0]['stats']['kills']
			assists = game['participants'][0]['stats']['assists']
			deaths = game['participants'][0]['stats']['deaths']
			MatchID = str(game['matchId'])
			Win = game['participants'][0]['stats']['winner']
			ChampionID = game['participants'][0]['championId']
			AddMatch(MatchID, DatabaseUserID, ChampionID, Win,
					kills, assists, deaths, CreationTime)

	### Change the LastUpdated in the users table.
	connection = GetConnection()
	try:
		cursor = connection.cursor()
		# Create new record
		sql = "UPDATE Users SET LastUpdated=%s WHERE UserID='%s';"
		cursor.execute(sql, (datetime.now(), DatabaseUserID))
		connection.commit()
	finally:
		connection.close()



####################################################
## Main loop
while True:
	## Get the list of users
	Users = GetUsers()
	# Delay in seconds, 5 minutes
	Delay = 5*60
	for user in Users:
		### User Format : {UserID, Username, Region, CreationTime}
		print('Updating: ', user[1], ' On ', user[2])
		## Update that individual user.
		UpdateHistory(user[1], EUROPE_WEST, user[0])
		print('\n')
	print("Pausing for: ", str(Delay/60), " Minutes.")
	time.sleep(Delay) ## its in seconds
