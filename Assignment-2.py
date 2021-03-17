import mysql.connector
import csv
import re
from mysql.connector import errorcode

cnx = mysql.connector.connect(user='root',
                              password='root',
                              host='127.0.0.1',
                              )
DB_NAME = 'games'


cursor = cnx.cursor()
# Function that creates the database
def create_database(cursor, DB_NAME):
    try:
        cursor.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
# Function that creates the table for game_developers
def create_table_game_developers(cursor):
    create_game_developers = "CREATE TABLE `game_developers` (" \
                 "  `studio_name` varchar(40) NOT NULL," \
                 "  `founded` varchar(40) NOT NULL," \
                 "  `founder` varchar(80) NOT NULL," \
                 "  `headquarters` varchar(40) NOT NULL," \
                 "  `type` varchar(100) NOT NULL," \
                 "  `country` varchar(40) NOT NULL," \
                 "  PRIMARY KEY (`studio_name`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating game developers: ")
        cursor.execute(create_game_developers)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print(error_alrexi)
        else:
            print(err.msg)
    else:
        print("OK")                
# Function that creates the table for multiplayer_games                 
def create_table_multiplayer_games(cursor):
    create_multiplayer_games = "CREATE TABLE `multiplayer_games` (" \
                 "  `title` varchar(40) NOT NULL," \
                 "  `genre` varchar(40) NOT NULL," \
                 "  `game_developer` varchar(40) NOT NULL," \
                 "  `publisher` varchar(40) NOT NULL," \
                 "  `release_year` varchar(100) NOT NULL," \
                 "  `sold_copies` varchar(40) NOT NULL," \
                 "  `review` varchar(10) NOT NULL," \
                 "  PRIMARY KEY (`title`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating multiplayer games: ")
        cursor.execute(create_multiplayer_games)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print(error_alrexi)
        else:
            print(err.msg)
    else:
        print("OK")
# Function that creates the table for singleplayer_games
def create_table_singleplayer_games(cursor):
    create_singleplayer_games = "CREATE TABLE `singleplayer_games` (" \
                 "  `title` varchar(40) NOT NULL," \
                 "  `genre` varchar(40) NOT NULL," \
                 "  `game_developer` varchar(40) NOT NULL," \
                 "  `publisher` varchar(40) NOT NULL," \
                 "  `release_year` varchar(100) NOT NULL," \
                 "  `sold_copies` varchar(40) NOT NULL," \
                 "  `review` varchar(100) NOT NULL," \
                 "  PRIMARY KEY (`title`)" \
                 ") ENGINE=InnoDB"

    try:
        print("Creating singleplayer games: ")
        cursor.execute(create_singleplayer_games)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print(error_alrexi)
        else:
            print(err.msg)
    else:
        print("OK")
# insert_into_game_developers imports the contents from the game_developers.csv file
# and inserts them into the table game_developers
def insert_into_game_developers(cursor):
    insert_sql_game_developers = []
    with open("game_developers.csv", newline='', encoding="utf-8") as file:
        next(file) # Skips the first row in the .csv file
        reader = csv.reader(file)
        for row in reader:
            gamedev_tuple = tuple(row)
            gamedev_str = str(gamedev_tuple)
            game_developers = ("INSERT INTO game_developers (studio_name, founded, founder, headquarters, type, country)"
                      "VALUES "  + gamedev_str + ";")
            insert_sql_game_developers.append(game_developers)
                    
    for query in insert_sql_game_developers:
        try:
            print("SQL query {}: ".format(query), end='')
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            cnx.commit()
            print("OK")
# insert_into_singleplayer_games imports the contents from the singleplayer_games.csv file
# and inserts them into the table singleplayer_games
def insert_into_singleplayer_games(cursor):
    insert_sql_singleplayer_games = []
    with open("singleplayer_games.csv", newline='', encoding="utf=8") as file:
        next(file) # Skips the first row in the .csv file
        reader = csv.reader(file)
        for row in reader:
            singlegames_tuple = tuple(row)
            singlegames_str = str(singlegames_tuple)
            singleplayer_games = ("INSERT INTO singleplayer_games (title, genre, game_developer, publisher, release_year, sold_copies, review)"
                    "VALUES "  + singlegames_str + ";")
            insert_sql_singleplayer_games.append(singleplayer_games)

    for query in insert_sql_singleplayer_games:
        try:
            print("SQL query {}: ".format(query), end='')
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            cnx.commit()
            print("OK")
# insert_into_multiplayer_games imports the contents from the multiplayer_games.csv file
# and inserts them into the table multiplayer_games
def insert_into_multiplayer_games(cursor):
    insert_sql_multiplayer_games = []
    with open("multiplayer_games.csv", newline='', encoding="utf=8") as file:
        next(file) # Skips the first row in the .csv file
        reader = csv.reader(file)
        for row in reader:
            multigames_tuple = tuple(row)
            multigames_str = str(multigames_tuple)
            multiplayer_games = ("INSERT INTO multiplayer_games (title, genre, game_developer, publisher, release_year, sold_copies, review)"
                    "VALUES "  + multigames_str + ";")
            insert_sql_multiplayer_games.append(multiplayer_games)

    for query in insert_sql_multiplayer_games:
        try:
            print("SQL query {}: ".format(query), end='')
            cursor.execute(query)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            cnx.commit()
            print("OK")

# Prints the layout of the main menu
def print_menu():
    print("------------------------------------------------------------"
          "\nMain menu: \n"
          "\n1. List all games\n"
          "2. Search for game details\n"
          "3. Search for game studio\n"
          "4. Search for what country the game has been developed in\n"
          "5. Average reviews for game developers\n"
          "6. Search for all the games a developer has made\n"
          "Q. Quit\n"
          "------------------------------------------------------------")

# Creates a view called all_games from the two tables singleplayer_games and multiplayer_games 
# that contains two column title and game_developer
def view_all_games():
     cursor.execute("CREATE VIEW all_games AS "
                    "SELECT title, game_developer "
                    "FROM singleplayer_games  "
                    "UNION "
                    "SELECT title, game_developer "
                    "FROM multiplayer_games")

        

# menu_choice_1 when called will print out all the names of either all the singleplayer games
# or multiplayer games or all the games that have both singleplayer and multiplayer
def menu_choice_1():
    print("------------------------------------------------------------"
           "\nDo you want to list all singleplayer or multiplayer games?\n"
           "1. All singleplayer games\n"
           "2. All multiplayer games\n"
           "3. All games that have multiplayer and singleplayer\n"
           "4. All games\n"
           "Press enter to return to main menu\n"
           "------------------------------------------------------------")
    choice = input("Please choose one option: ")

    if choice == "1":
        cursor.execute("SELECT title "
                    "FROM singleplayer_games")
        result = cursor.fetchall()

        for i in result:
            i = str(i)
            i = re.sub(f"[{regular}]", "", i)
            print(i)
        input(f"\n{press_enter}")

    elif choice == "2":
        cursor.execute("SELECT title "
                   "FROM multiplayer_games")
        result = cursor.fetchall()

        for i in result:
            i = str(i)
            i = re.sub(f"[{regular}]", "", i)
            print(i)
        input(f"{press_enter}")
    # JOINS the two tables multiplayer_games and singleplayer_games on title and
    # will join on all the atributes that they share and then print them out
    elif choice == "3":
        cursor.execute("SELECT multiplayer_games.title "
                   "FROM (multiplayer_games JOIN singleplayer_games "
                   "ON multiplayer_games.title = singleplayer_games.title)")
        result = cursor.fetchall()

        for i in result:
            i = str(i)
            i = i.strip("(),")
            print(i)
        input(f"\n{press_enter}")
    # Prints out all the titles from the view all_games
    elif choice == "4":
        cursor.execute("SELECT title "
                        "FROM all_games "
                        "ORDER BY title")
        
        result = cursor.fetchall()
        for i in result:
            i = str(i)
            i = i.strip("(),")
            print(i)
        input(f"\n{press_enter}")

# menu_choice_2 when called will ask for the name of a game that exists in either multiplayer or singleplayer games
# or both tables and print out all the information for that game
def menu_choice_2():

    search = input("Please enter title: ")
    cursor.execute( "SELECT * "
                "FROM singleplayer_games "
                f"WHERE title ='{search}'"
                "UNION "
                "SELECT * "
                "FROM multiplayer_games "
                f"WHERE title ='{search}'")
    result = cursor.fetchall()

    headers = ["Title", "Genre", "Game Developer", "Publisher", "Release year", "Sold copies", "Review(metacritic)"]
    index = 0
    for i in result:
        for j in i:
            print(f"{headers[index]}: {j}")
            index += 1
            
    input(f"\n{press_enter}")

# menu_choice_3 when called will ask for a studio name and print out all the 
# information linked to that studio name
def menu_choice_3():
    
    search = input("Please enter studio name: ")
    cursor.execute("SELECT * "
                "FROM game_developers "
               f"WHERE studio_name = '{search}'")
    result = cursor.fetchall()

    headers = ["Studio Name", "Founded", "Founder", "Headquarters", "Type", "Country"]
    index = 0
    for i in result:
        for j in i:
            print(f"{headers[index]}: {j}")
            index += 1
            
    input(f"\n{press_enter}")

# menu_choice_4 when called will ask for a name of a game and will then
# print out the country where the game developers for the game are located
def menu_choice_4():

    search = input("Enter the name of the game: ")
    cursor.execute( "SELECT country "
                    "FROM game_developers "
                    "WHERE studio_name = ( "
                        "SELECT game_developer "
                        "FROM singleplayer_games "
                        f"WHERE title ='{search}') "
                    "UNION "
                    "SELECT country "
                    "FROM game_developers "
                    "WHERE studio_name = ( "
                        "SELECT game_developer "
                        "FROM multiplayer_games "
                        f"WHERE title ='{search}') ")
                      
    result = cursor.fetchall()
    for i in result:
        i = str(i)
        i = re.sub(f"[{regular}]", "", i)
        print(i)
    input(f"\n{press_enter}")
# menu_choice_5 print out the average review score of every game developer
def menu_choice_5():

    cursor.execute("SELECT game_developer, AVG(review) " 
                    "FROM singleplayer_games "
                    "WHERE review <> 'NA' "
                    "GROUP BY game_developer "
                    "UNION "
                    "SELECT game_developer, AVG(review) " 
                    "FROM multiplayer_games "
                    "WHERE review <> 'NA' "
                    "GROUP BY game_developer ")                      
    result = cursor.fetchall()

    headers = ["Studio Name", "Average Review"]
    index = 0
    for i in result:
        for j in i:
            if index == 1:
                print(f"{headers[index]}: {j}")
                index = 0
            else:
                print(f"{headers[index]}: {j}")
                index += 1

    input(f"\n{press_enter}")

# menu_choice_6 will ask for the name of a game dveloper and then print out
# all the games that the developer has made
def menu_choice_6():

    search = input("Enter the name of the game developer: ")
    cursor.execute( "SELECT title "
                    "FROM singleplayer_games "
                    "WHERE game_developer = ( "
                        "SELECT studio_name "
                        "FROM game_developers "
                        f"WHERE studio_name ='{search}')"
                    "UNION "
                    "SELECT title "
                    "FROM multiplayer_games "
                    "WHERE game_developer = ( "
                        "SELECT studio_name "
                        "FROM game_developers "
                        f"WHERE studio_name ='{search}')")
    result = cursor.fetchall()
    for i in result:
        i = str(i)
        i = i.strip("(),")
        print(i)
    input(f"\n{press_enter}")


# Program starts
try:
    cursor.execute("USE {}".format(DB_NAME)) #"USE games"
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor, DB_NAME)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
        create_table_game_developers(cursor)
        create_table_multiplayer_games(cursor)
        create_table_singleplayer_games(cursor)
        insert_into_game_developers(cursor)
        insert_into_multiplayer_games(cursor)
        insert_into_singleplayer_games(cursor)
        view_all_games()
    else:
        print(err)
try:
    # Some variabels that repeats alot in the program
    regular = "(),"
    press_enter = "Press Enter"
    error_alrexi = "already exists."


    # The menu with all the choices
    while True:
        print_menu()
        user_input = input("Please chose one option: ")
        if user_input == "1":
            menu_choice_1()
        elif user_input == "2":
            menu_choice_2()
        elif user_input == "3":
            menu_choice_3()
        elif user_input == "4":
            menu_choice_4()
        elif user_input == "5":
            menu_choice_5()
        elif user_input == "6":
            menu_choice_6()
        elif user_input == "q" or  user_input == "Q":
            print("Bye bye!")
            cursor.close()
            cnx.close()
            exit(code=0)
        else:
            input("Please use one of the options above! ")
except Exception as e:
    print(e)