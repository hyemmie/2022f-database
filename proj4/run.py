import pymysql.cursors
import csv

STUDENT_ID = 'DB2017_19651'
TABLE_MOVIE = 'Movie'
TABLE_AUDIENCE = 'Audience'
TABLE_BOOK = 'Book'
TABLE_RATE = 'Rate'

MALE = 'male'
FEMALE = 'female'

def connect_db():
    return pymysql.connect(
        host='astronaut.snu.ac.kr',
        port=7000, 
        user=STUDENT_ID, 
        password=STUDENT_ID, 
        database=STUDENT_ID,
    )

# Problem 1 (5 pt.)
def reset():
    drop_table(TABLE_RATE)
    drop_table(TABLE_BOOK)
    drop_table(TABLE_AUDIENCE)
    drop_table(TABLE_MOVIE)
    create_table(TABLE_MOVIE)
    create_table(TABLE_AUDIENCE)
    create_table(TABLE_BOOK)
    create_table(TABLE_RATE)

    initialize()
    print('Initialized database')

def drop_table(table_name):
    with connection.cursor() as cursor:
        sql = f"DROP TABLE IF EXISTS {table_name}"
        result = cursor.execute(sql)
    connection.commit()

def create_table(table_name):
    if table_name == TABLE_MOVIE:
        with connection.cursor() as cursor:
            sql= f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    `m_id` INT NOT NULL AUTO_INCREMENT,
                    `title` VARCHAR(80) NOT NULL,
                    `director` VARCHAR(80) NOT NULL,
                    `price` INT UNSIGNED NOT NULL,
                    PRIMARY KEY (`m_id`))"""
            cursor.execute(sql)
    if table_name == TABLE_AUDIENCE:
        with connection.cursor() as cursor:
            sql= f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    `a_id` INT NOT NULL AUTO_INCREMENT,
                    `name` VARCHAR(80) NOT NULL,
                    `gender` ENUM('{MALE}', '{FEMALE}') NOT NULL,
                    `age` INT UNSIGNED NOT NULL,
                    PRIMARY KEY (`a_id`))"""
            cursor.execute(sql)
    if table_name == TABLE_BOOK:
        with connection.cursor() as cursor:
            sql= f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    `b_id` INT NOT NULL AUTO_INCREMENT,
                    `m_id` INT NOT NULL,
                    `a_id` INT NOT NULL,
                    PRIMARY KEY (`b_id`),
                    CONSTRAINT `book_table`
                        FOREIGN KEY (`m_id`)
                        REFERENCES {TABLE_MOVIE} (`m_id`)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE,
                    CONSTRAINT `book_audience`
                        FOREIGN KEY (`a_id`)
                        REFERENCES {TABLE_AUDIENCE} (`a_id`)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE)"""
            cursor.execute(sql)
    if table_name == TABLE_RATE:
        with connection.cursor() as cursor:
            sql= f"""CREATE TABLE IF NOT EXISTS {table_name} (
                    `r_id` INT NOT NULL AUTO_INCREMENT,
                    `b_id` INT NOT NULL,
                    `score` INT NOT NULL,
                    PRIMARY KEY (`r_id`),
                    CONSTRAINT `rate_book`
                        FOREIGN KEY (`b_id`)
                        REFERENCES {TABLE_BOOK} (`b_id`)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE)"""
            cursor.execute(sql)
    connection.commit()

def initialize():
    f = open('data.csv','r')
    rdr = csv.reader(f)
    is_first = True
 
    for line in rdr:
        if is_first:
            is_first = False
        else:
            insert_movie_inner(line[0], line[1], line[2])
            insert_audience_inner(line[3], line[4], line[5])

    f.close()

def insert_movie_inner(title, director, price):
    with connection.cursor() as cursor:
        sql= f"""INSERT INTO {TABLE_MOVIE}
                (`title`, `director`, `price`)
                VALUES (%s, %s, %s)"""
        cursor.execute(sql, (title, director, price))
    connection.commit()

def insert_audience_inner(name, gender, age):
    with connection.cursor() as cursor:
        sql= f"""INSERT INTO {TABLE_AUDIENCE}
                (`name`, `gender`, `age`)
                VALUES (%s, %s, %s)"""
        cursor.execute(sql, (name, gender, age))
    connection.commit()

def print_line(data, lengthList):
    data = list(map(str, data))
    length = len(data)
    formatStr = ""
    for i in range(0, length):
        formatStr = formatStr + "{" + "{num}:<{len}s".format(num=i, len=lengthList[i]) + "}"
        if i != length-1:
            formatStr = formatStr + "    "
    print(formatStr.format(*data))
    
def print_align(column_list, records):
    col_num = len(column_list)
    max_len = [0] * col_num
    
    for i in range(0, col_num):
        max_len[i] = len(column_list[i])

    for rec in records:
        for i in range(0, col_num):
            max_len[i] = max(max_len[i], len(str(rec[i])))
    
    print("-------------------------------------------------------------------------------------------------------------------------")
    print_line(column_list, max_len)
    print("-------------------------------------------------------------------------------------------------------------------------")
    
    for i in records:
        print_line(i, max_len)

# Problem 2 (3 pt.)
def print_movies():
    # YOUR CODE GOES HERE
    with connection.cursor() as cursor:
        sql= f"""SELECT m_id, title, director, price, COUNT(b_id), AVG(score)
                FROM {TABLE_MOVIE} NATURAL LEFT OUTER JOIN ({TABLE_BOOK} NATURAL LEFT OUTER JOIN {TABLE_RATE})
                GROUP BY m_id
                ORDER BY m_id"""
        cursor.execute(sql)
        result = cursor.fetchall()
    title_list = ('id', 'title', 'director', 'price', 'book count', 'average rate')
    print_align(title_list, result)

# Problem 3 (3 pt.)
def print_audiences():
    # YOUR CODE GOES HERE
    with connection.cursor() as cursor:
        sql= f"""SELECT a_id, name, gender, age
                FROM {TABLE_AUDIENCE}
                ORDER BY a_id"""
        cursor.execute(sql)
        result = cursor.fetchall()
    title_list = ('id', 'name', 'gender', 'age')
    print_align(title_list, result)

# Problem 4 (3 pt.)
def insert_movie():
    # YOUR CODE GOES HERE
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = input('Movie price: ')

    if int(price) >= 0:
        insert_movie_inner(title, director, price)
        # success message
        print('A movie is successfully inserted')
    else:
        print('Movie price cannot be negative')

# Problem 6 (4 pt.)
def remove_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')

    with connection.cursor() as cursor:
        sql= f"""DELETE FROM {TABLE_MOVIE}
                WHERE m_id=%s"""
        result = cursor.execute(sql, movie_id)
    connection.commit()

    if result == 0: 
            # error message
        print(f'Movie {movie_id} does not exist')
    else:
        # success message
        print('A movie is successfully removed')

# Problem 5 (3 pt.)
def insert_audience():
    # YOUR CODE GOES HERE
    name = input('Audience name: ')
    gender = input('Audience gender: ')
    age = input('Audience age: ')

    if gender == MALE or gender == FEMALE:
        insert_audience_inner(name, gender, age)
        print('An audience is successfully inserted')
    else:
        print("Audience gender should be 'male' or 'female'")

# Problem 7 (4 pt.)
def remove_audience():
    # YOUR CODE GOES HERE
    audience_id = input('Audience ID: ')

    with connection.cursor() as cursor:
        sql= f"""DELETE FROM {TABLE_AUDIENCE}
                WHERE a_id=%s"""
        result = cursor.execute(sql, audience_id)
    connection.commit()

    if result == 0:
        # error message
        print(f'Audience {audience_id} does not exist')
    else:
        # success message
        print('An audience is successfully removed')

# Problem 8 (5 pt.)
def book_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    audience_id = input('Audience ID: ')

    with connection.cursor() as cursor:
        # check movie id
        sql= f"""SELECT m_id
                FROM {TABLE_MOVIE}
                WHERE m_id=%s"""
        cursor.execute(sql, movie_id)
        mid_result = cursor.fetchall()

        # check audience id
        sql= f"""SELECT a_id
                FROM {TABLE_AUDIENCE}
                WHERE a_id=%s"""
        cursor.execute(sql, audience_id)
        aid_result = cursor.fetchall()

        # check if already booked
        sql= f"""SELECT b_id
                FROM {TABLE_BOOK}
                WHERE m_id=%s AND a_id=%s"""
        cursor.execute(sql, (movie_id, audience_id))
        duplicate_result = cursor.fetchall()


        if len(mid_result) < 1:
            print(f'Movie {movie_id} does not exist')
        elif len(aid_result) < 1:
            print(f'Audience {audience_id} does not exist')
        elif len(duplicate_result) >= 1:
            print('One audience cannot book the same movie twice')
        else:
            sql= f"""INSERT INTO {TABLE_BOOK}
                    (`m_id`, `a_id`)
                    VALUES (%s, %s)"""
            cursor.execute(sql, (movie_id, audience_id))
            connection.commit()  
            print('Successfully booked a movie')

# Problem 9 (5 pt.)
def rate_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    audience_id = input('Audience ID: ')
    rating = input('Ratings (1~5): ')

    with connection.cursor() as cursor:
        # check movie id
        sql= f"""SELECT m_id
                FROM {TABLE_MOVIE}
                WHERE m_id=%s"""
        cursor.execute(sql, movie_id)
        mid_result = cursor.fetchall()

        # check audience id
        sql= f"""SELECT a_id
                FROM {TABLE_AUDIENCE}
                WHERE a_id=%s"""
        cursor.execute(sql, audience_id)
        aid_result = cursor.fetchall()

        # check if already booked
        sql= f"""SELECT b_id
                FROM {TABLE_BOOK}
                WHERE m_id=%s AND a_id=%s"""
        cursor.execute(sql, (movie_id, audience_id))
        bid_result = cursor.fetchall()

        if 1 > int(rating) or int(rating) > 5:
             print(f'Wrong value for a rating')
        elif len(mid_result) < 1:
            print(f'Movie {movie_id} does not exist')
        elif len(aid_result) < 1:
            print(f'Audience {audience_id} does not exist')
        elif len(bid_result) < 1:
            print('Audience cannot rate movie without booking')
        else:
            # check if already rated
            sql= f"""SELECT r_id
                    FROM {TABLE_RATE}
                    WHERE b_id=%s"""
            cursor.execute(sql, bid_result[0])
            rid_result = cursor.fetchall()
            if len(rid_result) >= 1:
                print('One audience cannot rate the same movie twice')
            else:
                sql= f"""INSERT INTO {TABLE_RATE}
                        (`b_id`, `score`)
                        VALUES (%s, %s)"""
                cursor.execute(sql, (bid_result[0], rating))
                connection.commit()  
                print('Successfully rated a movie')    

# Problem 10 (5 pt.)
def print_audiences_for_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')

    with connection.cursor() as cursor:
        # check movie id
        sql= f"""SELECT m_id
                FROM {TABLE_MOVIE}
                WHERE m_id=%s"""
        cursor.execute(sql, movie_id)
        mid_result = cursor.fetchall()

        if len(mid_result) < 1:
            # error message
            print(f'Movie {movie_id} does not exist')
        else:
            sql= f"""SELECT DISTINCT a_id, name, gender, age, score
                    FROM {TABLE_AUDIENCE} NATURAL JOIN ({TABLE_BOOK} NATURAL LEFT OUTER JOIN {TABLE_RATE})
                    WHERE m_id=%s"""
            cursor.execute(sql, movie_id)
            result = cursor.fetchall()
            title_list = ('id', 'name', 'gender', 'age', 'rate')
            print_align(title_list, result)

# Problem 11 (5 pt.)
def print_movies_for_audience():
    # YOUR CODE GOES HERE
    audience_id = input('Audience ID: ')

    with connection.cursor() as cursor:
        # check movie id
        sql= f"""SELECT a_id
                FROM {TABLE_AUDIENCE}
                WHERE a_id=%s"""
        cursor.execute(sql, audience_id)
        aid_result = cursor.fetchall()

        if len(aid_result) < 1:
             # error message
            print(f'Audience {audience_id} does not exist')
        else:
            sql= f"""SELECT DISTINCT m_id, title, director, price, score
                    FROM {TABLE_MOVIE} NATURAL JOIN ({TABLE_BOOK} NATURAL LEFT OUTER JOIN {TABLE_RATE})
                    WHERE a_id=%s"""
            cursor.execute(sql, audience_id)
            result = cursor.fetchall()
            title_list = ('id', 'title', 'director', 'price', 'rate')
            print_align(title_list, result)

# Problem 12 (10 pt.)
def recommend():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    audience_id = input('Audience ID: ')


    # error message
    print(f'Movie {movie_id} does not exist')
    print(f'Audience {audience_id} does not exist')
    print('Rating does not exist')
    # YOUR CODE GOES HERE
    pass


# Total of 60 pt.
def main():
    # initialize database
    reset()

    while True:
        print('============================================================')
        print('1. print all movies')
        print('2. print all audiences')
        print('3. insert a new movie')
        print('4. remove a movie')
        print('5. insert a new audience')
        print('6. remove an audience')
        print('7. book a movie')
        print('8. rate a movie')
        print('9. print all audiences who booked for a movie')
        print('10. print all movies rated by an audience')
        print('11. recommend a movie for an audience')
        print('12. exit')
        print('13. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            print_movies()
        elif menu == 2:
            print_audiences()
        elif menu == 3:
            insert_movie()
        elif menu == 4:
            remove_movie()
        elif menu == 5:
            insert_audience()
        elif menu == 6:
            remove_audience()
        elif menu == 7:
            book_movie()
        elif menu == 8:
            rate_movie()
        elif menu == 9:
            print_audiences_for_movie()
        elif menu == 10:
            print_movies_for_audience()
        elif menu == 11:
            recommend()
        elif menu == 12:
            print('Bye!')
            break
        elif menu == 13:
            reset()
        else:
            print('Invalid action')

    connection.close()


connection = connect_db()
if __name__ == "__main__":
    main()