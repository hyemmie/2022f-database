# 2022f-database
2022-f database course projects

## Proj1-1: Implementing SQL Parser
The goal of this project is to implement a simple SQL parser.
A SQL parser is a tool that understands and analyzes the structure of SQL syntax.
The SQL parser implemented in Project 1-1 will continue to be used in future projects 1-2 and 1-3.

## Proj1-2: Implementing DDL
The goal of this project is to add functionality to the SQL parser program implemented in Project 1-1 to store and access schemas.
The program you implement should be able to handle four DDL statements: create table, drop table, desc, and show tables.
The table schema should be stored in a file so that it doesn't disappear when the program exits. Use Berkeley DB for storage.
You will be using the code from Project 1-2 to implement the actual insertion and deletion of records in Project 1-3, so keep that in mind as you build.

## Proj1-3: Implementing DML
The goal of this project is to add functionality to the programs implemented in Project 1-1 and Project 1-2 so that they can handle simple DML.
The implemented program should be able to handle the four DML statements (insert, delete, select, update). The table data should be stored in a file so that it does not disappear when the program exits. Berkeley DB is used to store the data.

## Proj2: Implementing a Simple Database Application
The goal of this project is to design and implement a simple application that utilizes a database. You will use Python and MySQL to create a simple application that simulates movie ticketing. Through this project, students will learn how to integrate an application with a database.
