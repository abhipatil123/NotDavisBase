DATABASE: DAVISBASE
Run the following commands in the following order to check for all functionalities
1) Run DavisBase.java file
(On command prompt: 
a. javac DavisBase.java
b. java DavisBase)

2) Use the following command:

a) show tables; To get all the tables in the database
b) help; To get all the commands supported by database
c) create table student (rowid INT, name TEXT, marks INT); To create a table in the database
d) select * from student;	To get the data from table student
e) insert into student (rowid,name,marks) values (1,Abhi,100); To insert data into the table
f) select * from student; To get the data from table student
g) update student set marks = 100 where rowid =1; To update the record (Bonus)
h) insert into student (rowid,name,marks) values (2,Patil,99); To insert data into the table
i) select * from student where rowid=2; To get the row with rowid=2
j) delete from table student where rowid=2; To delete a data from the table 
k) drop table student; To drop the table from the database
l) select * from student;
