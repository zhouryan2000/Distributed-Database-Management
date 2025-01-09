# Distributed Database system

To run the code locally,

first, build the code

```
ant
```

Then, start ECS client, 
```
java -jar m4-ecs.jar -p 4000
```

Then, start multiple servers
```
java -jar m4-server.jar -p 5000 -b localhost:4000
java -jar m4-server.jar -p 7000 -b localhost:4000
java -jar m4-server.jar -p 8000 -b localhost:4000
java -jar m4-server.jar -p 9000 -b localhost:4000
```

Then, start a client
```
java -jar m4-client.jar
```

Then, connect to any server
```
connect localhost 5000
```

Then, you can do SQL operation

we suppor following queries so far, and predicate support 6 ops: =, !=, >, >=, <=, <.
Only <predicate> can include space, and there cannot be any space between column names or values, where should be only delimited by comma ','.

```
create table <table name> (<column1>,<column2>,...)
insert into <table name> values (<value1>,<value2>,...)
select <column1>,<column2>,... from <table name OR derived table> where <predicate>
delete from <table name> where <predicate>
update <table name> set column1=<value1>,column2=<value2> where <predicate>
```

Following are some examples
```
create table student (name,mark,gender)

insert into student values (Ryan,90,male) 
insert into student values (Sandra,80,female)
insert into student values (Yan,95,female)
insert into student values (Mickey,70,male)

select name from student where mark = 80

select name,mark from student where mark <= 80

select mark,name from student where mark > 80

select * from (select * from student where mark >= 80) where mark <= 90

select * from (select * from (select * from student where name = Sandra) where mark >= 80) where mark <= 90

select * from (select * from (select * from student where name = Ryan) where mark >= 80) where mark <= 90

update student set mark=98,gender=female where name = Ryan
update student set mark=85 where name = Ryan
delete from student where name = Ryan

select * from student
select * from teacher
select * from people

create table teacher (name,age,salary)
insert into teacher values (Tom,30,1000) 
insert into teacher values (Amy,30,2000) 
insert into teacher values (John,40,2000) 

update teacher set salary=5000 where age <= 30
delete from teacher where name = Tom
delete from teacher where name = Amy
```
