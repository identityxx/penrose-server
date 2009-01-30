create table users (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(10),
    primary key (username)
);

create table groups (
    groupname varchar(50),
    description varchar(50),
    primary key (groupname)
);
