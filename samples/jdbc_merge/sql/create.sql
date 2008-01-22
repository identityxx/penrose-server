create table users (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    encPassword varchar(255),
    password varchar(10),
    primary key (username)
);

create table users2 (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    encPassword varchar(255),
    password varchar(10),
    primary key (username)
);

create table usergroups (
    groupname varchar(50),
    username varchar(50),
    primary key (groupname, username)
);
