create table users_cache (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    encPassword varchar(255),
    password varchar(10),
    primary key (username)
);

create table groups_cache (
    groupname varchar(50),
    primary key (groupname)
);

create table usergroups_cache (
    groupname varchar(50),
    username varchar(50),
    primary key (groupname, username)
);

create table tracker (
    sourceName varchar(50),
    changeNumber integer,
    primary key (sourceName)
);
