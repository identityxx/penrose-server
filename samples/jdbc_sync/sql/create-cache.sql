create table users_cache (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(10),
    primary key (username)
);

create table users_tracker (
    changeNumber integer,
    changeTimestamp datetime,
    primary key (changeNumber)
);

create table groups_cache (
    groupname varchar(50),
    primary key (groupname)
);

create table groups_tracker (
    changeNumber integer,
    changeTimestamp datetime,
    primary key (changeNumber)
);

create table usergroups_cache (
    groupname varchar(50),
    username varchar(50),
    primary key (groupname, username)
);

create table usergroups_tracker (
    changeNumber integer,
    changeTimestamp datetime,
    primary key (changeNumber)
);
