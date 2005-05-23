drop table users;

create table users (
    firstName varchar(50),
    lastName varchar(50),
    username varchar(50),
    password varchar(50),
    primary key (username)
);

insert into users values ('George', 'Washington', 'gwashington', 'usa');
insert into users values ('James', 'Bond', 'jbond', '007');

drop table groups;

create table groups (
    groupname varchar(30) not null,
    description varchar(255),
    primary key (groupname)
);

insert into groups values ('uspresidents', 'US Presidents');
insert into groups values ('britishagents', 'British Agents');

drop table usergroups;

create table usergroups (
    username varchar(30) not null,
    groupname varchar(30) not null,
    primary key (username, groupname)
);

insert into usergroups values ('gwashington', 'uspresidents');
insert into usergroups values ('jbond', 'britishagents');