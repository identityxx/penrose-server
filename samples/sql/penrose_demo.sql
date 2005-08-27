drop table users;

create table users (
    firstName varchar(50),
    lastName varchar(50),
    username varchar(50),
    password varchar(50),
    primary key (username)
);

insert into users values ('George', 'Washington', 'gwashington', '1st');
insert into users values ('Thomas', 'Jefferson', 'tjefferson', '3rd');
insert into users values ('Abraham', 'Lincoln', 'alincoln', '16th');
insert into users values ('Theodore', 'Roosevelt', 'troosevelt', '26th');
insert into users values ('Bill', 'Fairbanks', 'bfairbanks', '002');
insert into users values ('Stuart', 'Thomas', 'sthomas', '005');
insert into users values ('Alec', 'Trevelyan', 'atrevelyan', '006');
insert into users values ('James', 'Bond', 'jbond', '007');
insert into users values ('Brosnan', 'Pierce', 'pbrosnan', 'k33ly');
insert into users values ('Williams', 'Vanessa', 'vwilliams', 'p0cah0nta5');
insert into users values ('Bean', 'Julian', 'jbean', 'mrbean');

drop table groups;

create table groups (
    groupname varchar(30) not null,
    description varchar(255),
    primary key (groupname)
);

insert into groups values ('uspresidents', 'US Presidents');
insert into groups values ('britishagents', 'British Agents');
insert into groups values ('singers', 'Singers');
insert into groups values ('actors', 'Actors');
insert into groups values ('dancers', 'Dancer');

drop table usergroups;

create table usergroups (
    username varchar(30) not null,
    groupname varchar(30) not null,
    primary key (username, groupname)
);

insert into usergroups values ('gwashington', 'uspresidents');
insert into usergroups values ('tjefferson', 'uspresidents');
insert into usergroups values ('alincoln', 'uspresidents');
insert into usergroups values ('troosevelt', 'uspresidents');
insert into usergroups values ('bfairbanks', 'britishagents');
insert into usergroups values ('sthomas', 'britishagents');
insert into usergroups values ('atrevelyan', 'britishagents');
insert into usergroups values ('jbond', 'britishagents');
insert into usergroups values ('pbrosnan', 'actors');
insert into usergroups values ('vwilliams', 'singers');
insert into usergroups values ('vwilliams', 'actors');
