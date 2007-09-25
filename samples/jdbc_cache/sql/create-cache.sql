create table users (
    uid varchar(50),
    cn varchar(50),
    sn varchar(50),
    userPassword varchar(50),
    primary key (uid)
);

create table groups (
    cn varchar(50),
    primary key (cn)
);

create table usergroups (
    cn varchar(50),
    uniqueMember varchar(200),
    primary key (cn, uniqueMember)
);
