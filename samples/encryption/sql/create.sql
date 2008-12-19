create table plain (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(255),
    primary key (username)
);

create table crypt (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(255),
    primary key (username)
);

create table crypt_md5 (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(255),
    primary key (username)
);

create table crypt_sha256 (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(255),
    primary key (username)
);

create table crypt_sha512 (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(255),
    primary key (username)
);

create table md5 (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(255),
    primary key (username)
);

create table sha (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(255),
    primary key (username)
);
