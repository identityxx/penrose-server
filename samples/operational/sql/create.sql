create table users (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    password varchar(10),
    creatorsName varchar(50),
    modifiersName varchar(50),
    createTimestamp datetime,
    modifyTimestamp datetime,
    primary key (username)
);
