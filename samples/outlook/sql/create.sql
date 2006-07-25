create table users (
    username varchar(50),

    firstName varchar(50),
    initials varchar(10),
    lastName varchar(50),

    title varchar(50),
    company varchar(50),
    department varchar(50),
    office varchar(50),
    assistant varchar(100),

    businessAddress varchar(255),
    businessCity varchar(50),
    businessState varchar(50),
    businessZipCode varchar(10),
    businessCountry varchar(50),
    homeAddress varchar(255),

    businessPhone varchar(50),
    businessPhone2 varchar(50),
    businessFax varchar(50),
    assistantPhone varchar(50),
    homePhone varchar(50),
    homeFax varchar(50),
    mobile varchar(50),
    pager varchar(50),

    notes varchar(255),

    encPassword varchar(255),
    password varchar(10),

    primary key (username)
);

create table relationships (
    manager varchar(50),
    report varchar(50),

    primary key (manager, report)
);
