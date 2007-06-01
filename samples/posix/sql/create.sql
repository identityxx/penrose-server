create table posixAccount (
    cn varchar(255),
    uid varchar(100),
    uidNumber int(11),
    gidNumber int(11),
    homeDirectory varchar(255),
    userPassword varchar(255),
    loginShell varchar(255),
    gecos varchar(255),
    description varchar(255),
    primary key (uid)
);

create table posixGroup (
    cn varchar(100),
    gidNumber int(11),
    userPassword varchar(255),
    description varchar(255),
    primary key (cn)
);

create table posixGroup_memberUid (
    cn varchar(100),
    memberUid varchar(100),
    primary key (cn, memberUid)
);

create table device (
    cn varchar(100),
    serialNumber varchar(255),
    seeAlso varchar(255),
    owner varchar(255),
    ou varchar(255),
    o varchar(255),
    l varchar(255),
    description varchar(255),
    primary key (cn)
);

create table ipHost (
    cn varchar(100),
    ipHostNumber varchar(100),
    manager varchar(255),
    primary key (cn)
);

create table ipHost_cn (
    cn varchar(100),
    ipHostNumber varchar(100),
    primary key (cn,ipHostNumber)
);
