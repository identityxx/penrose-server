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

create table ipHost_alias (
    cn varchar(100),
    alias varchar(100),
    primary key (cn,alias)
);

create table ipNetwork (
    cn varchar(100),
    ipNetworkNumber varchar(255),
    ipNetmaskNumber varchar(255),
    l varchar(255),
    description varchar(255),
    manager varchar(255),
    primary key (cn)
);

create table ipNetwork_alias (
    cn varchar(100),
    alias varchar(100),
    primary key (cn,alias)
);

create table ipService (
    cn varchar(100),
    ipServicePort int(11),
    ipServiceProtocol varchar(100),
    description varchar(255),
    primary key (cn,ipServiceProtocol)
);

create table ipService_alias (
    cn varchar(100),
    ipServiceProtocol varchar(100),
    alias varchar(100),
    primary key (cn,ipServiceProtocol,alias)
);

create table ipProtocol (
    cn varchar(100),
    ipProtocolNumber int(11),
    description varchar(255),
    primary key (cn)
);

create table ipProtocol_alias (
    cn varchar(100),
    alias varchar(100),
    primary key (cn,alias)
);

create table oncRpc (
    cn varchar(100),
    oncRpcNumber int(11),
    description varchar(255),
    primary key (cn)
);

create table oncRpc_alias (
    cn varchar(100),
    alias varchar(100),
    primary key (cn,alias)
);

create table nisNetgroup (
    cn varchar(100),
    description varchar(255),
    primary key (cn)
);

create table nisNetgroup_nisNetgroupTriple (
    cn varchar(100),
    nisNetgroupTriple varchar(100),
    primary key (cn,nisNetgroupTriple)
);

create table nisNetgroup_memberNisNetgroup (
    cn varchar(100),
    memberNisNetgroup varchar(100),
    primary key (cn,memberNisNetgroup)
);
