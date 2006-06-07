create table users (
    username varchar(50),
    firstName varchar(50),
    lastName varchar(50),
    encPassword varchar(255),
    password varchar(10),
    uid varchar(50),
    user_sid varchar(50),
    gid varchar(50),
    group_sid varchar(50),
    account_flags varchar(50),
    lm_password varchar(50),
    nt_password varchar(50),
    kickoff_time varchar(50),
    logon_time varchar(50),
    logon_script varchar(50),
    logon_hours varchar(50),
    logoff_time varchar(50),
    password_can_change varchar(50),
    password_must_change varchar(50),
    password_last_set varchar(50),
    password_history varchar(50),
    home_path varchar(50),
    home_drive varchar(50),
    profile_path varchar(50),
    user_workstations varchar(50),
    domain_name varchar(50),
    munged_dial varchar(50),
    bad_password_count varchar(50),
    bad_password_time varchar(50),
    primary key (username)
);

create table groups (
    groupname varchar(50),
    type varchar(50),
    gid varchar(50),
    group_sid varchar(50),
    primary key (groupname)
);

create table usergroups (
    groupname varchar(50),
    username varchar(50),
    primary key (groupname, username)
);