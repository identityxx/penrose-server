-- users --

drop table users_changes;

create table users_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    changeUser varchar(10),
    username varchar(50),
    primary key (changeNumber)
);

drop trigger users_add;

create trigger users_add after insert on users
for each row insert into users_changes values (null, now(), 'ADD', substring_index(user(),_utf8'@',1), new.username);

drop trigger users_modify;

delimiter |
create trigger users_modify after update on users
for each row begin
    if new.username = old.username then
        insert into users_changes values (null, now(), 'MODIFY', substring_index(user(),_utf8'@',1), new.username);
    else
        insert into users_changes values (null, now(), 'DELETE', substring_index(user(),_utf8'@',1), old.username);
        insert into users_changes values (null, now(), 'ADD', substring_index(user(),_utf8'@',1), new.username);
    end if;
end;|
delimiter ;

drop trigger users_delete;

create trigger users_delete after delete on users
for each row insert into users_changes values (null, now(), 'DELETE', substring_index(user(),_utf8'@',1), old.username);

-- groups --

drop table groups_changes;

create table groups_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    changeUser varchar(10),
    groupname varchar(50),
    primary key (changeNumber)
);

drop trigger groups_add;

create trigger groups_add after insert on groups
for each row insert into groups_changes values (null, now(), 'ADD', substring_index(user(),_utf8'@',1), new.groupname);

drop trigger groups_modify;

delimiter |
create trigger groups_modify after update on groups
for each row begin
    if new.groupname = old.groupname then
        insert into groups_changes values (null, now(), 'MODIFY', substring_index(user(),_utf8'@',1), new.groupname);
    else
        insert into groups_changes values (null, now(), 'DELETE', substring_index(user(),_utf8'@',1), old.groupname);
        insert into groups_changes values (null, now(), 'ADD', substring_index(user(),_utf8'@',1), new.groupname);
    end if;
end;|
delimiter ;

drop trigger groups_delete;

create trigger groups_delete after delete on groups
for each row insert into groups_changes values (null, now(), 'DELETE', substring_index(user(),_utf8'@',1), old.groupname);

-- usergroups --

drop table usergroups_changes;

create table usergroups_changes (
    changeNumber integer auto_increment,
    changeTime datetime,
    changeAction varchar(10),
    changeUser varchar(10),
    groupname varchar(50),
    username varchar(50),
    primary key (changeNumber)
);

drop trigger usergroups_add;

create trigger usergroups_add after insert on usergroups
for each row insert into usergroups_changes values (null, now(), 'ADD', substring_index(user(),_utf8'@',1), new.groupname, new.username);

drop trigger usergroups_modify;

delimiter |
create trigger usergroups_modify after update on usergroups
for each row begin
    if new.groupname = old.groupname and new.username = old.username then
        insert into usergroups_changes values (null, now(), 'MODIFY', substring_index(user(),_utf8'@',1), new.groupname, new.username);
    else
        insert into usergroups_changes values (null, now(), 'DELETE', substring_index(user(),_utf8'@',1), old.groupname, old.username);
        insert into usergroups_changes values (null, now(), 'ADD', substring_index(user(),_utf8'@',1), new.groupname, new.username);
    end if;
end;|
delimiter ;

drop trigger usergroups_delete;

create trigger usergroups_delete after delete on usergroups
for each row insert into usergroups_changes values (null, now(), 'DELETE', substring_index(user(),_utf8'@',1), old.groupname, old.username);
