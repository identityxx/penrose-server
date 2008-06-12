insert into users (username, firstName, lastName, password) values ('jstockton', 'Jim', 'Stockton', 'j5t0ckt0n');
insert into users (username, firstName, lastName, password) values ('swhite', 'Scott', 'White', 'swh1t3');
insert into users (username, firstName, lastName, password) values ('pfarmer', 'Pete', 'Farmer', 'pf4rm3r');
insert into users (username, firstName, lastName, password) values ('lwalker', 'Lee', 'Walker', 'lw4lk3r');
insert into users (username, firstName, lastName, password) values ('alange', 'Andy', 'Lange', '4l4ng3');

insert into groups (groupname) values ('manager');
insert into groups (groupname) values ('administrator');
insert into groups (groupname) values ('user');

insert into usergroups (groupname, username) values ('manager', 'jstockton');
insert into usergroups (groupname, username) values ('administrator', 'swhite');
insert into usergroups (groupname, username) values ('user', 'jstockton');
insert into usergroups (groupname, username) values ('user', 'swhite');
insert into usergroups (groupname, username) values ('user', 'alange');
insert into usergroups (groupname, username) values ('user', 'lwalker');
