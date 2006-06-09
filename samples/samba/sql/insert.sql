insert into users (username, firstName, lastName, encPassword, password) values ('jstockton', 'Jim', 'Stockton', '29609449ad6a267aac664f9c1d9c267b54aade19', 'j5t0ckt0n');
insert into users (username, firstName, lastName, encPassword, password) values ('swhite', 'Scott', 'White', '1c60f10728e0f6878d60f7345225a8c680844d19', 'swh1t3');
insert into users (username, firstName, lastName, encPassword, password) values ('pfarmer', 'Pete', 'Farmer', 'e7beca51ea8907ea427f538c73638058401ff795', 'pf4rm3r');
insert into users (username, firstName, lastName, encPassword, password) values ('lwalker', 'Lee', 'Walker', 'ed7369e175739f26aa2f421a67e3d8a66d753bdd', 'lw4lk3r');
insert into users (username, firstName, lastName, encPassword, password) values ('alange', 'Andy', 'Lange', '47af3474c807aaea8b876c39e6ce8fb41e7ac637', '4l4ng3');

insert into groups (groupname) values ('manager');
insert into groups (groupname) values ('administrator');
insert into groups (groupname) values ('user');

insert into usergroups values ('manager', 'jstockton');
insert into usergroups values ('administrator', 'swhite');
insert into usergroups values ('user', 'jstockton');
insert into usergroups values ('user', 'swhite');
insert into usergroups values ('user', 'alange');
insert into usergroups values ('user', 'lwalker');
