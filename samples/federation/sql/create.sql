create table global_parameters (
    name varchar(100),
    value text,
    primary key (name)
);

create table repositories (
    name varchar(100),
    type varchar(50),
    primary key (name)
);

create table repository_parameters (
    repository varchar(100),
    name varchar(100),
    value text,
    primary key (repository, name)
);
