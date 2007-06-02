insert into device (
    cn,
    serialNumber,
    seeAlso,
    owner,
    ou,
    o,
    l,
    description
) values (
    'penrose.example.com',
    null,
    null,
    null,
    null,
    null,
    null,
    null
);

insert into ipHost (
    cn,
    ipHostNumber,
    manager
) values (
    'penrose.example.com',
    '127.0.0.1',
    null
);

insert into ipHost_alias (
    cn,
    alias
) values (
    'penrose.example.com',
    'penrose'
);
