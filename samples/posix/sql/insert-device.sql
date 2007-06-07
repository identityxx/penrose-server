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
    authPassword,
    userPassword,
    l,
    description,
    manager
) values (
    'penrose.example.com',
    '127.0.0.1',
    null,
    null,
    null,
    null,
    null
);

insert into ipHost_alias (
    cn,
    alias
) values (
    'penrose.example.com',
    'penrose'
);

insert into ieee802Device (
    cn,
    macAddress
) values (
    'penrose.example.com',
    '00:00:92:90:ee:e2'
);
