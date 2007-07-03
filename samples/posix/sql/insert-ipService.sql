insert into ipService (
    cn,
    ipServicePort,
    ipServiceProtocol,
    description
) values (
    'ftp',
    21,
    'tcp',
    null
);

insert into ipService (
    cn,
    ipServicePort,
    ipServiceProtocol,
    description
) values (
    'ftp',
    21,
    'udp',
    null
);

insert into ipService_alias (
    cn,
    ipServiceProtocol,
    alias
) values (
    'ftp',
    'udp',
    'fsp'
);

insert into ipService_alias (
    cn,
    ipServiceProtocol,
    alias
) values (
    'ftp',
    'udp',
    'fspd'
);
