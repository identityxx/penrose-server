insert into ipNetwork (
    cn,
    ipNetworkNumber,
    ipNetmaskNumber,
    l,
    description,
    manager
) values (
    'default',
    '0.0.0.0',
    '255.255.255.0',
    null,
    null,
    null
);

insert into ipNetwork_alias (
    cn,
    alias
) values (
    'default',
    'network'
);
