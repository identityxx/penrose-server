insert into posixAccount (
    cn,
    uid,
    uidNumber,
    gidNumber,
    homeDirectory,
    userPassword,
    loginShell,
    gecos,
    description
) values (
    'Test User',
    'test',
    1000,
    1000,
    '/home/test',
    '$1$G7z/ptsu$LZ0MsnHSQrsEVoOkLp9Rp0',
    '/bin/bash',
    null,
    null
);

insert into posixGroup (
    cn,
    gidNumber,
    userPassword,
    description
) values (
    'test',
    1000,
    null,
    null
);

insert into posixGroup_memberUid (
    cn,
    memberUid
) values (
    'test',
    'test'
);
