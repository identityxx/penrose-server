insert into posixAccount (
    cn,
    uid,
    uidNumber,
    gidNumber,
    homeDirectory,
    authPassword,
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
    null,
    '$1$G7z/ptsu$LZ0MsnHSQrsEVoOkLp9Rp0',
    '/bin/bash',
    null,
    null
);

insert into shadowAccount (
    uid,
    authPassword,
    userPassword,
    description,
    shadowLastChange,
    shadowMin,
    shadowMax,
    shadowWarning,
    shadowInactive,
    shadowExpire,
    shadowFlag
) values (
    'test',
    null,
    '$1$G7z/ptsu$LZ0MsnHSQrsEVoOkLp9Rp0',
    null,
    null,
    0,
    99999,
    7,
    null,
    null,
    null
);

insert into posixGroup (
    cn,
    gidNumber,
    authPassword,
    userPassword,
    description
) values (
    'test',
    1000,
    null,
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
