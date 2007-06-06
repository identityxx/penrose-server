insert into automountMap (
    automountMapName,
    description
) values (
    'auto.master',
    null
);

insert into automountMap (
    automountMapName,
    description
) values (
    'auto.home',
    null
);

insert into automount (
    automountMapName,
    automountKey,
    automountInformation,
    description
) values (
    'auto.master',
    '/home',
    'auto.home',
    null
);

insert into automount (
    automountMapName,
    automountKey,
    automountInformation,
    description
) values (
    'auto.home',
    '*',
    'fc5:/home/&',
    null
);
