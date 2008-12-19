insert into users (
    username, firstName, initials, lastName,
    title, company, department, office, assistant,
    address, city, state, zipCode, country,
    businessPhone, businessPhone2, businessFax, assistantPhone, homePhone, mobile, pager,
    notes, password
) values (
    'jstockton', 'James', 'P.', 'Stockton',
    'Manager', 'Example Corp.', 'Services', 'San Francisco', 'Sarah',
    '500 Grant Ave', 'San Francisco', 'CA', '94108', 'US',
    '415-808-0808', '415-808-0809', '415-808-0800', '415-808-0801', '415-808-0802', '415-808-0803', '415-808-0804',
    "I'm out of the office today. Please contact my assistant Sarah at 415-808-0801.",
    'j5t0ckt0n'
);

insert into users (
    username, firstName, initials, lastName,
    title, company, department, office, assistant,
    address, city, state, zipCode, country,
    businessPhone, businessPhone2, businessFax, assistantPhone, homePhone, mobile, pager,
    notes, password
) values (
    'swhite', 'Scott', null, 'White',
    'Administrator', 'Example Corp.', 'IT', 'San Antonio', null,
    '100 N Main Ave', 'San Antonio', 'TX', '78205', 'US',
    '210-123-4567', null, '210-123-4569', null, '210-123-4561', '210-123-4562', '210-123-4563',
    null, 'swh1t3'
);

insert into users (
    username, firstName, initials, lastName,
    title, company, department, office, assistant,
    address, city, state, zipCode, country,
    businessPhone, businessPhone2, businessFax, assistantPhone, homePhone, mobile, pager,
    notes, password
) values (
    'jfarmer', 'James', null, 'Farmer',
    'Software Quality Assurance', 'Example Corp.', 'Services', 'Austin', null,
    '1100 Riverside Dr.', 'Austin', 'TX', '78704', 'US',
    '512-444-4444', null, '512-444-4446', null, '512-444-4448', '512-444-4449', '512-444-4440',
    null, 'jf4rm3r'
);

insert into users (
    username, firstName, initials, lastName,
    title, company, department, office, assistant,
    address, city, state, zipCode, country,
    businessPhone, businessPhone2, businessFax, assistantPhone, homePhone, mobile, pager,
    notes, password
) values (
    'dwalker', 'Debbie', null, 'Walker',
    'Project Manager', 'Example Corp.', 'Services', 'Austin', 'Alex',
    '10699 Parmer Ln.', 'Austin', 'TX', '78717', 'US',
    '512-123-4567', '512-123-4568', '512-123-4569', '512-123-4560', '512-123-4561', '512-123-4562', '512-123-4563',
    "I'm out of the office today. In case of emergency please contact my manager Jim Stockton at 512-123-4567.",
    'dw4lk3r'
);

insert into users (
    username, firstName, initials, lastName,
    title, company, department, office, assistant,
    address, city, state, zipCode, country,
    businessPhone, businessPhone2, businessFax, assistantPhone, homePhone, mobile, pager,
    notes, password
) values (
    'alange', 'Andy', null, 'Lange',
    'Software Developer', 'Example Corp.', 'Services', 'Austin', null,
    '1100 Riverside Dr.', 'Austin', 'TX', '78704', 'US',
    '512-345-6789', null, '512-345-6781', null, '512-345-6783', '512-345-6784', '512-345-6785',
    null, '4l4ng3'
);

insert into relationships (manager, report) values ('jstockton', 'dwalker');
insert into relationships (manager, report) values ('jstockton', 'swhite');
insert into relationships (manager, report) values ('dwalker', 'alange');
insert into relationships (manager, report) values ('dwalker', 'jfarmer');
