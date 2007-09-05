create table changelog (
    changeNumber int(11) auto_increment,
    targetDN varchar(255),
    changeType varchar(255),
    changes text,
    newRDN varchar(255),
    deleteOldRDN boolean,
    newSuperior varchar(255),
    primary key (changeNumber)
);
