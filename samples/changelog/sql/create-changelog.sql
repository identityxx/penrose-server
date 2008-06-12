create table changelog (
    changeNumber int(11) auto_increment,
    targetDN text,
    changeType varchar(255),
    changes text,
    newRDN text,
    deleteOldRDN boolean,
    newSuperior text,
    primary key (changeNumber)
);
