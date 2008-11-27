Installation
------------

The federation example requires an external LDAP server such as OpenLDAP or Red Hat DS.

For OpenLDAP, configure OpenLDAP using the sample configuration provided in the openldap folder.
For Red Hat DS, use ldapadd to add the schema files in the rhds folder.

Use ldapadd to Add the initial entries in the ldif/init.ldif into the LDAP server. Then optionally
add the ldif/nis.ldif.

Next, copy the samples/federation folder into the partitions folder. You can rename the new folder
to something else (e.g. example).

Go to the DIR-INF folder, open connections.xml, edit the connection settings. Open federation.xml,
edit the connection settings the repositories.

Restart Penrose Server.
