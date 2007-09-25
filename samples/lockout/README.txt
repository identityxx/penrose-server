INTRODUCTION
------------

This partition runs as a proxy of an external LDAP server and provides an account lockout mechanism.
It uses a module to intercept bind requests and records the number of bind failures.
If the number of failures reaches a certain number, the account will be locked out and all subsequent bind operations
will fail until the account is reset. This example also includes a script to view and reset the locked accounts.

INSTALLATION
------------

Copy the entire PENROSE_SERVER_HOME/samples/lockout folder into a new folder in PENROSE_SERVER_HOME/partitions.

Prepare an LDAP server. You can use the ldif/init.ldif to populate your LDAP server. Edit DIR-INF/connections.xml
and enter the connection parameters to the LDAP server.

Create a lockout database and initialize it using the sql/create.sql. Edit DIR-INF/connections.xml and enter the
connection parameters to the database server.

The default naming space used in this example is dc=my-domain,dc=com. If you want to use a different naming space,
change it in DIR-INF/mapping.xml and DIR-INF/modules.xml.

By default the module will count up to 3 bind failures before locking out an account. To change this limit, edit
DIR-INF/modules.xml and edit the maxAttempts parameter.

EXAMPLE
-------

Attempt bind operation with the correct password, this should work:

ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=my-domain,dc=com" -w 4l4ng3 -x -b "" -s base

Attempt 3 bind operations with a wrong password:

ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=my-domain,dc=com" -w wrong -x -b "" -s base
ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=my-domain,dc=com" -w wrong -x -b "" -s base
ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=my-domain,dc=com" -w wrong -x -b "" -s base

Attempt bind operation with the correct password again, this should fail:

ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=my-domain,dc=com" -w 4l4ng3 -x -b "" -s base

View locked accounts:

cd PENROSE_SERVER_HOME/partitions/lockout/bin

./lockout.sh -D uid=admin,ou=system -w secret list lockout
Locked accounts:
 - uid=alange,ou=users,dc=my-domain,dc=com

Reset account:

./lockout.sh -D uid=admin,ou=system -w secret reset lockout "uid=alange,ou=users,dc=my-domain,dc=com"

Attempt bind operation with the correct password again, this should work:

ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=my-domain,dc=com" -w 4l4ng3 -x -b "" -s base
