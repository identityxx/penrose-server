INTRODUCTION
------------

This partition runs as a proxy of an external LDAP server and provides an account lockout mechanism.
It uses a module to intercept bind requests and counts the number of consecutive bind failures. When the
counter reaches a certain limit, the account will be locked out and all subsequent bind operations will
fail. The lock can be reset manually via script or LDAP interface, or automatically via scheduler.

INSTALLATION
------------

Copy the entire PENROSE_SERVER_HOME/samples/lockout folder into a new folder in PENROSE_SERVER_HOME/partitions.
Prepare an LDAP server. You can use the ldif/init.ldif to populate your LDAP server. Create a database and
initialize it using the sql/create.sql.

CONFIGURATION
-------------

Edit DIR-INF/connections.xml and set the connection parameters to the LDAP server and the database server.

The default naming space used in this example is dc=my-domain,dc=com. If you want to use a different naming
space, change it in DIR-INF/directory.xml and DIR-INF/modules.xml.

By default the module will count up to 3 consecutive bind failures before locking out the account. To change
the limit, edit DIR-INF/modules.xml and set the limit parameter.

By default the lock will automatically expire in 5 minutes. To change the expiration, edit DIR-INF/modules
and set the expiration parameter. To disable expiration, set the value to 0.

EXAMPLE
-------

Attempt bind operation with the correct password, this should work:

ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=Lockout,dc=Example,dc=com" -w 4l4ng3 -x -b "" -s base

Attempt 3 bind operations with a wrong password:

ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=Lockout,dc=Example,dc=com" -w wrong -x -b "" -s base
ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=Lockout,dc=Example,dc=com" -w wrong -x -b "" -s base
ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=Lockout,dc=Example,dc=com" -w wrong -x -b "" -s base

Attempt bind operation with the correct password again, this should fail:

ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=Lockout,dc=Example,dc=com" -w 4l4ng3 -x -b "" -s base

Attempt bind operation with the correct password again, this should fail:

ldapsearch -h localhost -p 10389 -D "uid=alange,ou=Users,dc=Lockout,dc=Example,dc=com" -w 4l4ng3 -x -b "" -s base

LOCK MANAGEMENT VIA SCRIPT
--------------------------

You can view and reset locks using a script located in PENROSE_SERVER_HOME/partitions/lockout/bin.

To view locked accounts:

./lockout.sh -D uid=admin,ou=system -w secret list lockout

Account: uid=alange,ou=users,dc=lockout,dc=example,dc=com
Counter: 3
Timestamp: 2007-09-26 18:13:58

To reset the account:

./lockout.sh -D uid=admin,ou=system -w secret reset lockout "uid=alange,ou=users,dc=lockout,dc=example,dc=com"

LOCK MANAGEMENT VIA LDAP
------------------------

You can view and reset locks using any standard LDAP client.

To view locked accounts:

ldapsearch -h localhost -p 10389 -D uid=admin,ou=system -w -x -b "cn=lockout" -s one

dn: account=uid=\alange\,ou=users\,dc=lockout\,dc=example\,dc=com,cn=lockout
objectClass: extensibleObject
account: uid=alange,ou=users,dc=lockout,dc=example,dc=com
counter: 3
timestamp: 2007-09-26 18:13:58

To reset the account:

ldapdelete -h localhost -p 10389 -D uid=admin,ou=system -w -x "account=uid\=alange\,ou=users\,dc=lockout\,dc=example\,dc=com,cn=lockout"
