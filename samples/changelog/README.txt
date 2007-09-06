This partition contains a module that captures LDAP operations received by Penrose and stores the changes
in a database. The partition also contains a mapping that presents the data into as a change log subtree.

ldapsearch -h localhost -p 10389 -D uid=admin,ou=system -w secret -x -b "cn=changelog"

dn: cn=changelog
o: changelog
cn: changelog
objectClass: extensibleObject
objectClass: top

dn: changenumber=1,cn=changelog
changeNumber: 1
changes:: c246IFVzZXIKY246IFRlc3QgVXNlcgpvYmplY3RDbGFzczogaW5ldE9yZ1BlcnNvbgpv
 YmplY3RDbGFzczogb3JnYW5pemF0aW9uYWxQZXJzb24Kb2JqZWN0Q2xhc3M6IHBlcnNvbgp1c2VyU
 GFzc3dvcmQ6IHRlc3QKdWlkOiB0ZXN0Cg==
objectClass: changeLogEntry
objectClass: top
targetDN: uid=test,ou=Users,dc=JDBC,dc=Example,dc=com
changeType: add

dn: changenumber=2,cn=changelog
changeNumber: 2
objectClass: changeLogEntry
objectClass: top
targetDN: uid=test,ou=Users,dc=JDBC,dc=Example,dc=com
changeType: delete
