INTRODUCTION
------------

This partition maps data from database tables into entries in LDAP tree in real-time. 

INSTALLATION
------------

Install the jdbc and changelog examples.
Copy the entire PENROSE_SERVER_HOME/samples/jdbc_cache folder into a new folder in PENROSE_SERVER_HOME/partitions.
Prepare 3 databases:
 - jdbc
 - jdbc_cache
 - changelog
Initialize the databases using the sql/create.sql, sql/create-cache.sql, and create-changelog.sql scripts respectively.
Edit DIR-INF/connections.xml if necessary.

EXAMPLE
-------

Insert some entries into the original tables using sql/insert.sql script. The new data will appear in the LDAP tree
after cache refresh every 30 seconds.