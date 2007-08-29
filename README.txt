${product.title} Server
--------------
Version ${product.version}
Copyright (c) 2000-2006, Identyx Corporation.

Overview
--------

${product.title} is an open source java-based virtual directory server. A virtual directory does not store
any information itself, unlike other LDAP implementations. Requests received from LDAP client
applications are processed by the virtual directory server and passed on to the data source hosting
the desired data. Frequently this data source will be a relational database, and more often than not
it will be the authoritative source of the directory information.

Documentation
-------------

Please find ${product.title} documentation online at http://penrose.safehaus.org/Documentation.

Getting the Source Code
-----------------------

Checkout the project from:

   svn co https://svn.safehaus.org/repos/penrose/trunk

Building
--------

To build ${product.title} execute the following command:

   ant dist

The distribution files can be found under the target directory.
