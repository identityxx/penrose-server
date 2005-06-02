Penrose Virtual Directory Server
--------------------------------
Version 0.9.1
Copyright (c) Verge Labs, LLC.

Overview
--------

Penrose is an open source java-based virtual directory server based on Apache Directory server.
A Virtual Directory does not store any information itself, unlike other LDAP implementations.
Requests received from LDAP client applications are processed by the Virtual Directory Server
and passed on to the data source hosting the desired data. Frequently this data source will be
a relational database, and more often than not it will be the authoritative source of the directory
information.

Documentation
-------------

Please find Penrose documentation online at http://penrose.safehaus.org/Documentation.

Getting the Source Code
-----------------------

Checkout the project from:

   svn co https://svn.safehaus.org/repos/penrose/trunk

Building
--------

To build Penrose execute the following command:

   ant dist

The distribution files can be found under the target directory.
