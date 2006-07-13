Summary: Penrose Virtual Directory Server
Name: ${project.name}
Version: ${project.version}
Release: 1
License: GPL
Vendor: Identyx, Inc.
Group: System Environment/Base
Source: http://dist.safehaus.org/penrose/release/${project.name}-${project.version}/artifacts/${project.name}-${project.version}-src.tar.gz
BuildRoot: /var/tmp/${project.name}-${project.version}

%description
Penrose is a Java-based virtual directory server. Virtual directory enables federating (aggregating) identity data from multiple heterogeneous sources like directory, databases, flat files, and web services - real-time - and makes it available to identity consumers via LDAP.
http://penrose.safehaus.org

%prep
%setup -q

%build
ant dist docs

%install
ant -Dprefix=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/usr/local/${project.name}-${project.version}
