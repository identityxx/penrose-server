Summary: Penrose Server
Name: ${module.name}
Version: ${project.version}
Release: 1
License: GPL
Vendor: Identyx, Inc.
Group: System Environment/Base
Source: http://dist.safehaus.org/penrose/release/${module.name}-${project.version}/artifacts/${project.name}-${project.version}-src.tar.gz
BuildRoot: /var/tmp/${module.name}-${project.version}

%description
Penrose is a Java-based virtual directory server. Virtual directory enables federating (aggregating) identity data from multiple heterogeneous sources like directory, databases, flat files, and web services - real-time - and makes it available to identity consumers via LDAP.
http://penrose.safehaus.org

%prep
%setup -q

%build
ant -Dproject.version=${project.version} -Dprefix=${rpm.prefix}$RPM_BUILD_ROOT dist docs

%install
ant -Dproject.version=${project.version} -Dprefix=${rpm.prefix}$RPM_BUILD_ROOT install

%post
cat << EOF
Penrose Server ${project.version} has been installed in /usr/local/${module.name}-${project.version}.
EOF

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/usr/local/${module.name}-${project.version}
