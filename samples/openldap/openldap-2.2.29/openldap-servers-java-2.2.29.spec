Summary: OpenLDAP Java Backend
Name: ${project.name}
Version: ${project.version}
Release: 1.FC3
License: GPL
Vendor: Identyx, Inc.
Group: System Environment/Base
Source: http://dist.safehaus.org/penrose/release/penrose-1.1/artifacts/${project.name}-${project.version}-src.tar.gz
BuildRoot: /var/tmp/${project.name}-${project.version}

%description

%prep
%setup -q

%build

%install
ant -Dprefix=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/usr/sbin/openldap

