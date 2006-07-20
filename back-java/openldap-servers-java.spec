Summary: OpenLDAP Java Backend
Name: ${project.name}
Version: ${project.version}
Release: ${rpm.releases}
License: GPL
Vendor: Identyx, Inc.
Group: System Environment/Base
Source: ${project.name}-${project.version}-src.tar.gz
BuildRoot: /var/tmp/${project.name}-${project.version}
AutoReqProv: no

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

