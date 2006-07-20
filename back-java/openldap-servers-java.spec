Summary: OpenLDAP Java Backend
Name: ${project.name}
Version: ${project.version}
Release: ${rpm.release}
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
ant -Dprefix=${rpm.prefix}$RPM_BUILD_ROOT -Dopenldap.version=${openldap.version} install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/usr/sbin/openldap
/usr/share/openldap/lib
/usr/share/doc/${project.name}-${project.version}

