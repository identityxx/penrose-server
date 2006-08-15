Summary: OpenLDAP Java Backend
Name: ${module.name}
Version: ${module.version}
Release: ${rpm.release}
License: GPL
Vendor: Identyx, Inc.
Group: System Environment/Base
Source: ${module.name}-${module.version}-src.tar.gz
BuildRoot: /var/tmp/${module.name}-${module.version}
AutoReqProv: no

%description

%prep
%setup -q

%build
ant -Dprefix=${rpm.prefix}$RPM_BUILD_ROOT -Dopenldap.version=${openldap.version} dist docs

%install
ant -Dprefix=${rpm.prefix}$RPM_BUILD_ROOT -Dopenldap.version=${openldap.version} install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/usr/sbin/openldap
/usr/share/openldap/lib
/usr/share/doc/${module.name}-${module.version}

