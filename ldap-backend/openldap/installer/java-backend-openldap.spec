Summary: ${product.title} for OpenLDAP
Name: ${product.name}-openldap
Version: ${product.version}
Release: ${rpm.release}
License: GPL
Vendor: ${product.vendor}
Group: System Environment/Base
Source: ${product.fullName}-src.tar.gz
BuildRoot: /var/tmp/${product.fullName}
AutoReqProv: no

%description
LDAP backend is a sub-project of Penrose which provides an extension for
native LDAP directory server such as OpenLDAP and Fedora Directory Server
to run a Java program as its backend. LDAP backend is written in C and
uses JNI to invoke Java. LDAP backend can be used by any Java program,
not just Penrose.

%prep
%setup -q

%build
ant -Dprefix=${rpm.prefix}$RPM_BUILD_ROOT -Dopenldap.version=${openldap.version} dist docs

%install
ant -Dprefix=${rpm.prefix}$RPM_BUILD_ROOT -Dopenldap.version=${openldap.version} install

%post
cat << EOF
${product.title} ${product.version} for OpenLDAP has been installed in /usr/local/${product.fullName}.
EOF

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/usr/local/${product.fullName}
