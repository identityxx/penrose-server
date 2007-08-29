Summary: ${product.title} Server
Name: ${module.name}
Version: ${product.version}
Release: 1
License: GPL
Vendor: Identyx, Inc.
Group: System Environment/Base
Source: ${module.name}-${product.version}-src.tar.gz
BuildRoot: /var/tmp/${module.name}-${product.version}

%description
Penrose is a Java-based virtual directory server. Virtual directory enables federating (aggregating) identity data from multiple heterogeneous sources like directory, databases, flat files, and web services - real-time - and makes it available to identity consumers via LDAP.
http://penrose.safehaus.org

%prep
%setup -q

%build
ant -Dproduct.version=${product.version} -Dprefix=${rpm.prefix}$RPM_BUILD_ROOT docs dist

%install
ant -Dproduct.version=${product.version} -Dprefix=${rpm.prefix}$RPM_BUILD_ROOT install

%post
cat << EOF
${product.title} Server ${product.version} has been installed in /usr/local/${module.name}-${product.version}.
EOF

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/usr/local/${module.name}-${product.version}
