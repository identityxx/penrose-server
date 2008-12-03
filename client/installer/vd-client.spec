Summary: ${product.title}
Name: ${product.name}
Version: ${product.version}
Release: 1
License: Commercial
Vendor: ${product.vendor}
Group: System Environment/Base
Source: ${product.name}-${product.version}.tar.gz
BuildRoot: /var/tmp/${product.name}-${product.version}

%description
Penrose is a Java-based virtual directory server. Virtual directory enables federating (aggregating) identity data from multiple heterogeneous sources like directory, databases, flat files, and web services - real-time - and makes it available to identity consumers via LDAP.
http://penrose.safehaus.org

%prep
%setup -q
%build

%install
rm -rf $RPM_BUILD_ROOT/opt/${product.name}-${product.version}
mkdir -p $RPM_BUILD_ROOT/opt/${product.name}-${product.version}
cp -R * $RPM_BUILD_ROOT/opt/${product.name}-${product.version}

%post
echo ${product.title} ${product.version} has been installed in /opt/${product.name}-${product.version}.

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/opt/${product.name}-${product.version}
