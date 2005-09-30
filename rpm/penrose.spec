Summary: Penrose Virtual Directory Server
Name: penrose
Version: 0.9.6
Release: 1
Copyright: OSL 2.1
Group: System Environment/Base
Source: http://penrose.safehaus.org/download/penrose-0.9.6-src.tar.gz
BuildRoot: /var/tmp/%{name}-buildroot

%description
Penrose Virtual Directory Server
http://penrose.safehaus.org

%prep
%setup -q

%build
ant dist

%install
ant -Dprefix=$RPM_BUILD_ROOT install

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/etc/init.d/penrose
/usr/local/penrose
