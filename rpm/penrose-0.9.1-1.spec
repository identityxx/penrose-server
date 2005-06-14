Summary: Penrose Virtual Directory Server
Name: penrose
Version: 0.9.1
Release: 1
Copyright: OSL 2.1
Group: System Environment/Base
Source: http://penrose.safehaus.org/download/penrose-0.9.1-src.tar.gz
BuildRoot: /var/tmp/%{name}-buildroot

%description
Penrose Virtual Directory Server
http://penrose.safehaus.org

%prep
%setup -q

%build

%install
ant -Dprefix=$RPM_BUILD_ROOT install

%clean
#ant -Dprefix=$RPM_BUILD_ROOT uninstall
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)

/usr/local/penrose
