# vim:ft=spec

%define file_prefix M4_FILE_PREFIX
%define file_ext M4_FILE_EXT
%define file_version M4_FILE_VERSION
%define file_release_tag %{nil}M4_FILE_RELEASE_TAG
%define file_release_number M4_FILE_RELEASE_NUMBER
%define file_build_number M4_FILE_BUILD_NUMBER
%define file_commit_ref M4_FILE_COMMIT_REF

%define user_name upload_rest_api
%define user_group upload_rest_api
%define user_gid 336
%define user_uid 336

Name:           python3-upload-rest-api
Version:        %{file_version}
Release:        %{file_release_number}%{file_release_tag}.%{file_build_number}.git%{file_commit_ref}%{?dist}
Summary:        REST API for file uploads to passipservice.csc.fi
Group:          Applications/Archiving
License:        LGPLv3+
URL:            http://www.csc.fi
Source0:        %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}.%{file_ext}
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch
Requires:       python3
Requires:       python36-flask
Requires:       python36-pymongo
Requires:       python3-mongoengine
Requires:       python3-magic
Requires:       python3-rq
Requires:       python3-metax-access
Requires:       python3-archive-helpers
Requires:       python3-flask-tus-io
Requires:       python3-rehash
Requires:       python36-click
BuildRequires:  python3-setuptools
BuildRequires:  python36-setuptools_scm
BuildRequires:  python36-pytest
BuildRequires:  python3-mongobox
BuildRequires:  python36-mock
BuildRequires:  python3-fakeredis
BuildRequires:  python3-requests-mock
BuildRequires:  python36-pytest-catchlog
BuildRequires:  python3-pytest-mock

%description
REST API for file uploads to passipservice.csc.fi

%prep
%setup -n %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}

%build

%pre
getent group %{user_group} >/dev/null || groupadd -f -g %{user_gid} -r %{user_group}
if ! getent passwd %{user_name} >/dev/null ; then
    if ! getent passwd %{user_uid} >/dev/null ; then
      useradd -r -m -K UMASK=0027 -u %{user_uid} -g %{user_group} -s /sbin/nologin -c "upload-rest-api user" -d /var/lib/%{user_name} %{user_name}
    else
      useradd -r -g %{user_group} -s /sbin/nologin -c "upload-rest-api user" %{user_name}
    fi
fi

usermod -aG %{user_group} %{user_name}

%install
rm -rf $RPM_BUILD_ROOT
make install PREFIX="%{_prefix}" DESTDIR="%{buildroot}" SETUPTOOLS_SCM_PRETEND_VERSION=%{file_version}
mkdir -p %{buildroot}/var/spool/upload

# Rename executable to prevent name collision with Python 2 RPM
sed -i 's/\/bin\/upload-rest-api$/\/bin\/upload-rest-api-3/g' INSTALLED_FILES

mv %{buildroot}%{_bindir}/upload-rest-api %{buildroot}%{_bindir}/upload-rest-api-3

%post
chown %{user_name}:%{user_group} /var/lib/%{user_name}
chmod 770 /var/lib/%{user_name}

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root,-)
%config(noreplace) /etc/upload_rest_api.conf
%attr(-,upload_rest_api,upload_rest_api) /var/spool/upload

# TODO: For now changelog must be last, because it is generated automatically
# from git log command. Appending should be fixed to happen only after %changelog macro
%changelog
