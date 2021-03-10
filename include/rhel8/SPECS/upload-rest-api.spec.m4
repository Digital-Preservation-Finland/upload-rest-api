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
%define user_gid 10336
%define user_uid 10336

Name:           upload-rest-api
Version:        %{file_version}
Release:        %{file_release_number}%{file_release_tag}.%{file_build_number}.git%{file_commit_ref}%{?dist}
Summary:        REST API for file uploads to passipservice.csc.fi
Group:          Applications/Archiving
License:        LGPLv3+
URL:            https://www.digitalpreservation.fi
Source0:        %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}.%{file_ext}
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch
Requires:       python3
Requires:       python3-flask
Requires:       python3-pymongo
Requires:       python3-magic
Requires:       python3-six
Requires:       python3-rq
Requires:       metax-access
Requires:       archive-helpers
BuildRequires:  python3-setuptools
BuildRequires:  python3-pytest
BuildRequires:  python3-mongomock
BuildRequires:  python3-mock
# TODO: python3-fakeredis hasn't been packaged yet
# BuildRequires:  python3-fakeredis

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
make install3 PREFIX="%{_prefix}" DESTDIR="%{buildroot}"
mkdir -p %{buildroot}/var/spool/upload

%post
chown %{user_name}:%{user_group} /var/lib/%{user_name}
chmod 770 /var/lib/%{user_name}

%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES
%defattr(-,root,root,-)
%attr(-,upload_rest_api,upload_rest_api) /var/spool/upload

# TODO: For now changelog must be last, because it is generated automatically
# from git log command. Appending should be fixed to happen only after %changelog macro
%changelog
