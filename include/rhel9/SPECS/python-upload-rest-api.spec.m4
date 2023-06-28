# vim:ft=spec

%define file_prefix M4_FILE_PREFIX
%define file_ext M4_FILE_EXT

%define file_version M4_FILE_VERSION
%define file_release_tag %{nil}M4_FILE_RELEASE_TAG
%define file_release_number M4_FILE_RELEASE_NUMBER
%define file_build_number M4_FILE_BUILD_NUMBER
%define file_commit_ref M4_FILE_COMMIT_REF

Name:           python-upload-rest-api
Version:        %{file_version}
Release:        %{file_release_number}%{file_release_tag}.%{file_build_number}.git%{file_commit_ref}%{?dist}
Summary:        REST API for file uploads to passipservice.csc.fi
License:        LGPLv3+
URL:            http://www.digitalpreservation.fi
Source0:        %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}.%{file_ext}
BuildArch:      noarch


BuildRequires:  python3-devel
BuildRequires:  pyproject-rpm-macros
BuildRequires:  %{py3_dist fakeredis[lua]}
BuildRequires:  %{py3_dist mongobox}
BuildRequires:  %{py3_dist pip}
BuildRequires:  %{py3_dist pytest}
BuildRequires:  %{py3_dist pytest-mock}
BuildRequires:  %{py3_dist requests_mock}
BuildRequires:  %{py3_dist setuptools}
BuildRequires:  %{py3_dist setuptools_scm}
BuildRequires:  %{py3_dist wheel}

%global _description %{expand:
REST API for file uploads to passipservice.csc.fi
}

%description %_description

%package -n python3-upload-rest-api
Summary: %{summary}
Requires: %{py3_dist metax-access}
Requires: %{py3_dist archive-helpers}
Requires: %{py3_dist flask_tus_io}

%description -n python3-upload-rest-api %_description

%prep
%autosetup -n %{file_prefix}-v%{file_version}%{?file_release_tag}-%{file_build_number}-g%{file_commit_ref}

%build
export SETUPTOOLS_SCM_PRETEND_VERSION=%{file_version}
%pyproject_wheel

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
%pyproject_install
%pyproject_save_files upload_rest_api

# Copy config file to /etc/upload_rest_api.cfg with correct permissions
install -D -m 0644 include/etc/upload_rest_api.conf %{buildroot}%{_sysconfdir}/upload_rest_api.conf

# TODO: executables with "-3" suffix are added to maintain compatibility with our systems.
# executables with "-3" suffix should be deprecated.
cp %{buildroot}%{_bindir}/upload-rest-api %{buildroot}%{_bindir}/upload-rest-api-3

%post
chown %{user_name}:%{user_group} /var/lib/%{user_name}
chmod 770 /var/lib/%{user_name}

%files -n python3-upload-rest-api -f %{pyproject_files}
%{_bindir}/upload-rest-api
%{_bindir}/upload-rest-api-3
%config(noreplace) %{_sysconfdir}/upload_rest_api.conf
%license LICENSE
%doc README.rst

# TODO: For now changelog must be last, because it is generated automatically
# from git log command. Appending should be fixed to happen only after %changelog macro
%changelog
