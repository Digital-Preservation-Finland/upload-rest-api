DESTDIR ?= /
PREFIX ?= /usr
ETC=${DESTDIR}/etc

install:
	mkdir -p "${ETC}"

	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Copy config files
	cp include/etc/upload_rest_api.conf ${ETC}/

	# Use Python setuptools
	python ./setup.py install -O1 --prefix="${PREFIX}" --root="${DESTDIR}" --record=INSTALLED_FILES
	cat INSTALLED_FILES | sed 's/^/\//g' >> INSTALLED_FILES

test:
	py.test  tests/unit_tests -svvvv --junitprefix=upload_rest_api --junitxml=junit.xml \

coverage:
	py.test tests --cov=upload_rest_api --cov-report=html
	coverage report -m
	coverage html
	coverage xml

clean: clean-rpm
	find . -iname '*.pyc' -type f -delete
	find . -iname '__pycache__' -exec rm -rf '{}' \; | true
	rm -rf coverage.xml htmlcov junit.xml .coverage

clean-rpm:
	rm -rf rpmbuild

rpm: clean
	create-archive.sh
	preprocess-spec-m4-macros.sh include/rhel7
	build-rpm.sh
