DESTDIR ?= /
PREFIX ?= /usr
ETC=${DESTDIR}/etc

install:
	mkdir -p "${ETC}"

	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Use Python setuptools
	python ./setup.py install -O1 --prefix="${PREFIX}" --root="${DESTDIR}" --record=INSTALLED_FILES

	# Remove requires.txt from egg-info because it contains PEP 508 URL requirements
	# that break siptools-research on systems that use old version of
	# python setuptools (older than v.20.2)
	rm ${DESTDIR}${PREFIX}/lib/python2.7/site-packages/*.egg-info/requires.txt
	sed -i '/\.egg-info\/requires.txt$$/d' INSTALLED_FILES

install3:
	mkdir -p "${ETC}"

	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Copy config files
	cp include/etc/upload_rest_api.conf ${ETC}/

	# Use Python setuptools
	python3 ./setup.py install -O1 --prefix="${PREFIX}" --root="${DESTDIR}" --record=INSTALLED_FILES

test:
	py.test  tests/unit_tests -v \
	    --junitprefix=upload_rest_api --junitxml=junit.xml

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

rpm3: clean
	create-archive.sh
	preprocess-spec-m4-macros.sh include/rhel8
	build-rpm.sh
