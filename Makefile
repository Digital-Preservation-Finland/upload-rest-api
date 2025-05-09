DESTDIR ?= /
PREFIX ?= /usr
ETC = ${DESTDIR}/etc
PYTHON ?= python3

install:
	mkdir -p "${ETC}"

	# Cleanup temporary files
	rm -f INSTALLED_FILES

	# Copy config files
	cp include/etc/upload_rest_api.conf ${ETC}/

	# Use Python setuptools
	python3 ./setup.py install -O1 --prefix="${PREFIX}" --root="${DESTDIR}" --record=INSTALLED_FILES

test-with-db-logging:
	${PYTHON} -m pytest tests -vs --log-queries \
		--junitprefix=upload_rest_api --junitxml=junit.xml

clean: clean-rpm
	find . -iname '*.pyc' -type f -delete
	find . -iname '__pycache__' -exec rm -rf '{}' \; | true
	rm -rf coverage.xml htmlcov junit.xml .coverage

clean-rpm:
	rm -rf rpmbuild

