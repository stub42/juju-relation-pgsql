APT_PREREQS=python3-dev python-virtualenv
PROJECT=pgsql
TESTS=tests/

.PHONY: all
all:
	@echo "make clean - Clean all test & doc build artifacts"
	@echo "make docclean - Clean just doc build artifacts"
	@echo "make test - Run tests"
	@echo "make docs - Build html documentation"

.PHONY: clean
clean:
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete
	find . -name '*~' -delete
	rm -rf .venv3
	rm -rf docs/build

.PHONY: docclean
docclean:
	-rm -rf docs/_build

.venv3:
	@echo Processing apt package prereqs
	@for i in $(APT_PREREQS); do dpkg -l | grep -w $$i >/dev/null || sudo apt-get install -y $$i; done
	virtualenv .venv3 --python=python3
	.venv3/bin/pip install -IUr test_requirements.txt

.PHONY: lint
# lint: .venv3
# 	@echo Checking for Python syntax...
# 	.venv3/bin/flake8 $(PROJECT) $(TESTS) \
# 	    && echo Py3 OK
lint:
	flake8 requires.py

# Note we don't even attempt to run tests if lint isn't passing.
.PHONY: test
test: lint test2 test3

.PHONY: test2
test2: .venv
	@echo Starting Py2 tests...
	.venv/bin/nosetests -s --nologcapture tests/

.PHONY: test3
test3: .venv3
	@echo Starting Py3 tests...
	.venv3/bin/nosetests -s --nologcapture tests/

.PHONY: docs
docs: .venv3
	make -C docs html SPHINXBUILD=../.venv3/bin/sphinx-build
