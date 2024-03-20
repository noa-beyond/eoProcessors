install:
	pip install -e .

install-test:
	pip install .[test]

test:
	pytest

show-coverage:
	coverage html --show-contexts || true
	open htmlcov/index.html
