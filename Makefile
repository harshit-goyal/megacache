.PHONY: run test

run:
	PYTHONPATH=src python3 -m megacache

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

