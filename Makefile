.PHONY: run test conformance

run:
	PYTHONPATH=src python3 -m megacache

test:
	PYTHONPATH=src python3 -m unittest discover -s tests -v

conformance:
	PYTHONPATH=src python3 conformance/run.py --sdk all
