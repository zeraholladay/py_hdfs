.PHONY: build
build:
	python setup.py build_ext --inplace

test: build
	nosetests

clean:
	-rm py_hdfs.c py_hdfs.so *.pyc *.log
	-rm -rf build/

all: build
	@echo Done
