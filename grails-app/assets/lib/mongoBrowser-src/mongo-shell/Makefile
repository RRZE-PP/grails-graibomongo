.PHONY: clean 

MONGO_SOURCES := $(filter-out mongo-shell.js, $(wildcard *.js))

all: mongo-shell.js

mongo-shell.js: $(MONGO_SOURCES)
	./namespaceify.sh -f mongo-shell.js

clean:
	rm mongo-shell.js
