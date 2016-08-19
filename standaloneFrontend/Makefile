.PHONY: clean

UGLIFY := $(shell command -v uglifyjs 2> /dev/null)
CLEANCSS := $(shell command -v cleancss 2> /dev/null)

all: minified


assets:
	mkdir -p bin/assets

	cp -r ./src/assets/images bin/assets
	cp ./src/assets/mongobrowser.tpl bin/assets


minified: withDeps withoutDeps
ifndef UGLIFY
	$(warning "==============================================================")
	$(warning "=== UGLIFY NOT INSTALLED -- FALLING BACK TO ONLINE-SERVICE ===")
	$(warning "==============================================================")
	curl -X POST -s --data-urlencode 'input@./bin/mongobrowser.js' https://javascript-minifier.com/raw > ./bin/mongobrowser.min.js
	curl -X POST -s --data-urlencode 'input@./bin/mongobrowser-withDependencies.js' https://javascript-minifier.com/raw > ./bin/mongobrowser-withDependencies.min.js
else
	uglifyjs bin/mongobrowser.js -c -m -o bin/mongobrowser.min.js
	uglifyjs bin/mongobrowser-withDependencies.js -c -m -o bin/mongobrowser-withDependencies.min.js
endif

ifndef UGLIFY
	$(warning "================================================================")
	$(warning "=== CLEANCSS NOT INSTALLED -- FALLING BACK TO ONLINE-SERVICE ===")
	$(warning "================================================================")
	curl -X POST -s --data-urlencode 'input@./bin/mongobrowser.css' https://cssminifier.com/raw > ./bin/mongobrowser.min.css
	curl -X POST -s --data-urlencode 'input@./bin/mongobrowser-withDependencies.css' https://cssminifier.com/raw > ./bin/mongobrowser-withDependencies.min.css
else
	cleancss -o bin/mongobrowser.min.css bin/mongobrowser.css
	cleancss -o bin/mongobrowser-withDependencies.min.css bin/mongobrowser-withDependencies.css
endif


withDeps: assets mongoShell
	awk 'FNR==1{print ""}1' \
		./src/assets/lib/codemirror/lib/codemirror.js \
		./src/assets/lib/codemirror/mode/javascript/javascript.js \
		./src/assets/lib/codemirror/addon/edit/matchbrackets.js \
		./src/assets/lib/codemirror/addon/hint/show-hint.js \
		./src/assets/lib/jquery-2.2.3.min.js \
		./src/assets/lib/jquery-ui-1.11.4.custom/jquery-ui.min.js \
		./src/assets/lib/jquery-contextMenu/jquery.contextMenu.min.js \
		./src/assets/lib/jquery-contextMenu/jquery.ui.position.min.js \
		./src/assets/lib/jquery-resizable-columns/jquery.resizableColumns.js \
		./src/assets/lib/clipboardjs/clipboard.min.js \
		./src/mongobrowser-tabs.js \
		./src/mongobrowser-guiCreation.js \
		./src/mongobrowser.js | \
		grep -v "//# sourceMappingURL" > bin/mongobrowser-withDependencies.js

	awk 'FNR==1{print ""}1' \
		./src/assets/lib/jquery-ui-1.11.4.custom/jquery-ui.structure.min.css \
		./src/assets/lib/jquery-contextMenu/jquery.contextMenu.min.css \
		./src/assets/lib/jquery-resizable-columns/jquery.resizableColumns.css \
		./src/assets/lib/codemirror/lib/codemirror.css \
		./src/assets/lib/codemirror/addon/hint/show-hint.css \
		./src/mongobrowser.css > bin/mongobrowser-withDependencies.css


withoutDeps: assets mongoShell
	awk 'FNR==1{print ""}1' \
		./src/mongobrowser-tabs.js \
		./src/mongobrowser-guiCreation.js \
		./src/mongobrowser.js > bin/mongobrowser.js

	cp ./src/mongobrowser.css bin/mongobrowser.css


mongoShell:
	mkdir -p bin

	$(MAKE) -C src/mongo-shell
	cp ./src/mongo-shell/mongo-shell.js bin/


clean:
	rm -rf bin