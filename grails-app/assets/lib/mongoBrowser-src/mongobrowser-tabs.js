/*
 * This file contains the classes ConnectionTab and TabFactory
 * which are exported to the MongoBrowserNS namespace. It will eventually be
 * imported in mongobrowser.js, its members included there and the NS deleted
 * thereafter.
 */

window.MongoBrowserNS = (function(MongoBrowserNS){

	/**
	 * Represents a tab in the GUI
	 * @class ConnectionTab
	 *
	 * @param {String} prefix - a prefix to prepend before the name to make it unique
	 *                          across {@link MongoBrowser } instances
	 * @param {JQuery} dummyLink - a jQuery wrapped <tt>HTMLElement</tt> (LI) to append as tab handle. Must
	 *                             contain a .tabText to put the tab title in
	 * @param {JQuery} dummyTab - a jQuery wrapped <tt>HTMLElement</tt> to append as tab content
	 * @param {MongoNS.DB} database - the database to which db should equate in this tab
	 * @param {string} collection - the default collection to use
	 * @param {MongoBrowser~options} options - the default options which were passed to the instantiating mongobrowser
	 */
	function ConnectionTab(prefix, dummyLink, dummyTab, database, collection, options){
		this.uiElements = {};
		this.state = {};
		this.options = options;

		var link = this.uiElements.link = dummyLink.clone();
		var tab = this.uiElements.tab = dummyTab.clone();
		var id = prefix+"_"+ConnectionTab.instances++;
		var connection = database.getMongo().host.substr(0, database.getMongo().host.indexOf("/"));
		var defaultPrompt = "db.getCollection(\""+collection+"\").find({})";

		link.find("a").attr("href", "#"+id);
		tab.attr('id', id);

		var ui = this.uiElements = {title: link.find(".tabText"),
									   info: {
									    	connection: tab.find(".info .connection span"),
									    	database: tab.find(".info .database span"),
									    	collection: tab.find(".buttonBar .collection span"),
									    	time: tab.find(".buttonBar .time span"),
									   },
									   prompt: tab.find(".prompt textarea"),
									   tab: this.uiElements.tab,
									   link: this.uiElements.link,
									   printContainer: tab.find(".printContainer"),
									   printedLines: tab.find(".printContainer pre"),
									   resultsTable: tab.find(".resultsTable"),
									   results: tab.find(".resultsTable tbody"),
									   iterate: {
									    	max: tab.find(".maxIterate"),
									    	start: tab.find(".startIterate"),
									    	prev: tab.find(".prevIterate"),
									    	next: tab.find(".nextIterate")
									   }
									}

		ui.info.connection.text(connection);
		ui.info.database.text(database.toString());
		ui.info.collection.text(collection);
		ui.prompt.val(defaultPrompt);
		this.setTitle(defaultPrompt);

		//calling this here fixes problems with minWidth
		ui.resultsTable.resizableColumns({minWidth: 15});

		var codeMirror = CodeMirror.fromTextArea(ui.prompt[0],
			{mode: {name: "javascript", mongo: true},
			 extraKeys: {"Ctrl-Space": "autocomplete"},
			 hintOptions: {hint: MongoNS.mongoDBHintAdapter, connectedTab: this},
			 matchBrackets: true
			});
		codeMirror.on("focus", (function(self){return function(){self.uiElements.prompt.focus()}})(this));

		ui.iterate.prev.on("click", (function(self){return function(){self.prevBatch()}})(this));
		ui.iterate.next.on("click", (function(self){return function(){self.nextBatch()}})(this));

		this.state.id = id;
		this.state.db = database
		this.state.collection = collection;
		this.state.displayedResult = [];
		this.state.codeMirror = codeMirror;

	}

	/** The total number of Connection Tabs created to savely create unique IDs
	 * @static
	 * @memberof ConnectionTab
	 * @type {number}
	 */
	ConnectionTab.instances = 0;

	/**
	 * Appends this tab to the GUI
	 *
	 * @method
	 * @memberof ConnectionTab
	 * @param {JQuery} parent - a jQuery wrapped <tt>HTMLElement</tt> to this tab to. Must contain
	 *                          a ul.tabList to append the handle to
	 */
	ConnectionTab.prototype.appendTo = function(parent){
		parent.append(this.uiElements.tab);
		parent.children(".tabList").append(this.uiElements.link);

		this.state.codeMirror.refresh();
		parent.tabs("refresh");

		//select the first tab, if no tab was selected before
		if(parent.children(".tabList").children().size() === 1)
			parent.tabs("option", "active", 0);
	}

	/**
	 * Execute the code from the prompt or the given code snippet within the MongoNS namespace
	 * on this tab's db
	 *
	 * @param {string} [code] - if provided, this code will be executed, else the content of the prompt
	 * @method
	 * @memberof ConnectionTab
	 */
	ConnectionTab.prototype.execute = function(code){
		var self = this;

		var startTime = $.now();
		var printedLines = [];
		var code = typeof code !== "undefined" ? code : this.state.codeMirror.getDoc().getValue();

		this.state.currentCursor = null;
		this.state.currentQuery = null;
		this.uiElements.info.collection.parent().hide();

		//handle special cases
		var isUseCommand = code.match(/^ *use +([^$. ][^. ]*)$/)
		if(isUseCommand != null){
			//use db
			var ret = executeUseCommand(isUseCommand[1]);
		}else{
			var ret = executeQuery(code);
		}

		printExecutionResult(startTime, ret);

		function executeUseCommand(newDb){
			code = "db = db.getSiblingDB('" + newDb.replace("'", "\'") + "')";
			self.state.db = MongoNS.execute(MongoNS, self.state.db, code);

			self.uiElements.info.database.text(newDb);
			self.uiElements.info.collection.parent().hide();
			printedLines.push("Switched to db " + newDb);

			return {__magicNoPrint: true};
		}

		function executeQuery(code){
			var ret;
			try {
				ret = MongoNS.execute(MongoNS, self.state.db, code);

				if(ret instanceof MongoNS.DBQuery){
					self.state.collection = ret._collection._shortName;
					self.state.db = ret._db;
					self.state.currentQuery = ret;

					self.uiElements.info.database.text(ret._db._name);
					self.uiElements.info.collection.text(ret._collection._shortName);
					self.uiElements.info.collection.parent().show();
					ret = ret._exec();
				}

				if(ret instanceof MongoNS.WriteResult){
					printedLines.push(ret.toString());
					ret.__magicNoPrint = 1;
				}
			}catch(e){
				ret = undefined;
				printedLines.push(e.toString());
			}
			return ret;
		}


		function printExecutionResult(startTime, result){
			var duration = $.now() - startTime;
			self.uiElements.info.time.text(duration/1000);

			self.uiElements.printContainer.hide()
			self.uiElements.resultsTable.hide();
			self.uiElements.printedLines.text("");
			self.uiElements.results.children().remove();
			self.uiElements.iterate.start.val(0);

			if(typeof result !== "undefined" && result !== null && typeof result.__magicNoPrint === "undefined"){
				self.uiElements.resultsTable.show();

				if(result instanceof MongoNS.Cursor){
					printBatch(self, result, parseInt(self.uiElements.iterate.max.val()));
				}else if(result instanceof MongoNS.DBCommandCursor){
					printBatch(self, result, result.objsLeftInBatch());
				}else{
					self.state.displayedResult = [result];
					printLine(self, "", "(" + 1 + ")", result, 0).attr("data-index", 0);
				}
				if(self.options.expandFirstDoc)
					self.uiElements.results.children().eq(0).trigger("dblclick"); //expand the first element
				self.uiElements.results.children("[data-indent]").each(function(index, elem){
					$(elem).children().eq(0).css("padding-left", parseInt($(elem).attr("data-indent"))*25+"px");
				});

			}else if(printedLines.length === 0){
				printedLines.push("Script executed successfully but there is no output to display.")
			}

			if(printedLines.length !== 0){
				printText(self, printedLines);
			}

			self.uiElements.resultsTable.find("th").css("width", "");
			self.uiElements.resultsTable.resizableColumns("destroy");
			self.uiElements.resultsTable.prev(".resizableColumnsFix").remove();
			self.uiElements.resultsTable.resizableColumns({minWidth: 15});
			self.uiElements.resultsTable.prev().wrap($("<div class='resizableColumnsFix' style='width:0px;'></div>"))

			self.setTitle(self.state.codeMirror.getDoc().getValue());
		}
	}

	/**
	 * Prints a batch of entries from the given cursor to the results table. Clears the table before printing.
	 *
	 * @param {ConnectionTab} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @param {MongoNS.Cursor} cursor - the cursor to take elements from
	 * @param {number} count - the size of the batch
	 */
	function printBatch(self, cursor, count){
		self.uiElements.results.children().remove();

		self.state.displayedResult = [];
		try {
			for(var i=0; i < count && cursor.hasNext(); i++){
				var val = cursor.next();
				self.state.displayedResult.push(val);
				var displayedKey = "(" + (i + 1) + ")";
				if(val._id instanceof MongoNS.ObjectId)
					displayedKey += " " + val._id.toString();
				var lines = printLine(self, "", displayedKey, val, 0);
				lines.attr("data-index", i);
			}
			self.state.currentCursor = cursor;
		}catch(e) {
			printText(self, e.toString());
		}
	}

	/**
	 * Prints a line to the results table. A line represents an object or primitive object.
	 *
	 * @param {ConnectionTab} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @param {string} key - the key in the parent object or array
	 * @param {string} displayedKey - the key in the parent object or array as it should be shown to the user (usually the same as key)
	 * @param {anything} val - the value to display. can be any datatype
	 * @param {number} indent - the indendation depth of this line (for root objects this should be 0)
	 * @private
	 * @memberof MongoBrowser(NS)~
	 */
	function printLine(self, key, displayedKey, val, indent) {
		function base_print(indent, image, alt, col1, col2, col3, hasChildren, key) {
			var newLine = $("<tr data-indent='" + indent + "' data-key='" + key + "' class='collapsed " + (hasChildren ? "hasChildren" : "") + "' \
				style='"+ (indent > 0 ? "display:none" : "") + "'> \
				<td><span class='foldIcon'>&nbsp;</span> \
					<img src='" + self.options.assetPrefix + "assets/images/" + image + "' class='typeIcon' alt='" + alt + "' /> " +
					col1 +
				"</td> \
				<td>" + col2 + "</td> \
				<td>" + col3 + "</td></tr>");
			newLine.appendTo(self.uiElements.results);
			return newLine;
		}

		function printObject(key, displayedKey, val, indent) {
			var keys = Object.keys(val);

			var ret = base_print(indent, "bson_object_16x16.png", "object", displayedKey, "{ " + keys.length + " fields }", "Object", keys.length !== 0, key);

			for(var i=0; i<keys.length; i++){
				var newLine = printLine(self, keys[i], keys[i], val[keys[i]], indent + 1);
				ret = ret.add(newLine);
			}
			return ret;
		}

		function printArray(key, displayedKey, val, indent) {
			var keys = Object.keys(val);

			var ret = base_print(indent, "bson_array_16x16.png", "array", displayedKey, "[ " + val.length + " Elements ]", "Array", keys.length !== 0, key);

			for(var i=0; i<keys.length; i++){
				var newLine = printLine(self, keys[i], "[" + keys[i] + "]", val[keys[i]], indent + 1);
				ret = ret.add(newLine);
			}

			return ret;
		}

		function printObjectId(key, displayedKey, val, indent) {
			return base_print(indent, "bson_unsupported_16x16.png", "oid", displayedKey, val.toString(), "ObjectId", false, key);
		}

		function printRegExp(key, displayedKey, val, indent) {
			return base_print(indent, "bson_unsupported_16x16.png", "regex", displayedKey, val.toString(), "Regular Expression", false, key);
		}

		function printDate(key, displayedKey, val, indent) {
			return base_print(indent, "bson_datetime_16x16.png", "date", displayedKey, val.toString(), "Date", false, key);
		}

		function printString(key, displayedKey, val, indent) {
			return base_print(indent, "bson_string_16x16.png", "string", displayedKey, val, "String", false, key);
		}

		function printDouble(key, displayedKey, val, indent) {
			return base_print(indent, "bson_double_16x16.png", "double", displayedKey, val, "Double", false, key);
		}

		function printInt(key, displayedKey, val, indent) {
			return base_print(indent, "bson_integer_16x16.png", "int", displayedKey, val, "Int32", false, key);
		}

		function printLong(key, displayedKey, val, indent) {
			return base_print(indent, "bson_integer_16x16.png", "long", displayedKey, val.toString(), "Int64", false, key);
		}

		function printBoolean(key, displayedKey, val, indent) {
			return base_print(indent, "bson_bool_16x16.png", "boolean", displayedKey, val, "Boolean", false, key);
		}

		function printNull(key, displayedKey, val, indent) {
			return base_print(indent, "bson_null_16x16.png", "null", displayedKey, "null", "Null", false, key);
		}

		function printUndefined(key, displayedKey, val, indent) {
			return base_print(indent, "bson_unsupported_16x16.png", "undefined", displayedKey, "undefined", "Undefined", false, key);
		}

		function printUnsupported(key, displayedKey, val, indent) {
			return base_print(indent, "bson_unsupported_16x16.png", "unsupported", displayedKey, "", "unsupported", false, key);
		}

		if(val instanceof Array)
			return printArray(key, displayedKey, val, indent);
		else if(val instanceof MongoNS.ObjectId)
			return printObjectId(key, displayedKey, val, indent);
		else if(val instanceof MongoNS.NumberLong)
			return printLong(key, displayedKey, val, indent);
		else if(val instanceof RegExp)
			return printRegExp(key, displayedKey, val, indent);
		else if(val instanceof Date)
			return printDate(key, displayedKey, val, indent);
		else if(typeof val === "string" || val instanceof String)
			return printString(key, displayedKey, val, indent);
		else if((typeof val === "number" || val instanceof Number) && parseInt(val) === val)
			return printInt(key, displayedKey, val, indent);
		else if(typeof val === "number" || val instanceof Number)
			return printDouble(key, displayedKey, val, indent);
		else if(typeof val === "boolean")
			return printBoolean(key, displayedKey, val, indent);
		else if(val === null) //TODO: Int vs Double!
			return printNull(key, displayedKey, val, indent);
		else if(typeof val === "undefined")
			return printUndefined(key, displayedKey, val, indent);
		else if(typeof val === "object") //this comes last after all others have been ruled out
			return printObject(key, displayedKey, val, indent);
		else
			return printUnsupported(key, displayedKey, val, indent); //should not happen
	}

	/**
	 * Prints text to the result table.
	 *
	 * @param {ConnectionTab} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @param {string|string[]} text - the text to print or an array of strings, which will be interpreted as lines
	 * @private
	 * @memberof MongoBrowser(NS)~
	 */
	function printText(self, text){
		self.uiElements.printContainer.show();

		if(text instanceof Array){
			var textString = "";
			for(var i=0; i < text.length; i++){
				textString += text[i] + "\n";
			}
			text = textString;
		}
		self.uiElements.printedLines.text(text);
	}



	/**
	 * Return information about this tab
	 * @method
	 * @memberof ConnectionTab
	 * @returns {database: MongoNS.DB, collection: String} this tab's DB and collection
	 */
	ConnectionTab.prototype.getInfo = function(){
		return {database: this.state.db,
				collection: this.state.collection}
	}

	/**
	 * Return the data row of the currently displayed data with index x (counting only top-level rows)
	 * @method
	 * @memberof ConnectionTab
	 * @param {number} idx - the index to get
	 * @returns {object|null} the object or null if the row does not exist (no data or index too large)
	 */
	ConnectionTab.prototype.getDataRow = function(idx){
		if(idx >= this.state.displayedResult.length)
			return null;
		return this.state.displayedResult[idx];

	}

	/**
	 * Return this tab's id
	 * @method
	 * @memberof ConnectionTab
	 * @returns {string} the id of this tab
	 */
	ConnectionTab.prototype.id = function(){
		return this.state.id;
	}

	/**
	 * Gets the next batch of results from the cursor
	 */
	ConnectionTab.prototype.nextBatch = function(){
		var start = parseInt(this.uiElements.iterate.start.val());
		var count = parseInt(this.uiElements.iterate.max.val());

		if(this.state.currentCursor === null)
			return;

		printBatch(this, this.state.currentCursor, count);

		this.uiElements.iterate.start.val(start + count);

		this.uiElements.results.children().eq(0).trigger("dblclick"); //expand the first element
		this.uiElements.results.children("[data-indent]").each(function(index, elem){
			$(elem).children().eq(0).css("padding-left", parseInt($(elem).attr("data-indent"))*25+"px");
		});
	}

	/**
	 * Gets the previous batch of results from the cursor
	 */
	ConnectionTab.prototype.prevBatch = function(){
		var start = parseInt(this.uiElements.iterate.start.val());
		var count = parseInt(this.uiElements.iterate.max.val());
		var newStart = start - count < 0 ? 0 : start - count;

		var curQuery = this.state.currentQuery;

		if(curQuery === null)
			return;

		var origSkip = curQuery.origSkip || curQuery._skip; //save skip-value, in case the user called .skip()

		curQuery = curQuery.clone();
		this.state.currentQuery.origSkip = origSkip;

		curQuery.skip(origSkip + newStart);

		this.state.currentCursor = curQuery._exec();
		printBatch(this, this.state.currentCursor, count);

		this.uiElements.iterate.start.val(newStart);

		this.uiElements.results.children().eq(0).trigger("dblclick"); //expand the first element
		this.uiElements.results.children("[data-indent]").each(function(index, elem){
			$(elem).children().eq(0).css("padding-left", parseInt($(elem).attr("data-indent"))*25+"px");
		});
	}

	/**
	 * Select this tab to be the current tab
	 * @method
	 * @memberof ConnectionTab
	 */
	ConnectionTab.prototype.select = function(){
		this.uiElements.link.children("a").click();
	}

	/**
	 * Sets the title of the tab and updates the tooltip
	 * @method
	 * @memberof ConnectionTab
	 */
	ConnectionTab.prototype.setTitle = function(newTitle){
		if(newTitle.length > 23)
			this.uiElements.title.text(newTitle.substr(0, 20) + "...");
		else
			this.uiElements.title.text(newTitle);

		this.uiElements.title.attr("title", newTitle);
	}

	/**
	 * Shows the hint in this tab's prompt
	 * @method
	 * @memberof ConnectionTab
	 */
	ConnectionTab.prototype.showHint = function() {
		this.state.codeMirror.showHint({completeSingle: false, hint: MongoNS.mongoDBHintAdapter, connectedTab: this});
	}

	/**
	 * (Re-)Prints the results as stored in state.displayedResults. Can be used to update the view after the state was altered.
	 */
	ConnectionTab.prototype.updateView = function(){
		this.uiElements.results.children().remove();

		for(var i=0; i < this.state.displayedResult.length; i++){
			var val = this.state.displayedResult[i];

			var displayedKey = "(" + (i + 1) + ")";
			if(val._id instanceof MongoNS.ObjectId)
				displayedKey += " " + val._id.toString();
			var lines = printLine(this, "", displayedKey, val, 0);
			lines.attr("data-index", i);
		}
	}

	/**
	 * Creates new tabs. Each MongoBrowser should have its own factory to prevent collisions in the tab-ids
	 * @class TabFactory
	 *
	 * @param {JQuery} dummyLink - a jQuery wrapped <tt>HTMLElement</tt> (LI) to append as tab handle. Must
	 *                             contain a .tabText to put the tab title in
	 * @param {JQuery} dummyTab - a jQuery wrapped <tt>HTMLElement</tt> to append as tab content
	 * @param {MongoBrowser~options} options - the default options which were passed to the instantiating mongobrowser
	 */
	function TabFactory(dummyLink, dummyTab, options){
		this.prefix = "tabs"+TabFactory.instances++;
		this.dummyLink = dummyLink;
		this.dummyTab = dummyTab;
		this.options = options;
	}

	/** The total number of factories created to savely create unique IDs
	 * @static
	 * @memberof TabFactory
	 * @type {number}
	 */
	TabFactory.instances = 0;

	/**
	 * Creates a new tab with the given name
	 *
	 * @method
	 * @memberof TabFactory
	 * @param {MongoNS.DB} database - the database to which db should equate in this tab
	 * @param {string} collection - the default collection to use
	 * @returns {ConnectionTab} the constructed ConnectionTab
	 */
	TabFactory.prototype.newTab = function(database, collection){
		return new ConnectionTab(this.prefix, this.dummyLink, this.dummyTab, database, collection, this.options);
	}


	if(typeof MongoBrowserNS === "undefined")
		MongoBrowserNS = {};

	MongoBrowserNS.TabFactory = TabFactory;

	return MongoBrowserNS;
})(window.MongoBrowserNS);