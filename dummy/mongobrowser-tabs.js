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
	 */
	function ConnectionTab(prefix, dummyLink, dummyTab, database, collection){
		this.uiElements = {};
		this.state = {};

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
									    	start: tab.find(".curIterate")
									   }
									}

		ui.info.connection.text(connection);
		ui.info.database.text(database.toString());
		ui.info.collection.text(collection);
		ui.prompt.val(defaultPrompt);
		this.setTitle(defaultPrompt);

		//calling this here fixes problems with minWidth
		ui.resultsTable.resizableColumns({minWidth: 15});

		this.state.id = id;
		this.state.db = database
		this.state.collection = collection;
		this.state.displayedResult = [];

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
		parent.tabs("refresh");

		//select the first tab, if no tab was selected before
		if(parent.children(".tabList").children().size() === 1)
			parent.tabs("option", "active", 0);
	}

	/**
	 * Execute the code from the prompt within the MongoNS namespace on this tab's db
	 * @method
	 * @memberof ConnectionTab
	 */
	ConnectionTab.prototype.execute = function(){
		var self = this;

		function base_print(indent, image, alt, col1, col2, col3, hasChildren, key) {
			var newLine = $("<tr data-indent='" + indent + "' data-key='" + key + "' class='collapsed " + (hasChildren ? "hasChildren" : "") + "' \
				style='"+ (indent > 0 ? "display:none" : "") + "'> \
				<td><span class='foldIcon'>&nbsp;</span> <img src='images/" + image + "' class='typeIcon' alt='" + alt + "' /> " + col1 + "</td> \
				<td>" + col2 + "</td> \
				<td>" + col3 + "</td></tr>");
			newLine.appendTo(self.uiElements.results);
			return newLine;
		}

		function printObject(key, displayedKey, val, indent) {
			var keys = Object.keys(val);

			var ret = base_print(indent, "bson_object_16x16.png", "object", displayedKey, "{ " + keys.length + " fields }", "Object", keys.length !== 0, key);

			for(var i=0; i<keys.length; i++){
				var newLine = printLine(keys[i], keys[i], val[keys[i]], indent + 1);
				ret = ret.add(newLine);
			}
			return ret;
		}

		function printArray(key, displayedKey, val, indent) {
			var keys = Object.keys(val);

			var ret = base_print(indent, "bson_array_16x16.png", "array", displayedKey, "[ " + val.length + " Elements ]", "Array", keys.length !== 0, key);

			for(var i=0; i<keys.length; i++){
				var newLine = printLine(keys[i], "[" + keys[i] + "]", val[keys[i]], indent + 1);
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

		function printLine(key, displayedKey, val, indent) {
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

		var startTime = $.now();

		//TODO: this fails if we yield the js thread (e.g. in an asynchronous ajax call?) before finishing MongoNS.execute and resetting the print fct
		var printedLines = []
		var oldPrint = MongoNS.__namespacedPrint;
		MongoNS.__namespacedPrint = function(line){
			printedLines.push(line);
		}
		try {
			var ret = MongoNS.execute(MongoNS, this.state.db, this.uiElements.prompt.val());

			if(ret instanceof MongoNS.DBQuery){
				this.state.collection = ret._collection._shortName;
				this.state.database = ret._database;

				this.uiElements.info.database.text(ret._db._name);
				this.uiElements.info.collection.text(ret._collection._shortName);
				this.uiElements.info.collection.parent().show();
				ret = ret._exec()
			}else{
				this.uiElements.info.collection.parent().hide();
			}

			if(ret instanceof MongoNS.WriteResult){
				MongoNS.__namespacedPrint(ret.toString());
				ret.__magicNoPrint = 1;
			}
		}catch(e){
			var ret = undefined;
			MongoNS.__namespacedPrint(e.toString());
		}

		var duration = $.now() - startTime;
		this.uiElements.info.time.text(duration/1000);

		this.uiElements.printContainer.hide()
		this.uiElements.resultsTable.hide();
		this.uiElements.printedLines.text("");
		this.uiElements.results.children().remove();

		if(typeof ret !== "undefined" && ret !== null && typeof ret.__magicNoPrint === "undefined"){
			this.uiElements.resultsTable.show();

			if(ret instanceof MongoNS.Cursor){
				this.state.displayedResult = [];
				for(var i=0; i < parseInt(this.uiElements.iterate.max.val()) && ret.more(); i++){
					var val = ret.next();
					this.state.displayedResult.push(val);
					var displayedKey = "(" + (i + 1) + ")";
					if(val._id instanceof MongoNS.ObjectId)
						displayedKey += " " + val._id.toString();
					var lines = printLine("", displayedKey, val, 0);
					lines.attr("data-index", i);
				}
			}else{
				this.state.displayedResult = [ret];
				printLine("", "(" + 1 + ")", ret, 0).attr("data-index", 0);
			}
			this.uiElements.results.children().eq(0).trigger("dblclick"); //expand the first element
			this.uiElements.results.children("[data-indent]").each(function(index, elem){
				$(elem).children().eq(0).css("padding-left", parseInt($(elem).attr("data-indent"))*25+"px");
			});

		}else if(printedLines.length === 0){
			printedLines.push("Script executed successfully but there is no output to display.")
		}

		if(printedLines.length !== 0){
			this.uiElements.printContainer.show();

			var text = "";
			for(var i=0; i < printedLines.length; i++){
				text += printedLines[i] + "\n";
			}
			self.uiElements.printedLines.text(text);
		}

		self.uiElements.resultsTable.find("th").css("width", "");
		self.uiElements.resultsTable.resizableColumns("destroy");
		self.uiElements.resultsTable.prev(".resizableColumnsFix").remove();
		self.uiElements.resultsTable.resizableColumns({minWidth: 15});
		self.uiElements.resultsTable.prev().wrap($("<div class='resizableColumnsFix' style='width:0px;'></div>"))

		self.setTitle(this.uiElements.prompt.val());
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
	 * Creates new tabs. Each MongoBrowser should have its own factory to prevent collisions in the tab-ids
	 * @class TabFactory
	 *
	 * @param {JQuery} dummyLink - a jQuery wrapped <tt>HTMLElement</tt> (LI) to append as tab handle. Must
	 *                             contain a .tabText to put the tab title in
	 * @param {JQuery} dummyTab - a jQuery wrapped <tt>HTMLElement</tt> to append as tab content
	 */
	function TabFactory(dummyLink, dummyTab){
		this.prefix = "tabs"+TabFactory.instances++;
		this.dummyLink = dummyLink;
		this.dummyTab = dummyTab;
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
		return new ConnectionTab(this.prefix, this.dummyLink, this.dummyTab, database, collection);
	}


	if(typeof MongoBrowserNS === "undefined")
		MongoBrowserNS = {};

	MongoBrowserNS.TabFactory = TabFactory;

	return MongoBrowserNS;
})(window.MongoBrowserNS);