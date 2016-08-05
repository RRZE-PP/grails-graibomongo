function TODO(){
	throw new Error("Not yet implemented. Sorry");
}

/**
 * @namespace MongoBrowser(NS)
 * @description Please note, that all methods of the {@link MongoBrowser } class can be called within this
 * namespace as well just like normal functions. E.g. <span class="signature">myMethod(self, foo, bar)</span>. You have to supply
 * the <i>self</i> argument when calling from within the namespace to point to the Object a method should
 * be called upon. Usually you can pass the <i>self</i> that was passed to your function
 * (Unfortunately JSDoc can't document this well). <br />
 * Furthermore all methods, which are listed as <span class="signature">(static)</span> but have a
 * <i>self</i> parameter are not <span class="signature">(static)</span>, but <span class="signature">(inner)</span>
 * that's another thing I could not work around with JSDoc.
 *
 * @author Tilman 't.animal' Adler <Tilman.Adler [at] fau [dot] de>
 * @license [Apache License v. 2.0]{@link http://www.apache.org/licenses/LICENSE-2.0.txt }
 *
 * @borrows MongoBrowser#connect as MongoBrowser(NS)~connect
 * @borrows MongoBrowser#addConnectionPreset as MongoBrowser(NS)~addConnetionPreset
 */
window.MongoBrowser = (function(){

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
									   results: tab.find(".resultsTable tbody"),
									   iterate: {
									    	max: tab.find(".maxIterate"),
									    	start: tab.find(".curIterate")
									   }
									}

		ui.title.text(defaultPrompt.substr(0, 20)+"...");
		ui.info.connection.text(connection);
		ui.info.database.text(database.toString());
		ui.info.collection.text(collection);
		ui.prompt.val(defaultPrompt);

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

		function base_print(indent, image, alt, col1, col2, col3, hasChildren) {
			return $("<tr data-indent='" + indent + "'  class='collapsed " + (hasChildren ? "hasChildren" : "") + "' \
				style='"+ (indent > 0 ? "display:none" : "") + "'> \
				<td><span class='foldIcon'>&nbsp;</span> <img src='images/" + image + "' class='typeIcon' alt='" + alt + "' /> " + col1 + "</td> \
				<td>" + col2 + "</td> \
				<td>" + col3 + "</td></tr>").appendTo(self.uiElements.results);
		}

		function printObject(key, val, indent) {
			var keys = Object.keys(val);

			var ret = base_print(indent, "bson_object_16x16.png", "object", key, "{ " + keys.length + " fields }", "Object", keys.length !== 0);

			for(var i=0; i<keys.length; i++){
				printLine(keys[i], val[keys[i]], indent + 1);
			}
			return ret;
		}

		function printArray(key, val, indent) {
			var keys = Object.keys(val);

			var ret = base_print(indent, "bson_array_16x16.png", "array", key, "[ " + val.length + " Elements ]", "Array", keys.length !== 0);

			for(var i=0; i<keys.length; i++){
				printLine("[" + keys[i] + "]", val[keys[i]], indent + 1);
			}

			return ret;
		}

		function printObjectId(key, val, indent) {
			return base_print(indent, "bson_unsupported_16x16.png", "oid", key, val.toString(), "ObjectId");
		}

		function printRegExp(key, val, indent) {
			return base_print(indent, "bson_unsupported_16x16.png", "regex", key, val.toString(), "Regular Expression");
		}

		function printDate(key, val, indent) {
			return base_print(indent, "bson_datetime_16x16.png", "date", key, val.toString(), "Date");
		}

		function printString(key, val, indent) {
			return base_print(indent, "bson_string_16x16.png", "string", key, val, "String");
		}

		function printDouble(key, val, indent) {
			return base_print(indent, "bson_double_16x16.png", "double", key, val, "Double");
		}

		function printInt(key, val, indent) {
			return base_print(indent, "bson_integer_16x16.png", "int", key, val, "Int32");
		}

		function printLong(key, val, indent) {
			return base_print(indent, "bson_integer_16x16.png", "long", key, val.toString(), "Int64");
		}

		function printBoolean(key, val, indent) {
			return base_print(indent, "bson_bool_16x16.png", "boolean", key, val, "Boolean");
		}

		function printNull(key, val, indent) {
			return base_print(indent, "bson_null_16x16.png", "null", key, "null", "Null");
		}

		function printUndefined(key, val, indent) {
			return base_print(indent, "bson_unsupported_16x16.png", "undefined", key, "undefined", "Undefined");
		}

		function printUnsupported(key, val, indent) {
			return base_print(indent, "bson_unsupported_16x16.png", "unsupported", key, "", "unsupported");
		}

		function printLine(key, val, indent) {
			if(val instanceof Array)
				return printArray(key, val, indent);
			else if(val instanceof MongoNS.ObjectId)
				return printObjectId(key, val, indent);
			else if(val instanceof MongoNS.NumberLong)
				return printLong(key, val, indent);
			else if(val instanceof RegExp)
				return printRegExp(key, val, indent);
			else if(val instanceof Date)
				return printDate(key, val, indent);
			else if(typeof val === "string" || val instanceof String)
				return printString(key, val, indent);
			else if((typeof val === "number" || val instanceof Number) && parseInt(val) === val)
				return printInt(key, val, indent);
			else if(typeof val === "number" || val instanceof Number)
				return printDouble(key, val, indent);
			else if(typeof val === "boolean")
				return printBoolean(key, val, indent);
			else if(val === null) //TODO: Int vs Double!
				return printNull(key, val, indent);
			else if(typeof val === "undefined")
				return printUndefined(key, val, indent);
			else if(typeof val === "object") //this comes last after all others have been ruled out
				return printObject(key, val, indent);
			else
				return printUnsupported(key, val, indent); //should not happen
		}

		var startTime = $.now();

		var ret = MongoNS.execute(MongoNS, this.state.db, this.uiElements.prompt.val());
		if(ret instanceof MongoNS.DBQuery)
			ret = ret._exec()

		var duration = $.now() - startTime;
		this.uiElements.info.time.text(duration/1000);

		this.uiElements.results.children().remove();

		if(ret instanceof MongoNS.Cursor){
			this.state.displayedResult = [];
			for(var i=0; i < parseInt(this.uiElements.iterate.max.val()) && ret.more(); i++){
				var val = ret.next();
				this.state.displayedResult.push(val);
				printLine("(" + (i + 1) + ")", val, 0).attr("data-index", i);
			}
		}else{
			this.state.displayedResult = [ret];
			printLine("(" + 1 + ")", ret, 0).attr("data-index", 0);
		}
		this.uiElements.results.children().eq(0).trigger("dblclick"); //expand the first element
		this.uiElements.results.children("[data-indent]").each(function(index, elem){
			$(elem).children().eq(0).css("padding-left", parseInt($(elem).attr("data-indent"))*25+"px");
		});

		return ret;
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


	/**
	 * Creates a MongoBrowser instance. Can be called using new or without it.
	 * @class MongoBrowser
	 *
	 * @classdesc Please note: in this documentation you will find a parameter 'self' in
	 * every method. Do not set it. It's there only for internal use, but cannot be ignored
	 * in this class documentation. It will be set automatically for you, when you call a method
	 * on an object. I.e: <span class="signature">myMongoBrowser.myMethod(arg1, arg2)</span>. But when calling the same method
	 * from inside the {@link MongoBrowser(NS) MongoBrowser } namespace, you have to set it.
	 * I.e.: <span class="signature">myMethod(self, arg1, arg2)</span>. (Unfortunately JSDoc can't document this well)
	 *
	 * @param {HTMLElement} appendTo - the container to wich to append the MongoBrowser's gui
	 * @param {MongoBrowser~options} options       - an object to get the options from
	 */
	function MongoBrowser(appendTo, options){
		//allow the user to omit new
		if (!(this instanceof MongoBrowser))
			return new MongoBrowser(appendTo, options);

		var self = this;
		self.options = typeof options !== "undefined" ? options : {};

		self.state = {
			connectionPresets: typeof options.connectionPresets !== "undefined" ? options.connectionPresets : [],
			connections: [],
			tabs: {}
		}

		self.instanceNo = MongoBrowser.instances++;

		initUIElements(self);

		self.rootElement.appendTo(appendTo);
		self.rootElement.addClass("mongoBrowser");
		self.rootElement.css("display", "block");
	}

	/** The total number of MongoBrowsers created to savely create unique IDs
	 * @static
	 * @memberof MongoBrowser
	 * @type {number}
	 */
	MongoBrowser.instances = 0;

	/**
	 * Adds a new tab to the MongoBrowser's gui.
	 * @param {MongoBrowser} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @param {string} database - the database to operate on in this tab
	 * @param {string} collection - the default collection in this tab
	 * @private
	 * @memberof MongoBrowser(NS)~
	 */
	function addTab(self, database, collection){
		var tab = self.state.tabFactory.newTab(database, collection);
		tab.appendTo(self.uiElements.tabs.container);
		tab.execute();
		tab.select();
		self.state.tabs[tab.id()] = tab;
	}

	/**
	 * Sets callbacks on the buttons in the actionBar and saves them
	 * @param {MongoBrowser} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @private
	 * @memberof MongoBrowser(NS)~
	 */
	function createActionBarButtons(self){
		var button = self.uiElements.buttons["connectionButton"] = self.rootElement.find(".actionBar .connectionButton");
		button.on("click", function(){openDialog(self, "connectionManager")});

		button = self.uiElements.buttons["executeButton"] = self.rootElement.find(".actionBar .executeButton");
		button.on("click", function(){var tab = getCurrentTab(self); if(tab !== null) tab.execute()});
	}


	/**
	 * Creates and stores the dialog elements in self.uiElements.dialogs.
	 * @param {MongoBrowser} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @private
	 * @memberof MongoBrowser(NS)~
	 */
	function createDialogs(self){
		/** @namespace dialogInitialisators
		*/

		/**
		 * Initialises the connection settings dialog. This function usually cannot be called directly (except within
		 * the function body of {@link MongoBrowser(NS)~createDialogs createDialogs }), but is called everytime
		 * {@link MongoBrowser(NS)~openDialog openDialog } is called with "connectionManager" as first argument
		 * and a list of presets or [] as second
		 * @param {MongoBrowser~connectionPreset[]} [presets] - the connections to present to the user. if unset or [], the presets of the
		 *                                         mongobrowser to which this dialog belongs, will be used
		 * @memberof dialogInitialisators~
		 * @inner
		 */
		function initConnectionManagerDialog(presets){
			if(typeof presets === "undefined" || !$.isArray(presets) || presets.length === 0)
				presets = self.state.connectionPresets;

			var table = this.find(".connectionsTable tbody");
			table.children().remove();
			for (var i=0; i<presets.length; i++) {
				var p = presets[i];

				var newLine = $("<tr data-connectionIndex='"+i+"'><td>"+p.name+"</td><td>"+p.host+":"+p.port+"</td><td> </td></tr>");
				newLine.on("dblclick", (function(p){
					return function(){
						connect(self, p.host, p.port, "test");
						self.uiElements.dialogs.connectionManager.dialog("close")}
					})(p));
				newLine.on("click", function(){table.children().removeClass("current"); $(this).addClass("current")});

				table.append(newLine);
			}
			table.children().eq(0).addClass("current");
			if(presets.length < 3)
				for (var i=0; i< 3 - presets.length; i++)
					table.append($("<tr class='whitespace'><td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td></tr>"));
		}

		function editCurrentConnectionPreset(){
				var curLine = self.uiElements.dialogs.connectionManager.find(".current");
				if(curLine.size() === 0)
					return;
				var idx = parseInt(curLine.attr("data-connectionIndex"));
				openDialog(self, "connectionSettings", idx)
		}

		function cloneCurrentConnectionPreset(){
				var curLine = self.uiElements.dialogs.connectionManager.find(".current");
				if(curLine.size() === 0)
					return;
				var idx = parseInt(curLine.attr("data-connectionIndex"));
				var cloned = $.extend({}, self.state.connectionPresets[idx]);
				cloned.name = "Copy of " + cloned.name;
				openDialog(self, "connectionSettings", cloned);
		}

		function removeCurrentConnectionPreset(){
				var curLine = self.uiElements.dialogs.connectionManager.find(".current");
				if(curLine.size() === 0)
					return;
				var idx = parseInt(curLine.attr("data-connectionIndex"));
				self.state.connectionPresets.splice(idx, 1);
				self.uiElements.dialogs.connectionManager.initialise();
		}

		function connectCurrentConnectionPreset(){
				var curLine = self.uiElements.dialogs.connectionManager.find(".current");
				if(curLine.size() === 0)
					return;
				var idx = parseInt(curLine.attr("data-connectionIndex"));
				var preset= self.state.connectionPresets[idx];
				connect(self, preset.host, preset.port, "test");
		}

		/**
		 * Initialises the connection settings dialog. This function usually cannot be called directly (except within
		 * the function body of {@link MongoBrowser(NS)~createDialogs createDialogs }), but is called everytime
		 * {@link MongoBrowser(NS)~openDialog openDialog } is called with "connectionSettings" as first argument
		 * and a preset or {} as second
		 * @param {number|MongoBrowser~connectionPreset} [preset] - if preset is unset or an empty object ({}) the dialog will be empty
		 *                                             and saving will trigger preset creation (create mode) <br />
		 *                                             if preset is a number the dialog will be prefilled with
		 *                                             the corresponding preset's data in the list of presets
		 *                                             of the mongobrowser instance. saving will trigger an update
		 *                                             on that preset (edit mode) <br />
		 *                                             if preset is a {@link MongoBrowser~connectionPreset} the dialog
		 *                                             will be prefilled with that preset's data and saving will trigger
		 *                                             a preset creation (clone mode)
		 * @memberof dialogInitialisators~
		 * @inner
		 */
		function initConnectionSettingsDialog(preset){
			if(typeof preset === "number"){
				this.mode = "edit";
				preset = self.state.connectionPresets[preset];
			}else if(typeof preset === "undefined" || Object.keys(preset).length === 0){
				this.mode = "create";
				preset = {name:"", host:"", port: 27017};
			}else {
				this.mode = "clone";
			}

			this.find("form")[0].reset();

			this.find(".connectionName").val(preset.name);
			this.find(".connectionHost").val(preset.host);
			this.find(".connectionPort").val(preset.port);

			if(preset.performAuth && typeof preset.auth !== undefined){
				this.find("[name=adminDatabase]").val(preset.auth.adminDatabase);
				this.find("[name=username]").val(preset.auth.username);
				this.find("[name=password]").val(preset.auth.password);
				this.find("[name=method]").val(preset.auth.method);
				this.find("[name=performAuth]").prop("checked", preset.performAuth).change();
			}

			self.uiElements.dialogs.connectionSettings.tabs.tabs( "option", "active", 0 );
		}

		function saveNewConnectionPreset(){
			var name = self.uiElements.dialogs.connectionSettings.find(".connectionName").val();
			var host = self.uiElements.dialogs.connectionSettings.find(".connectionHost").val();
			var port = parseInt(self.uiElements.dialogs.connectionSettings.find(".connectionPort").val());

			self.state.connectionPresets.push({name:name, host:host, port:port});
			self.uiElements.dialogs.connectionManager.initialise();
		}

		function updateCurrentConnectionPreset(){
			var curLine = self.uiElements.dialogs.connectionManager.find(".current");
			if(curLine.size() === 0)
				return;
			var idx = parseInt(curLine.attr("data-connectionIndex"));

			var curDialog = self.uiElements.dialogs.connectionSettings

			var name = curDialog.find(".connectionName").val();
			var host = curDialog.find(".connectionHost").val();
			var port = parseInt(curDialog.find(".connectionPort").val());

			var adminDatabase = curDialog.find("[name=adminDatabase]").val();
			var username = curDialog.find("[name=username]").val();
			var password = curDialog.find("[name=password]").val();
			var method = curDialog.find("[name=method]").val();
			var performAuth = curDialog.find("[name=performAuth]").prop("checked");

			self.state.connectionPresets[idx] = {name:name, host:host, port:port, performAuth: performAuth,
					auth: {adminDatabase: adminDatabase, username: username, password: password, method: method}};
			self.uiElements.dialogs.connectionManager.initialise();
		}

		function getSaveAction(){
			var curDialog = self.uiElements.dialogs.connectionSettings;
			if(curDialog.mode === "create" || curDialog.mode === "clone"){
				curDialog.mode = undefined;
				return saveNewConnectionPreset;
			}else if(curDialog.mode === "edit"){
				curDialog.mode = undefined;
				return updateCurrentConnectionPreset;
			}else{
				return function(){console.error("Invalid state: mode was not set correctly")};
			}
		}

		/**
		 * Initialises the edit document dialog. This function usually cannot be called directly (except within
		 * the function body of {@link MongoBrowser(NS)~createDialogs createDialogs }), but is called everytime
		 * {@link MongoBrowser(NS)~openDialog openDialog } is called with "editDocument" as first argument
		 * and a <tt>MongoNS.DB</tt> and document as third and fourth parameter
		 * @param {MongoNS.DB} db - the database to operate on when saving the document
		 * @param {Object} doc - the document to serialize and display
		 * @param {String} collection - the collection from which the doc is taken
		 * @memberof dialogInitialisators~
		 * @inner
		 */
		function initEditDocumentDialog(db, doc, collection){
			var curDialog = self.uiElements.dialogs.editDocument;
			var connection = db.getMongo().host.substr(0, db.getMongo().host.indexOf("/"));

			var buttons = curDialog.dialog("option", "buttons");
			buttons[1].click = function(){
				var newVal = self.uiElements.dialogs.editDocument.find(".documentEditor").val();
				try{
					var newObj = MongoNS.execute(MongoNS, db, "(function(){ return "+ newVal.replace(/\n/g, "") +";})()");
				}catch(e){
					alert("Invalid JSON");
					return;
				}
				if(typeof newObj === "undefined"){
					alert("Invalid JSON");
					return;
				}
				db.getCollection(collection).update({"_id": doc._id}, newObj);
				$(this).dialog("close");
			};
			curDialog.dialog("option", "buttons", buttons);

			curDialog.find(".documentEditor").val(MongoNS.tojson(doc));
			curDialog.find(".info .connection span").text(connection);
			curDialog.find(".info .database span").text(db.toString());
			curDialog.find(".info .collection span").text(collection);
		}

		/**
		 * Initialises the view document dialog. This function usually cannot be called directly (except within
		 * the function body of {@link MongoBrowser(NS)~createDialogs createDialogs }), but is called everytime
		 * {@link MongoBrowser(NS)~openDialog openDialog } is called with "viewDocument" as first argument
		 * and a <tt>MongoNS.DB</tt> and document as third and fourth parameter
		 * @param {MongoNS.DB} db - the database to display at the top border
		 * @param {Object} doc - the document to serialize and display
		 * @param {String} collection - the collection from which the doc is taken
		 * @memberof dialogInitialisators~
		 * @inner
		 */
		function initViewDocumentDialog(db, doc, collection){
			var curDialog = self.uiElements.dialogs.viewDocument;
			var connection = db.getMongo().host.substr(0, db.getMongo().host.indexOf("/"));

			curDialog.find(".documentEditor").val(MongoNS.tojson(doc));
			curDialog.find(".info .connection span").text(connection);
			curDialog.find(".info .database span").text(db.toString());
			curDialog.find(".info .collection span").text(collection);

		}

		//begin connection manager
		var curDialog = self.uiElements.dialogs.connectionManager =
			self.rootElement.find(".connectionManager").dialog({
				autoOpen: false,
				buttons: [
					{text: "Cancel",
					click: function() {
						$( this ).dialog( "close" );
						}
					},
					{text: "Connect",
					icons: {primary: "connectIcon"},
					click: function(){connectCurrentConnectionPreset(); $(this).dialog("close");}}
				],
				dialogClass: "mongoBrowser",
				modal: true,
				width: 'auto'
			});

		curDialog.initialise = initConnectionManagerDialog;
		curDialog.initialise();

		curDialog.find(".connectionCreateLink").on("click", function(){openDialog(self, "connectionSettings", {})});
		curDialog.find(".connectionEditLink").on("click", editCurrentConnectionPreset);
		curDialog.find(".connectionCloneLink").on("click", cloneCurrentConnectionPreset);
		curDialog.find(".connectionRemoveLink").on("click", removeCurrentConnectionPreset);

		//begin connection settings dialog
		var curDialog = self.uiElements.dialogs.connectionSettings =
			self.rootElement.find(".connectionSettings").dialog({
				autoOpen: false,
				buttons: [
					{text: "Test",
					icons: {primary: "testIcon"},
					click: TODO},
					{text: "Save",
					click: function(){getSaveAction()(); $(this).dialog("close");}},
					{text: "Cancel",
					click: function() {
						$( this ).dialog( "close" );
						}
					}
				],
				dialogClass: "mongoBrowser",
				modal: true,
				width: 380
			});

		//create tabs in connection settings dialog, make IDs unique by adding this mongobrowser's instanceNo
		curDialog.find(".tabList a").each(function(idx, obj){
			var oldId = $(obj).attr("href");
			var newId = oldId + "-" + self.instanceNo;

			curDialog.find(oldId).attr("id", newId.slice(1));
			$(obj).attr("href", newId);
		});
		curDialog.tabs = curDialog.find(".tabContainer").tabs();

		curDialog.find("[name='performAuth']").on("change", function(){
			var curTab = self.uiElements.dialogs.connectionSettings.find(".connectionSettingsAuthenticationTab");
			if($(this).prop("checked")){
				curTab.find(".connectionSettingsEnableDisableSwitch").removeClass("disabled");
				curTab.find("input, select").removeAttr("disabled");
			}else{
				curTab.find(".connectionSettingsEnableDisableSwitch").addClass("disabled");
				curTab.find("input, select").slice(1).attr("disabled", "disabled"); //slice: don't affect checkbox
			}
		});
		curDialog.find("form").on("reset", function(){
			//force change event
			var authElem = self.uiElements.dialogs.connectionSettings.find("[name='performAuth']");
			authElem.prop("checked", false);
			authElem.change();
		});

		curDialog.initialise = initConnectionSettingsDialog;
		curDialog.initialise();


		//begin document editor
		curDialog = self.uiElements.dialogs.editDocument = self.rootElement.find(".editDocument").dialog({
			autoOpen: false,
			dialogClass: "mongoBrowser",
			buttons:[
					{text: "Validate",
					icons: {primary: "validateIcon"},
					click: TODO},
					{text: "Save",
					click: TODO},
					{text: "Cancel",
					click: function() {
						$( this ).dialog( "close" );
						}
					}
				],
			modal: true,
			width: "auto",
			height: "auto",
		});
		curDialog.initialise = initEditDocumentDialog;

		//begin document viewer
		curDialog = self.uiElements.dialogs.viewDocument = self.rootElement.find(".viewDocument").dialog({
			autoOpen: false,
			dialogClass: "mongoBrowser",
			buttons:[
					{text: "Cancel",
					click: function() {
						$( this ).dialog( "close" );
						}
					}
				],
			modal: true,
			width: "auto",
			height: "auto",
		});
		curDialog.initialise = initViewDocumentDialog;

	}

	/**
	 * Create the root Element. Currently copies from a static, hidden DOM-Node. In the future it
	 * should create it from scratch, so as to remove the dependency on a html content
	 * @param {MongoBrowser} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @private
	 * @memberof MongoBrowser(NS)~
	 * @todo create element from scratch
	 */
	function createRootElement(self) {
		var dummy = $("#MongoBrowserDummy");

		if(dummy.size() == 0){
			// TODO
			throw new Error("Creating UI from scratch is not yet implemented");
		}

		self.rootElement = self.uiElements.root = dummy.clone();
	}

	/**
	 * Create the sidebar and delegate click events on its elements for collapsing/opening elements
	 * @param {MongoBrowser} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @private
	 * @memberof MongoBrowser(NS)~
	 */
	function createSidebarEnvironment(self){
		var sideBar = self.uiElements.sideBar = self.rootElement.find(".sideBar ul").eq(0);

		function toggleCollapsed(elem){
			var elem = $(elem);
			if(elem.hasClass("collapsed")){
				elem.addClass("opened").removeClass("collapsed");
			}else{
				elem.removeClass("opened").addClass("collapsed");
			}
		}

		sideBar.delegate(".foldIcon", "click", function(evt){toggleCollapsed(evt.target.parentNode)});
	}

	/**
	 * Create the tab environment and stores several dummy elements for later copying
	 * @param {MongoBrowser} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @private
	 * @memberof MongoBrowser(NS)~
	 */
	function createTabEnvironment(self) {
		var container = self.uiElements.tabs.container = self.rootElement.find(".mainTabs.tabContainer");

		var dummyTab = container.children("div").eq(0);
		var dummyLink = container.find("li").eq(0);

		self.state.tabFactory = new TabFactory(dummyLink.clone().css("display", ""), dummyTab.clone().css("display", ""));

		dummyLink.remove();
		dummyTab.remove();

		container.tabs();

		//make tabs closeable
		self.uiElements.tabs.container.delegate(".closeButton", "click", function(){
			var panelId = $(this).closest("li").remove().attr("aria-controls");
			$("#" + panelId ).remove();
			delete self.state.tabs[panelId];
			self.uiElements.tabs.container.tabs("refresh");
		});

		//highlight a clicked result item with the current selector
		self.uiElements.tabs.container.delegate(".resultsTable tbody tr", "click contextmenu", function(){
			self.uiElements.tabs.container.find(".resultsTable .current").removeClass("current");
			$(this).addClass("current");
		});

		//make result items collapsible
		function collapseOrExpandResult(result, collapse, recursively){
			var depth = parseInt(result.attr("data-indent"));
			var next = result.next();
			var skipAllDeeperThan = null;
			while(next.size() !== 0 && parseInt(next.attr("data-indent")) > depth){
				if(skipAllDeeperThan !== null){
					if(parseInt(next.attr("data-indent")) > skipAllDeeperThan){
						next = next.next();
						continue;
					}
					skipAllDeeperThan = null; //only, when not continued
				}

				if(!collapse && !recursively && next.hasClass("collapsed")){ //when opening skip nested collapsed TRs
					skipAllDeeperThan = parseInt(next.attr("data-indent"));
				}

				if(recursively){
					next.removeClass("collapsed opened").addClass(collapse?"collapsed":"opened");
				}
				next.css("display", collapse?"none":"table-row");

				next = next.next();
			}

			result.removeClass("collapsed opened").addClass(collapse?"collapsed":"opened");
		}

		self.uiElements.tabs.container.delegate(".resultsTable tbody tr .foldIcon", "click", function(){
			var tr = $(this).parentsUntil("tr").parent();
			collapseOrExpandResult(tr, tr.hasClass("opened"), false);
		});
		self.uiElements.tabs.container.delegate(".resultsTable tbody tr", "dblclick", function(){
			var tr = $(this);
			collapseOrExpandResult(tr, tr.hasClass("opened"), false);

		});

		//register a context menu on result items
		self.uiElements.tabs.container.contextMenu({
			className: "mongoBrowser",
			selector: ".resultsTable tbody tr",
			items: {
				expand: {name: "Expand recursively",
				            callback: function(){collapseOrExpandResult($(this), false, true);},
				            disabled: function(){$(this).hasClass("hasChildren")}},
				collapse: {name: "Collapse recursively", callback: function(){collapseOrExpandResult($(this), true, true);},
				            disabled: function(){$(this).hasClass("hasChildren")}},
				"sep1": "---------",
				edit: {name: "Edit Document...", callback: function(){
						var idx = parseInt($(this).attr("data-index"));
						var curTab = getCurrentTab(self);
						openDialog(self, "editDocument", curTab.getInfo().database, curTab.getDataRow(idx), curTab.getInfo().collection)}},
				view: {name: "View Document...", callback: function(){
						var idx = parseInt($(this).attr("data-index"));
						var curTab = getCurrentTab(self);
						openDialog(self, "viewDocument", curTab.getInfo().database, curTab.getDataRow(idx), curTab.getInfo().collection)}},
				insert: {name: "Insert Document..."},
				"sep2": "---------",
				copy: {name: "Copy JSON", callback: function(){
						var idx = parseInt($(this).attr("data-index"));
						var elem = $("<p>").attr("data-clipboard-text", JSON.stringify(getCurrentTab(self).getDataRow(idx)));
						var c = new Clipboard(elem[0]);
						elem.click(); c.destroy(); elem.remove(); //well that was quick :/
						}},
				delete: {name: "Delete Document..."}
			}
		});
	}

	/**
	 * Returns the currently shown {@link ConnectionTab }, if there is one
	 * @param {MongoBrowser} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @private
	 * @memberof MongoBrowser(NS)~
	 * @returns {ConnectionTab} the currently shown connection tab or <tt>null</tt>
	 */
	function getCurrentTab(self){
		var visibleTab = self.uiElements.tabs.container.find("[aria-hidden=false]")
		if(visibleTab.size() == 0)
			return null;
		return self.state.tabs[visibleTab.attr("id")];
	}

	/**
	 * Creates and initiates all UI Elements
	 * @param {MongoBrowser} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @private
	 * @memberof MongoBrowser(NS)~
	 */
	function initUIElements(self){
		self.uiElements = {root:null, dialogs: {}, tabs:{}, buttons:{}, sideBar:null};
		createRootElement(self);
		createDialogs(self);
		createTabEnvironment(self);
		createActionBarButtons(self);
		createSidebarEnvironment(self);
	}

	/**
	 * Opens one of the dialogs from self.uiElements.dialogs. Currently there are:
	 * connectionManager, connectionSettings
	 * @param {MongoBrowser} self - as this is a private member <i>this</i> is passed as <i>self</i> explicitly
	 * @param {string} dialogName - the name of the dialog to open
	 * @param {object...} [initArgs] -
	 *                               when set, the initialisation function of the
	 *                               dialog will be called (if it exists) and the initArgs passed as parameters <br />
	 *                               for further information, see the parameters to the functions in
	 *                               {@link dialogInitialisators }
	 * @throws {ReferenceError} When no dialog with the name 'dialogName' exists
	 * @private
	 * @memberof MongoBrowser(NS)~
	 */
	function openDialog(self, dialogName, initArgs){
		var dialog = self.uiElements.dialogs[dialogName];
		if(typeof dialog === "undefined")
			throw new ReferenceError("No such dialog: "+dialogName);

		if(typeof initArgs !== "undefined" && typeof dialog.initialise !== "undefined")
			dialog.initialise.apply(dialog, Array.prototype.splice.call(arguments, 2));

		dialog.dialog("open");
	}
	/******************************************************************************************************************
	 *                                         BEGIN PUBLIC MEMBERS                                                   *
	 *                     (the following functions will be exported via the MongoBrowser.prototype)                  *
	 ******************************************************************************************************************/


	/**
	 * Adds a preset to the list of connections in the Connection Manager
	 * @param {MongoBrowser} self - Please see Class/Namespace description!
	 * @param {string} name - the name of the connection
	 * @param {string} host - the host of the connection (ipv4, ipv6, domain)
	 * @param {number} port - the port of the connection
	 * @param {boolean} [performAuth=false] - whether to perform an authentication given the following parameters
	 * @param {string} [adminDatabase=admin] - the admin database which stores the user credentials and roles
	 * @param {string} [username=] - the username to authenticate with
	 * @param {string} [password=] - the password to authenticate with
	 * @param {string} [method=scram-sha-1] - one of ["scram-sha-1", "mongodb-cr"]
	 * @memberof MongoBrowser#
	 */
	function addConnectionPreset(self, name, host, port, performAuth, adminDatabase, username, password, method){
		if(typeof performAuth === "undefined"){ performAuth = false; }
		if(typeof adminDatabase === "undefined"){ adminDatabase = "admin"; }
		if(typeof username === "undefined"){ username = ""; }
		if(typeof password === "undefined"){ password = ""; }
		if(typeof method === "undefined"){ method = "scram-sha-1"; }

		self.state.connectionPresets.push({name:name, host:host, port:port, performAuth: performAuth,
					auth: {adminDatabase: adminDatabase, username: username, password: password, method: method}});
		self.uiElements.dialogs.connectionManager.initialise(self.uiElements.dialogs.connectionManager, self.state.connectionPresets);
	}

	/**
	 * Connects to a specific server and triggers the creation of all the gui elements
	 * (like collection list, tabs...) when the db-connection has been established.
	 * Note that there are two types of connections in this docu: <br/>
	 * 1) Connection to the backend-server: This is not a real socket-like connection
	 * but only the promise that there will be a controller handling our ajax-requests.
	 * They are epehemeral and created upon necessity and closed directly thereafter.
	 * We call them server-connections <br/>
	 * 2) Connection to the mongo-db: This is a real socket-connection to the actual
	 * mongo database. They are stored (if at all) only on the server to which we connect
	 * using server-connections. We call them db-connections.
	 * @param {MongoBrowser} self  - Please see Class/Namespace description!
	 * @param {string} hostname   - the hostname under which the mongodb is accessible
	 * @param {number} port       - the port at which the mongodb listens
	 * @param {string} database   - the database name to connect to
	 * @memberof MongoBrowser#
	 */
	function connect(self, hostname, port, database){
		var db = MongoNS.simple_connect(hostname, port, database);
		self.state.connections.push(db.getMongo());

		var mongo = db.getMongo();
		var databases = mongo.getDBNames();
		var listItem = $('<li class="collapsed"><span class="foldIcon">&nbsp;</span><span class="icon">&nbsp;</span><span class="listItem"></span></li>');

		var serverItem = listItem.clone().addClass("server");
		var databaseItems = $("<ul></ul>");

		serverItem.find(".listItem").text(hostname);
		serverItem.append(databaseItems);

		for(var i=0; i<databases.length; i++){
			var databaseName = databases[i];
			var dbItem = listItem.clone().addClass("database");
			var collectionsFolder = listItem.clone().addClass("folder");
			var foldersInDB = $("<ul></ul>");
			var collectionItems = $("<ul></ul>");

			dbItem.find(".listItem").text(databaseName);
			collectionsFolder.find(".listItem").text("Collections");

			collectionsFolder.append(collectionItems);
			foldersInDB.append(collectionsFolder);
			dbItem.append(foldersInDB);
			databaseItems.append(dbItem);

			var collections = mongo.getDB(databaseName).getCollectionNames();

			for(var j=0; j<collections.length; j++){
				var collection = collections[j];

				var collItem = listItem.clone().addClass("collection");
				collItem.find(".listItem").text(collection);
				collItem.on("dblclick", (function(database, collection){
					return function(){addTab(self, database, collection)};
				})(mongo.getDB(databaseName), collection));
				collectionItems.append(collItem);
			}
		}

		self.uiElements.sideBar.append(serverItem);

	}

	MongoBrowser.prototype.addConnectionPreset = function(){Array.prototype.unshift.call(arguments, this); return addConnectionPreset.apply(this, arguments)};
	MongoBrowser.prototype.connect             = function(){Array.prototype.unshift.call(arguments, this); return connect.apply(this, arguments)};

	return MongoBrowser;
})();


/**
 * A preset in the connection list of the connection manager. All the necessary information to create a db-connection.
 * @typedef {Object} MongoBrowser~connectionPreset
 * @property {string} name - the name of the connection
 * @property {string} host - the host of the connection (ipv4, ipv6, domain)
 * @property {number} port - the port of the connection
 * @property {boolean} performAuth - whether to perform an authentication given the following parameters
 * @property {Object} auth - information for the authentication
 * @property {string} auth.adminDatabase - the admin database which stores the user credentials and roles
 * @property {string} auth.username - the username to authenticate with
 * @property {string} auth.password - the password to authenticate with
 * @property {string} auth.method - one of ["scram-sha-1", "mongodb-cr"]
 */

/**
 * The options for mongobrowser
 * @typedef {Object} MongoBrowser~options
 * @property {MongoBrowser~connectionPreset[]} [connectionPresets] - a list of connection presets
 */