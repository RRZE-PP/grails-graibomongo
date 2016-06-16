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
			self.uiElements.results.append($("<tr data-indent='" + indent + "'  class='collapsed " + (hasChildren ? "hasChildren" : "") + "' \
				style='"+ (indent > 0 ? "display:none" : "") + "'> \
				<td><span class='foldIcon'>&nbsp;</span> <img src='images/" + image + "' class='typeIcon' alt='" + alt + "' /> " + col1 + "</td> \
				<td>" + col2 + "</td> \
				<td>" + col3 + "</td></tr>"));
		}

		function printObject(key, val, indent) {
			var keys = Object.keys(val);

			base_print(indent, "bson_object_16x16.png", "object", key, "{ " + keys.length + " }", "Object", keys.length !== 0);

			for(var i=0; i<keys.length; i++){
				printLine(keys[i], val[keys[i]], indent + 1);
			}
		}

		function printArray(key, val, indent) {
			var keys = Object.keys(val);

			base_print(indent, "bson_array_16x16.png", "array", key, "[ " + val.length + " Elements ]", "Array", keys.length !== 0);

			for(var i=0; i<keys.length; i++){
				printLine(keys[i], val[keys[i]], indent + 1);
			}
		}

		function printString(key, val, indent) {
			base_print(indent, "bson_string_16x16.png", "string", key, val, "String");
		}

		function printNumber(key, val, indent) {
			base_print(indent, "bson_double_16x16.png", "number", key, val, "Double or Long or Int :(");
		}

		function printBoolean(key, val, indent) {
			base_print(indent, "bson_bool_16x16.png", "boolean", key, val, "Boolean");
		}

		function printNull(key, val, indent) {
			base_print(indent, "bson_null_16x16.png", "null", key, "null", "Null");
		}

		function printUndefined(key, val, indent) {
			base_print(indent, "bson_unsupported_16x16.png", "undefined", key, "undefined", "Undefined");
		}

		function printUnsupported(key, val, indent) {
			base_print(indent, "bson_unsupported_16x16.png", "unsupported", key, "", "unsupported");
		}

		function printLine(key, val, indent) {
			if(val instanceof Array)
				printArray(key, val, indent);
			else if(typeof val === "string" || val instanceof String)
				printString(key, val, indent);
			else if(typeof val === "number" || val instanceof Number) //TODO: Int vs Double!
				printNumber(key, val, indent);
			else if(typeof val === "boolean")
				printBoolean(key, val, indent);
			else if(val === null) //TODO: Int vs Double!
				printNull(key, val, indent);
			else if(typeof val === "undefined")
				printUndefined(key, val, indent);
			else if(typeof val === "object") //this comes last after all others have been ruled out
				printObject(key, val, indent);
			else
				printUnsupported(key, val, indent); //should not happen
		}

		var startTime = $.now();

		var ret = MongoNS.execute(MongoNS, this.state.db, this.uiElements.prompt.val());
		if(ret instanceof MongoNS.DBQuery)
			ret = ret._exec()

		var duration = $.now() - startTime;
		this.uiElements.info.time.text(duration/1000);

		this.uiElements.results.children().remove();

		if(ret instanceof MongoNS.Cursor){
			for(var i=0; i < parseInt(this.uiElements.iterate.max.val()) && ret.more(); i++){
				printLine("(" + (i + 1) + ")", ret.next(), 0);
			}
		}else{
			printLine("(" + 1 + ")", ret, 0);
		}
		this.uiElements.results.children("[data-indent]").each(function(index, elem){
			$(elem).children().eq(0).css("padding-left", parseInt($(elem).attr("data-indent"))*50+"px");
		});

		return ret;
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

		initUIElements(self);

		self.rootElement.appendTo(appendTo);
		self.rootElement.addClass("mongoBrowser");
		self.rootElement.css("display", "block");
	}

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

			this.find(".connectionName").val(preset.name);
			this.find(".connectionHost").val(preset.host);
			this.find(".connectionPort").val(preset.port);
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

			var name = self.uiElements.dialogs.connectionSettings.find(".connectionName").val();
			var host = self.uiElements.dialogs.connectionSettings.find(".connectionHost").val();
			var port = self.uiElements.dialogs.connectionSettings.find(".connectionPort").val();

			self.state.connectionPresets[idx] = {name:name, host:host, port:port};
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

		//begin connetion manager
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

		curDialog.initialise = initConnectionSettingsDialog;
		curDialog.initialise();
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
		var container = self.uiElements.tabs.container = self.rootElement.find(".tabContainer");

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

		//make result items collapsible
		self.uiElements.tabs.container.delegate(".foldIcon", "click", function(){
			$(this).parentsUntil("tr").parent().dblclick();
		});
		self.uiElements.tabs.container.delegate("tr", "dblclick", function(){
			var tr = $(this);
			var collapse = tr.hasClass("opened");
			var depth = parseInt(tr.attr("data-indent"));

			var next = tr.next();
			var skipAllDeeperThan = null;
			while(next.size() !== 0 && parseInt(next.attr("data-indent")) > depth){
				if(skipAllDeeperThan !== null){
					if(parseInt(next.attr("data-indent")) > skipAllDeeperThan){
						next = next.next();
						continue;
					}
					skipAllDeeperThan = null; //only, when not continued
				}
				if(!collapse && next.hasClass("collapsed")){ //when opening skip nested collapsed TRs
					skipAllDeeperThan = parseInt(next.attr("data-indent"));
				}
				next.css("display", collapse?"none":"table-row");
				next = next.next();
			}

			tr.removeClass("collapsed opened").addClass(collapse?"collapsed":"opened");
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
	 * @param {MongoBrowser~connectionPreset[] | MongoBrowser~connectionPreset} [initArg] -
	 *                                      when set, the initialisation function of the
	 *                                      dialog will be called (if it exists) and initArg passed as parameter <br />
	 *                                      for further information, see the parameters to the functions in
	 *                                      {@link dialogInitialisators }
	 * @throws {ReferenceError} When no dialog with the name 'dialogName' exists
	 * @private
	 * @memberof MongoBrowser(NS)~
	 */
	function openDialog(self, dialogName, initArg){
		var dialog = self.uiElements.dialogs[dialogName];
		if(typeof dialog === "undefined")
			throw new ReferenceError("No such dialog: "+dialogName);

		if(typeof initArg !== "undefined" && typeof dialog.initialise !== "undefined")
			dialog.initialise(initArg);

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
	 * @memberof MongoBrowser#
	 */
	function addConnectionPreset(self, name, host, port){
		self.state.connectionPresets.push({name: name, host: host, port:port});
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
 */

/**
 * The options for mongobrowser
 * @typedef {Object} MongoBrowser~options
 * @property {MongoBrowser~connectionPreset[]} [connectionPresets] - a list of connection presets
 */