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
window.MongoBrowserNS = (function(MongoBrowserNS){

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
		var options = self.options = typeof options !== "undefined" ? options : {};

		function def(optionName, defValue){
			options[optionName] = (typeof options[optionName] === "undefined") ? defValue : options[optionName];
		}

		def("assetPrefix",  "");
		def("window", "resizable");
		def("autoExecuteCode", true);
		def("expandFirstDoc", true);

		self.state = {
			connectionPresets: typeof options.connectionPresets !== "undefined" ? options.connectionPresets : [],
			connections: [],
			tabs: {},
			currentFocus: null //saves which element currently has focus *within this instance*
		}

		self.instanceNo = MongoBrowser.instanceCount++;

		initUIElements(self);

		self.rootElement.appendTo(appendTo);
		self.rootElement.addClass("mongoBrowser");
		self.rootElement.attr("id", "mongoBrowser-"+self.instanceNo)
		self.rootElement.css("display", "");

		self.rootElement.resizable({minWidth:642, minHeight:550});

		var optionKeys = Object.keys(options);
		for(var i = 0; i < optionKeys.length; i++){
			var option = optionKeys[i];
			self.option(option, options[option]);
		}

		MongoBrowser.instances["mongoBrowser-"+self.instanceNo] = self;
	}

	/** The total number of MongoBrowsers created to savely create unique IDs
	 * @static
	 * @memberof MongoBrowser
	 * @type {number}
	 */
	MongoBrowser.instanceCount = 0;

	/** All MongoBrowserInstances by id where the key is the id and the value is the instance
	 * @static
	 * @memberof MongoBrowser
	 * @type {Object}
	 */
	MongoBrowser.instances = {};

	/** 1) Sets up a global onclick handler which determines which mongobrowser instance has "focus".
	 *     This is necesarry to send keyboard shortcuts to the right instance (or none if none has focus).
	 *  2) Sets up a global onkeypress handler which forwards the events to the focused instance.
	 *  Executed only once upon startup.
	 *
	 * @static
	 * @private
	 * @memberof MongoBrowser
	 */
	function sendEventsToCorrectInstance(){
		var focusedMongoBrowser = null;
		$(document).click(function(event){
			var target = $(event.target);

			var id = target.closest(".mongoBrowser").attr("id");
			if(typeof id === "undefined"){
				focusedMongoBrowser = null;
			}else{
				focusedMongoBrowser = MongoBrowser.instances[id];
			}
		});

		$(document).keyup(function(event){
			if(focusedMongoBrowser !== null)
				return handleKeypress(focusedMongoBrowser, event);
		});

		//keydown events should not scroll
		$(document).keydown(function(event){
			if(focusedMongoBrowser !== null && focusedMongoBrowser.state.currentFocus !== null
				&& focusedMongoBrowser.state.currentFocus.is(".sideBar, .resultsTable")
				&& (event.key === "ArrowUp" || event.key === "ArrowDown") ){
				event.preventDefault();
			}
		});
	}
	sendEventsToCorrectInstance();

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
		if(self.options.autoExecuteCode)
			tab.execute();
		tab.select();
		self.state.tabs[tab.id()] = tab;
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

	function handleKeypress(self, event){
		if(self.state.currentFocus === null)
			return;

		if(event.ctrlKey && event.key === "Enter"){
			//if anything inside the tab-environment has focus, execute
			if(self.uiElements.tabs.container.find(self.state.currentFocus).size() > 0){
				getCurrentTab(self).execute();

				event.preventDefault();
				return false;
			}
		}

		if(event.key === "Enter"){
			if(self.state.currentFocus === self.uiElements.sideBar){
				self.uiElements.sideBar.find(".current").dblclick();

				event.preventDefault();
				return false;
			}
		}

		if(event.key === "ArrowUp" || event.key === "ArrowDown"){
			var current = self.state.currentFocus.find(".current");

			if(self.state.currentFocus === self.uiElements.sideBar)
				var pool = m.uiElements.sideBar.find(".server, li:not('.collapsed') > ul > li");
			else if(self.state.currentFocus.hasClass("resultsTable"))
				var pool = self.state.currentFocus.find("tbody tr:visible");
			else
				return false;

			if(event.key === "ArrowUp")
				var nextCurrent = pool.eq(pool.index(current) === 0 ? 0 : pool.index(current) - 1);
			else
				var nextCurrent = pool.eq(pool.index(current) + 1);

			if(nextCurrent.size() == 0) return false;

			current.removeClass("current");
			nextCurrent.addClass("current");

			event.preventDefault();
			return false;
		}

		if(event.key === "ArrowRight"){
			var current = self.state.currentFocus.find(".current");
			if(current.hasClass("collapsed")){
				current.find(".foldIcon").first().click();

				event.preventDefault();
				return false;
			}
		}

		if(event.key === "ArrowLeft"){
			var current = self.state.currentFocus.find(".current");
			if(current.hasClass("opened")){
				current.find(".foldIcon").first().click();

				event.preventDefault();
				return false;
			}
		}

		if(self.state.currentFocus.is(".prompt")){
			getCurrentTab(self).showHint();
			return false;
		}

		return true;
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
		createMenuBar(self);
		createDialogs(self);
		createTabEnvironment(self);
		createActionBarButtons(self);
		createSidebarEnvironment(self);

		setUpFocusHandlers(self);
	}

	function setUpFocusHandlers(self){

		function grantFocusTo(element, event){
			if(self.state.currentFocus !== null)
				self.state.currentFocus.removeClass("focused");
			self.state.currentFocus = element;
			self.state.currentFocus.addClass("focused");

			//we will stop propagating the event, but we have a onclick handler on document
			//so re-raise the event there
			$(document).trigger(event);
			event.stopPropagation();
			return false;
		}

		self.uiElements.sideBar.delegate("li", "click", function(event){
			return grantFocusTo(self.uiElements.sideBar, event);
		});

		self.uiElements.tabs.container.delegate(".promptContainer .prompt .userInput, .promptContainer .prompt .CodeMirror", "click focus", function(event){
			return grantFocusTo($(event.currentTarget).parent(), event);
		});

		self.uiElements.tabs.container.delegate(".resultsTable", "click focus", function(event){
			return grantFocusTo($(event.currentTarget), event);
		});

		self.rootElement.click(function(){
			self.rootElement.find(".focused").removeClass("focused");
			self.state.currentFocus = null;
		});
	}

	/**
	 * Opens one of the dialogs from self.uiElements.dialogs. Currently there are:
	 * connectionManager, connectionSettings, editDocument, viewDocument, insertDocument, deleteDocument
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

	/**
	 * Tests the db- and server-connection by connecting and listing the collection names on `database`. Does not change
	 * the sideBar though (like connect does) and does not change the state-object of this mongobrowser (i.e. does not
	 * store the connection).
	 *
	 * @param {MongoBrowser} self  - Please see Class/Namespace description!
	 * @param {string} hostname   - the hostname under which the mongodb is accessible for the db-connection
	 * @param {number} port       - the port at which the mongodb listens for the db-connection
	 * @param {string} database   - the database name to connect to
	 * @param {boolean} [performAuth=false] - whether to perform an authentication given the following parameters
	 * @param {string} [adminDatabase=admin] - the admin database which stores the user credentials and roles
	 * @param {string} [username=] - the username to authenticate with
	 * @param {string} [password=] - the password to authenticate with
	 * @param {string} [method=scram-sha-1] - one of ["scram-sha-1", "mongodb-cr"]
	 * @private
	 * @memberof MongoBrowser(NS)~
	 * @returns {boolean|string} true if the test was successful or a string describing the error
	 */
	function testConnection(self, hostname, port, database, performAuth, adminDatabase, username, password, method){
		if(typeof performAuth === "undefined"){ performAuth = false; }
		if(typeof adminDatabase === "undefined"){ adminDatabase = "admin"; }
		if(typeof username === "undefined"){ username = ""; }
		if(typeof password === "undefined"){ password = ""; }
		if(typeof method === "undefined"){ method = "scram-sha-1"; }

		try {
			if(performAuth){
				var db = MongoNS.simple_connect(hostname, port, database, username, password, adminDatabase, method, true);
			}else{
				var db = MongoNS.simple_connect(hostname, port, database);
			}
			var mongo = db.getMongo();

			mongo.getDB(database).getCollectionNames();
		}catch(e){
			return e.toString();
		}

		return true;
	}


	function windowMode(self, action){
		if(action === "enter"){
			self.rootElement.dialog({minWidth:642, minHeight:550});
			self.uiElements.menuBar.find("[name='windowMode']").prop("checked", true);
		}else if(action === "leave"){
			self.rootElement.is(".ui-dialog-content") && self.rootElement.dialog("destroy");
			self.uiElements.menuBar.find("[name='windowMode']").prop("checked", false);
		}
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
	 * @param {string} hostname   - the hostname under which the mongodb is accessible for the db-connection
	 * @param {number} port       - the port at which the mongodb listens for the db-connection
	 * @param {string} database   - the database name to connect to
	 * @param {boolean} [performAuth=false] - whether to perform an authentication given the following parameters
	 * @param {string} [adminDatabase=admin] - the admin database which stores the user credentials and roles
	 * @param {string} [username=] - the username to authenticate with
	 * @param {string} [password=] - the password to authenticate with
	 * @param {string} [method=scram-sha-1] - one of ["scram-sha-1", "mongodb-cr"]
	 * @memberof MongoBrowser#
	 */
	function connect(self, hostname, port, database, performAuth, adminDatabase, username, password, method){
		try {
			if(typeof performAuth === "undefined"){ performAuth = false; }
			if(typeof adminDatabase === "undefined"){ adminDatabase = "admin"; }
			if(typeof username === "undefined"){ username = ""; }
			if(typeof password === "undefined"){ password = ""; }
			if(typeof method === "undefined"){ method = "scram-sha-1"; }

			if(performAuth){
				var db = MongoNS.simple_connect(hostname, port, database, username, password, adminDatabase, method, true);
			}else{
				var db = MongoNS.simple_connect(hostname, port, database);
			}
			self.state.connections.push(db.getMongo());

			var mongo = db.getMongo();
			var databases = mongo.getDBNames();
			var listItem = $('<li class="collapsed"><span class="foldIcon">&nbsp;</span><span class="icon">&nbsp;</span><span class="listItem"></span><div class="selectionIndicator"></div></li>');

			var serverItem = listItem.clone().addClass("server");
			var databaseItems = $("<ul></ul>");

			var systemItem = listItem.clone().addClass("folder");
			var systemDatabases = $("<ul></ul>");
			systemItem.append(systemDatabases);
			systemItem.find(".listItem").text("System");

			databaseItems.append(systemItem);

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

				if(databaseName === "admin" || databaseName === "local")
					systemDatabases.append(dbItem);
				else
					databaseItems.append(dbItem);

				var collections = mongo.getDB(databaseName).getCollectionNames();

				for(var j=0; j<collections.length; j++){
					var collection = collections[j];

					var collItem = listItem.clone().addClass("collection");
					collItem.find(".listItem").text(collection);
					collItem.on("dblclick", (function(mongo, databaseName, collection){
						return function(){addTab(self, mongo.getDB(database), collection)};
					})(mongo, databaseName, collection));
					collectionItems.append(collItem);
				}
			}

			self.uiElements.sideBar.append(serverItem);
		}catch(e){
			openDialog(self, "showMessage", "Could not connect", e.toString(), "error");
		}
	}

	function option(self, option, value){
		if(typeof value === "undefined")
			return self.options[option];

		var optionCallbacks = {
			autoExecuteCode: function(v) {self.uiElements.menuBar.find("[name='autoExecuteCode']").prop("checked", v);},
			expandFirstDoc: function(v) {self.uiElements.menuBar.find("[name='expandFirstDoc']").prop("checked", v);},
			window: function(v) {if(v === "moveable") windowMode(self, "enter");
			                     else if(v === "resizable") windowMode(self, "leave");
			                     else return false}
		}

		if(typeof optionCallbacks[option] === "function" && optionCallbacks[option](value) === false)
			return;

		self.options[option] = value;

	}

	//Export MongoBrowser API
	MongoBrowser.prototype.addConnectionPreset = function(){Array.prototype.unshift.call(arguments, this); return addConnectionPreset.apply(this, arguments)};
	MongoBrowser.prototype.connect             = function(){Array.prototype.unshift.call(arguments, this); return connect.apply(this, arguments)};
	MongoBrowser.prototype.option              = function(){Array.prototype.unshift.call(arguments, this); return option.apply(this, arguments)};

	//Import dependencies
	if(typeof MongoBrowserNS === "undefined")
		throw Error("Could not fetch dependency: TabFactory, Gui creation code");

	var TabFactory = MongoBrowserNS.TabFactory;

	var guiCommands = MongoBrowserNS.getGuiCommands(openDialog, getCurrentTab, testConnection);
	var createRootElement = guiCommands.createRootElement;
	var createDialogs = guiCommands.createDialogs;
	var createTabEnvironment = guiCommands.createTabEnvironment;
	var createActionBarButtons = guiCommands.createActionBarButtons;
	var createSidebarEnvironment = guiCommands.createSidebarEnvironment;
	var createMenuBar = guiCommands.createMenuBar;

	//Export MongoBrowser as global name
	window.MongoBrowser = MongoBrowser;

	//Delete now unnecesarry NS
	delete window.MongoBrowserNS;
	return undefined;
})(window.MongoBrowserNS);


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
 * @property {String} [window] - controlls the main window. if it is set to "moveable" the window will behave like a regular
 *                               dialog (mimicing a window in MS Windows). If set to "resizable" it's a static div, but you
 *                               can change its size
 * @property {String} [assetPrefix] - a prefix to prepend to assets. can be used to redirect e.g. images to a cdn.
 */