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
		self.rootElement.attr("id", "mongoBrowser-"+self.instanceNo)
		self.rootElement.css("display", "");

		if(options.window === "moveable")
			self.rootElement.dialog({minWidth:642, minHeight:550});
		else if(options.window === "resizable")
			self.rootElement.resizable({minWidth:642, minHeight:550});
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
		var listItem = $('<li class="collapsed"><span class="foldIcon">&nbsp;</span><span class="icon">&nbsp;</span><span class="listItem"></span></li>');

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

	}

	//Export MongoBrowser API
	MongoBrowser.prototype.addConnectionPreset = function(){Array.prototype.unshift.call(arguments, this); return addConnectionPreset.apply(this, arguments)};
	MongoBrowser.prototype.connect             = function(){Array.prototype.unshift.call(arguments, this); return connect.apply(this, arguments)};

	//Import dependencies
	if(typeof MongoBrowserNS === "undefined")
		throw Error("Could not fetch dependency: TabFactory, Gui creation code");

	var TabFactory = MongoBrowserNS.TabFactory;

	var guiCommands = MongoBrowserNS.getGuiCommands(openDialog, getCurrentTab);
	var createRootElement = guiCommands.createRootElement;
	var createDialogs = guiCommands.createDialogs;
	var createTabEnvironment = guiCommands.createTabEnvironment;
	var createActionBarButtons = guiCommands.createActionBarButtons;
	var createSidebarEnvironment = guiCommands.createSidebarEnvironment;

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
 */