/*
 * This file is basically an extension for the file mongobrowser.js. Think of it like
 * #import "guiCreation.c" in C. Yes, it's just as ugly. BUT the other file got too large
 * to be handled reasonably.
 *
 * This exports a single function to the NS, which is used within mongobrowser.js. The
 * parameters to this function are the private members in the MongoBrowser class which are
 * used within this file and the return value are the private members to MongoBrowser which
 * should be added to the MongoBrowser class.
 *
 */

window.MongoBrowserNS = (function(MongoBrowserNS){

	//fetch private members of mongobrowser
	function getGuiCommands(openDialog, getCurrentTab, testConnection){

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
							self.connect(p.host, p.port, "test");
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
					connect(self, preset.host, preset.port, "test", //todo put correct database here
						preset.performAuth, preset.auth.adminDatabase, preset.auth.username, preset.auth.password, preset.auth.method);
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

				this.find(".connectionSettingsAuthenticationTab [name='password']").attr("type", "password");
				this.find(".connectionSettingsAuthenticationTab .revealPasswordButton").text("Show PW");
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

			function triggerConnectionTest(){
				var curDialog = self.uiElements.dialogs.connectionSettings

				var name = curDialog.find(".connectionName").val();
				var host = curDialog.find(".connectionHost").val();
				var port = parseInt(curDialog.find(".connectionPort").val());

				var adminDatabase = curDialog.find("[name=adminDatabase]").val();
				var username = curDialog.find("[name=username]").val();
				var password = curDialog.find("[name=password]").val();
				var method = curDialog.find("[name=method]").val();
				var performAuth = curDialog.find("[name=performAuth]").prop("checked");

				//todo: change test here, too!
				var testResult = testConnection(self, host, port, "test", performAuth, adminDatabase, username, password, method);
				if(testResult !== true){
					openDialog(self, "showMessage", "Test failed", testResult, "error");
				}else{
					openDialog(self, "showMessage", "Test successful", "Successfully connected, authenticated (if necesarry) and listed collections.", "success");
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
			 * Initialises the delete document dialog. This function usually cannot be called directly (except within
			 * the function body of {@link MongoBrowser(NS)~createDialogs createDialogs }), but is called everytime
			 * {@link MongoBrowser(NS)~openDialog openDialog } is called with "viewDocument" as first argument
			 * and a <tt>MongoNS.DB</tt> and document as third and fourth parameter
			 * @param {MongoNS.DB} db - the database to display at the top border
			 * @param {Object} doc - the document to remove
			 * @param {String} collection - the collection from which the doc is taken
			 * @memberof dialogInitialisators~
			 * @inner
			 */
			function initDeleteDocumentDialog(db, doc, collection){
				var curDialog = self.uiElements.dialogs.deleteDocument;
				var connection = db.getMongo().host.substr(0, db.getMongo().host.indexOf("/"));
				if(typeof doc._id === "undefined" || doc._id === null || typeof doc._id.tojson !== "function")
					var id = JSON.stringify(doc._id);
				else
					var id = doc._id.tojson();

				curDialog.find(".documentEditor").val(MongoNS.tojson(doc));
				curDialog.find(".info .connection span").text(connection);
				curDialog.find(".info .database span").text(db.toString());
				curDialog.find(".info .collection span").text(collection);
				curDialog.find(".reallyDeleteQuestion .docId").text(id);

			}


			/**
			 * Initialises the insert new document dialog. This function usually cannot be called directly (except within
			 * the function body of {@link MongoBrowser(NS)~createDialogs createDialogs }), but is called everytime
			 * {@link MongoBrowser(NS)~openDialog openDialog } is called with "viewDocument" as first argument
			 * and a <tt>MongoNS.DB</tt> and document as third and fourth parameter
			 * @param {MongoNS.DB} db - the database to display at the top border
			 * @param {String} collection - the collection into which the doc is to be inserted
			 * @memberof dialogInitialisators~
			 * @inner
			 */
			function initInsertDocumentDialog(db, collection){
				var curDialog = self.uiElements.dialogs.insertDocument;
				var connection = db.getMongo().host.substr(0, db.getMongo().host.indexOf("/"));

				curDialog.find(".documentEditor").val("{\n\n}");
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

			/**
			 * Initialises the show message dialog. This function usually cannot be called directly (except within
			 * the function body of {@link MongoBrowser(NS)~createDialogs createDialogs }), but is called everytime
			 * {@link MongoBrowser(NS)~openDialog openDialog } is called with "viewDocument" as first argument
			 * and a <tt>MongoNS.DB</tt> and document as third and fourth parameter
			 * @param {String} title - the title of the message
			 * @param {String} message - the message to display
			 * @param {String} [type] - a class to set on the icon div
			 * @memberof dialogInitialisators~
			 * @inner
			 */
			function initShowMessageDialog(title, message, type){
				if(typeof type === "undefined")
					type = "";

				var curDialog = self.uiElements.dialogs.showMessage;

				curDialog.find(".largeIcon").attr("class", "largeIcon " + type);
				curDialog.find(".title").text(title);
				curDialog.prev().find(".ui-dialog-title").text(title);
				curDialog.attr("title", title);
				curDialog.find(".content").text(message);
			}

			//begin connection manager
			var curDialog = self.uiElements.dialogs.connectionManager =
				self.rootElement.find(".connectionManager").dialog({
					autoOpen: false,
					dialogClass: "mongoBrowser",
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
					dialogClass: "mongoBrowser hasSpecialButton",
					buttons: [
						{text: "Test",
						icons: {primary: "testIcon"},
						click: triggerConnectionTest},
						{text: "Save",
						click: function(){getSaveAction()(); $(this).dialog("close");}},
						{text: "Cancel",
						click: function() {
							$( this ).dialog( "close" );
							}
						}
					],
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

			curDialog.find(".revealPasswordButton").on("click", function(){
				var passwordInput = self.uiElements.dialogs.connectionSettings.find(".connectionSettingsAuthenticationTab [name='password']");
				if(passwordInput.attr("type") === "password"){
					passwordInput.attr("type", "text");
					$(this).text("Hide PW");
				}else{
					passwordInput.attr("type", "password");
					$(this).text("Show PW");
				}

				return false;
			});

			curDialog.initialise = initConnectionSettingsDialog;
			curDialog.initialise();


			//begin document editor
			curDialog = self.uiElements.dialogs.editDocument = self.rootElement.find(".editDocument").dialog({
				autoOpen: false,
				dialogClass: "mongoBrowser hasSpecialButton",
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

			//begin document creator/insertor
			curDialog = self.uiElements.dialogs.insertDocument = self.rootElement.find(".insertDocument").dialog({
				autoOpen: false,
				dialogClass: "mongoBrowser hasSpecialButton",
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
			curDialog.initialise = initInsertDocumentDialog;

			//begin document delete
			curDialog = self.uiElements.dialogs.deleteDocument = self.rootElement.find(".deleteDocument").dialog({
				autoOpen: false,
				dialogClass: "mongoBrowser",
				buttons:[
						{text: "Yes",
						click: TODO},
						{text: "No",
						click: function() {
							$( this ).dialog( "close" );
							}
						}
					],
				modal: true,
				width: "auto",
				height: "auto",
			});
			curDialog.initialise = initDeleteDocumentDialog;

			//begin showMessage
			curDialog = self.uiElements.dialogs.showMessage = self.rootElement.find(".showMessage").dialog({
				autoOpen: false,
				dialogClass: "mongoBrowser",
				buttons:[
						{text: "OK",
						click: function() {
							$( this ).dialog( "close" );
							}
						}
					],
				modal: true,
				width: 750,
				height: "auto"
			});
			curDialog.initialise = initShowMessageDialog;

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
			var fragment = $("#MongoBrowserDummy");

			if(fragment.size() === 0){
				// TODO
				throw new Error("Creating UI from scratch is not yet implemented");
			}

			if(typeof fragment[0].content === "undefined"){
				//template tag is not yet supported
				var dummy = $("#MongoBrowserDummy").children().clone();
			}else{
				var dummy = $(document.importNode(fragment[0].content, true)).children();
			}

			self.rootElement = self.uiElements.root = dummy;
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
			sideBar.delegate("li", "click", function(evt){
				sideBar.find(".current").removeClass("current");
				$(evt.target).closest("li").addClass("current");})
			sideBar.parent().resizable({handles: "e", minWidth: 120});
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
				self.state.tabs[panelId].uiElements.resultsTable.resizableColumns("destroy");
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
						expand:    {name: "Expand recursively",
						            callback: function(){collapseOrExpandResult($(this), false, true);},
						            disabled: function(){return !$(this).hasClass("hasChildren")}},
						collapse:  {name: "Collapse recursively",
						            callback: function(){collapseOrExpandResult($(this), true, true);},
						            disabled: function(){return !$(this).hasClass("hasChildren")}},
						"sep1": "---------",
						edit:      {name: "Edit Document...",
						            callback: function(){
						            	var idx = parseInt($(this).attr("data-index"));
						            	var curTab = getCurrentTab(self);
						            	openDialog(self, "editDocument", curTab.getInfo().database, curTab.getDataRow(idx), curTab.getInfo().collection);
						            }},
						view:      {name: "View Document...",
						            callback: function(){
						            	var idx = parseInt($(this).attr("data-index"));
						            	var curTab = getCurrentTab(self);
						            	openDialog(self, "viewDocument", curTab.getInfo().database, curTab.getDataRow(idx), curTab.getInfo().collection);
						            }},
						insert:    {name: "Insert Document...",
						            callback: function(){
						            	var curTab = getCurrentTab(self);
						            	openDialog(self, "insertDocument", curTab.getInfo().database, curTab.getInfo().collection);
						            }},
						"sep2": "---------",
						copyJSON:  {name: "Copy JSON",
						            //callback: copyJSONAndValue,
						            disabled: function(){return !$(this).hasClass("hasChildren")},
						            },
						copyValue: {name: "Copy Value",
						            //callback: copyJSONAndValue,
						            disabled: function(){return $(this).hasClass("hasChildren")},
						           },
						delete:    {name: "Delete Document...",
						            callback: function(){
						            	var idx = parseInt($(this).attr("data-index"));
						            	var curTab = getCurrentTab(self);
						            	openDialog(self, "deleteDocument", curTab.getInfo().database, curTab.getDataRow(idx), curTab.getInfo().collection);
						           }}
					}
				});

			function copyJSONAndValue(){
				var idx = parseInt($(this).attr("data-index"));
				var keyList = getKeyList($(this));

				var obj = getCurrentTab(self).getDataRow(idx);
				for(var i=0; i < keyList.length; i++){
					obj = obj[keyList[i]];
				}

				var elem = $("<p>").attr("data-clipboard-text", JSON.stringify(obj));

				var c = new Clipboard(elem[0]);
				elem.click(); c.destroy(); elem.remove(); //well that was quick :/
			}

			/** Helper function traversing the items in the results table and returning a
			 * list of keys to reach the property representd by the line `elem`
			 *
			 * @param {jQuery} elem - the element representing the property to get the key list to
			 */
			function getKeyList(elem){
				var depth = parseInt(elem.attr("data-indent"));
				if(depth === 0)
					return [];

				var prevList = getKeyList(elem.prevUntil("[data-indent='" + (depth - 1) + "']").addBack().first().prev());
				prevList.push(elem.attr("data-key"));

				return prevList;
			}
		}

		return {
			createRootElement: createRootElement,
			createDialogs: createDialogs,
			createTabEnvironment: createTabEnvironment,
			createActionBarButtons: createActionBarButtons,
			createSidebarEnvironment: createSidebarEnvironment
		}
	}


	//Import dependencies
	if(typeof MongoBrowserNS === "undefined")
		throw Error("Could not fetch dependency: TabFactory");

	var TabFactory = MongoBrowserNS.TabFactory;

	//Export additional functions to namespace
	MongoBrowserNS.getGuiCommands = getGuiCommands;

	return MongoBrowserNS;
})(window.MongoBrowserNS);