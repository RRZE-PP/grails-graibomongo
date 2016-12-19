<template id="MongoBrowserDummy">
	<div style="width:100%; border-radius:2px;padding:1px;border:2px solid #333; display:none; min-height: 550px;"  tabindex="-1">
		<div style="" class="menuBar">
			<span class="menuRootElem">File
					<ul><li data-callback='openConnection'>Open Connection</li></ul>
			</span>
			<span class="menuRootElem hasCheckbox">Options
					<ul><li data-callback='windowMode'><label><input type="checkbox" name="windowMode"><div>Window mode</div></label></li>
						<li data-callback='expandFirstDoc'><label><input type="checkbox" name="expandFirstDoc"><div>Auto expand first document</div></label></li>
						<li data-callback='autoExecuteCode'><label><input type="checkbox" name="autoExecuteCode"><div>Automatically execute code in new tab</div></label></li></ul>
			</span>
			<span class="menuRootElem">Help
					<ul><li data-callback='about'>About</li>
						<li><a href="https://github.com/RRZE-PP/grails-graibomongo/issues/new">Report Bug or Feature Request</a></li></ul>
			</span>
		</div>
		<div style="display:flex; flex-direction:row;" class="actionBar">
			<div class="connectionButton dropDown button"><img src="assets/images/connect_24x24.png" /></div>
			<div class="divider"></div>
			<div class="executeButton button active"><img src="assets/images/execute_24x24.png" /></div>
			<div class="stopButton button inactive"><img src="assets/images/stop_24x24.png" /></div>
			<div class="rotateButton button inactive"><img src="assets/images/rotate_24x24.png" /></div>
			&nbsp;
		</div>
		<div class="mainBody">
			<div class="sideBar">
				<ul>
				</ul>
			</div>
			<div class="mainTabs tabContainer">
				<ul class="tabList"><li><a href="#"><img src="assets/images/mongodb_16x16.png" /><span class="tabText"></span></a><span class="closeButton">✖</span></li></ul>
				<div class="tab" style="display: none">
					<div class="promptContainer">
						<span class="info">
							<span class="connection"><img src="assets/images/server_16x16.png" alt="connection"><span></span></span>
							<span class="database"><img src="assets/images/database_16x16.png" alt="database"><span></span></span>
						</span>

						<div class="prompt">
							<textarea class="userInput"></textarea>
						</div>
					</div>

					<div class="buttonBar">
						<span class="collection"><img src="assets/images/collection_16x16.png"> <span></span></span>
						<span class="time"><img src="assets/images/time_16x16.png"> <span></span> sec.</span>
						<span class="pages"><span class="prevIterate"><img src="assets/images/left_16x16.png"></span><input class="startIterate" type="number" value="0" >&nbsp;<input value="20" class="maxIterate" type="number"><span class="nextIterate"><img src="assets/images/right_16x16.png"></span></span>

					</div>

					<div class="resultsTableContainer">
						<div class="printContainer">
							<pre></pre>
						</div>
						<table class="resultsTable">
							<thead>
								<tr>
									<th>Key</th>
									<th>Value</th>
									<th>Type</th>
								</tr>
							</thead>
							<tbody>
							</tbody>
						</table>
					</div>
				</div>
				<span style="clear:both"></span>
			</div>
		</div>
		<div class="statusBar">
			Graibomongo is not affiliated with <a href="https://robomongo">robomongo</a>. We simply love their application and tried to bring its core features to the web.
		</div>
		<div class="windowContainer" style="display:none">
			<div class="connectionManager window modal" title="MongoDB Connections">
				<a href="#" class="connectionCreateLink">Create</a>, <a href="#" class="connectionEditLink">edit</a>, <a href="#" class="connectionRemoveLink">remove</a>, <a href="#" class="connectionCloneLink">clone</a> or <s>reorder connections via drag'n'drop</s>.
				<table class="connectionsTable">
					<thead>
						<tr>
							<th>Name</th><th>Address</th><th>Auth. Database / User</th>
						</tr>
					</thead>
					<tbody>
					</tbody>
				</table>
			</div>

			<div class="connectionSettings window modal" title="Connection settings">
				<form>
					<div class="connectionTabs tabContainer">
						<ul class="tabList">
							<li><a href="#connectionSettingsConnectionTab">Connection</a></li>
							<li><a href="#connectionSettingsAuthenticationTab">Authentication</a></li>
						</ul>
						<div id="connectionSettingsConnectionTab" class="tab connectionSettingsConnectionTab">
							<table class="alignmentHelper">
								<tr><td>Name:</td> <td><input type="text" class="connectionName"> <br />Choose any connection name that will help you to identify this connection</td></tr>
								<tr><td>&nbsp;</td><td></td></tr>
								<tr><td>Address:</td><td><input type="text" class="connectionHost"> : <input type="number" value="27017" class="connectionPort" /> <br />Specify host and port of MongoDB server. Host can be either IPv4, IPv6 or domain name.</td></tr>
							</table>
						</div>
						<div id="connectionSettingsAuthenticationTab" class="tab connectionSettingsAuthenticationTab">
							<label><input type="checkbox" name="performAuth" /> Perform authentication</label>

							<div class="connectionSettingsEnableDisableSwitch disabled">

								<table class="alignmentHelper">
									<label><tr><td>Database: </td><td><input type="text" name="adminDatabase" disabled="disabled"/> </td></tr></label>
									<tr><td>&nbsp;</td>           <td>The admin database is unique in MongoDB. Users with normal access to the admin database have read and write access to <b>all databases</b>.</td></tr>
									<label><tr><td>User Name: </td><td><input type="text" name="username" disabled="disabled"/></td></tr></label>
									<label><tr><td>Password: </td><td><input type="password" name="password" disabled="disabled"/><button class="revealPasswordButton">Show PW</button></td></tr></label>
									<tr><td>&nbsp;</td><td>Please bear in mind: This password will possibly be stored in plain text in your browser and possibly sent
									in plain text over your internet connection.</td></tr>
									<label><tr><td>Auth Mechanism:</td>
										<td><select name="method" disabled="disabled">
												<option value="scram-sha-1">SCRAM-SHA-1</option>
												<option value="mongodb-cr">MONGODB-CR</option>
											</select>
										</td></tr></label>
								</table>
							</div>
						</div>
					</div>
				</form>
			</div>

			<div class="editDocument window modal" title="Edit Document">
					<span class="info">
							<span class="connection"><img src="assets/images/server_16x16.png" alt="connection"><span></span></span>
							<span class="database"><img src="assets/images/database_16x16.png" alt="database"><span></span></span>
							<span class="collection"><img src="assets/images/collection_16x16.png"> <span></span></span>
					</span>
					<div class="documentContainer">
						<textarea class="documentEditor userInput"></textarea>
					</div>
			</div>

			<div class="insertDocument window modal" title="Insert Document">
					<span class="info">
							<span class="connection"><img src="assets/images/server_16x16.png" alt="connection"><span></span></span>
							<span class="database"><img src="assets/images/database_16x16.png" alt="database"><span></span></span>
							<span class="collection"><img src="assets/images/collection_16x16.png"> <span></span></span>
					</span>
					<div class="documentContainer">
						<textarea class="documentEditor userInput"></textarea>
					</div>
			</div>

			<div class="deleteDocument window modal" title="Delete Document">
					<span class="info">
							<span class="connection"><img src="assets/images/server_16x16.png" alt="connection"><span></span></span>
							<span class="database"><img src="assets/images/database_16x16.png" alt="database"><span></span></span>
							<span class="collection"><img src="assets/images/collection_16x16.png"> <span></span></span>
					</span><br /><br />
					<span class="reallyDeleteQuestion">Really delete document with id: <span class="docId"></span>?</span>
					<div class="documentContainer">
						<textarea class="documentEditor userInput" disabled="disabled"></textarea>
					</div>
			</div>

			<div class="viewDocument window modal" title="View Document">
					<span class="info">
							<span class="connection"><img src="assets/images/server_16x16.png" alt="connection"><span></span></span>
							<span class="database"><img src="assets/images/database_16x16.png" alt="database"><span></span></span>
							<span class="collection"><img src="assets/images/collection_16x16.png"> <span></span></span>
					</span>
					<div class="documentContainer">
						<textarea class="documentEditor userInput" disabled="disabled"></textarea>
					</div>
			</div>

			<div class="showMessage window modal" title="">
				<div class="largeIcon"></div>
				<div class="message">
					<span class="title"></span><br/>
					<span class="content"></span>
				</div>
			</div>
			</div>
		</div>
	</div>
</template>