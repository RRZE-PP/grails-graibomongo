package de.rrze.graibomongo

import grails.converters.JSON

class ConnectionPreset {
	String name;
	String host;
	int port;

	boolean performAuth;
	boolean hidePassword = false;
	String username;
	String password;
	String authDatabase;
	Method method;

	static connectionIDToPreset = [:];
	static presetToConnectionID = [:];

	public enum Method {
		SCRAM_SHA_1("scram-sha-1"),
		MONGODB_CR("mongodb-cr")
	    String id

	    Method(String id) {
	        this.id = id
	    }
	}

	public static String getPasswordFromCache(String connectionId){
		if(connectionIDToPreset.containsKey(connectionId)){
			return connectionIDToPreset[connectionId].password;
		}else{
			throw new IllegalArgumentException("No password with this ID cached")
		}
	}

	ConnectionPreset(String name, String host, int port = 27017){
		this.name = name;
		this.host = host;
		this.port = port;
	}

	def auth(String username, String password, String database = "admin", Method method = Method.SCRAM_SHA_1){
		this.username = username;
		this.password = password;
		this.authDatabase = database;
		this.method = method;
		this.performAuth = true;

		return this;
	}

	def hidePassword(){
		this.hidePassword = true;

		return this;
	}

	public int hashCode(){
		return name.hashCode() ^ host.hashCode() ^ (performAuth?1:0) ^ (hidePassword?1:0) ^
		         username.hashCode() ^ password.hashCode() ^ authDatabase.hashCode() ^ method.hashCode();
	}

	public boolean equals(other){
		if(this.hashCode() == other.hashCode()){
			return name.equals(other.name) && host.equals(other.host) &&
			       (performAuth == other.performAuth) && (hidePassword == other.hidePassword) &&
		           username.equals(other.username) && password.equals(other.password) &&
		           authDatabase.equals(other.authDatabase) && method.equals(other.method)
		}
		return false;
	}

	def toJSON(){
		def result = [
			name: name,
			host: host,
			port: port,
			performAuth: performAuth,
			auth: [:]]

		if(performAuth){
			result.auth = [
				adminDatabase: authDatabase,
				username: username,
				method: method.id
			]

			if(!hidePassword){
				result.auth["password"] = password
			}else{
				if(!presetToConnectionID.containsKey(this)){
					//having something like this would probably hint to using a factory
					def connectionId = java.util.UUID.randomUUID().toString()
					synchronized(connectionIDToPreset){
						while(connectionIDToPreset.containsKey(connectionId)){
							connectionId = java.util.UUID.randomUUID().toString()
						}
						connectionIDToPreset[connectionId] = this
					}

					presetToConnectionID[this] = connectionId
				}

				result["auth"]["connectionId"] = presetToConnectionID[this]
			}
		}

		return (result as JSON).toString()
	}
}