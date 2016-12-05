package de.rrze.graibomongo

class ConnectionPreset {
	String name;
	String host;
	int port;

	boolean performAuth;
	String username;
	String password;
	String authDatabase;
	Method method;

	public enum Method {
		SCRAM_SHA_1("scram-sha-1"),
		MONGODB_CR("mongodb-cr")
	    String id

	    Method(String id) {
	        this.id = id
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

	def toJSON(){
		def result = """{name: "${name}",
                       host: "${host}",
                       port: 27017,
                       performAuth: ${performAuth}"""
        if(performAuth){
        	result += """,
        				auth: {
	                         adminDatabase: "${authDatabase}",
	                         username: "${username}",
	                         password: "${password}",
	                         method: "${method.id}"
                         }"""
        }else{
        	result += ", auth:{}"
        }

        result += "}"

        return result;
	}
}