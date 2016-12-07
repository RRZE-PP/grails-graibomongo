package de.rrze.graibomongo

import com.mongodb.MongoCredential

class ResultFlagType {
    /* returned, with zero results, when getMore is called but the cursor id
       is not valid at the server. */
    static ResultFlag_CursorNotFound = 1

    /* { $err : ... } is being returned */
    static ResultFlag_ErrSet = 2

    /* Have to update config from the server, usually $err is also set */
    static ResultFlag_ShardConfigStale = 4

    /* for backward compatibility: this let's us know the server supports
       the QueryOption_AwaitData option. if it doesn't, a repl slave client should sleep
    a little between getMore's.
    */
    static ResultFlag_AwaitCapable = 8
}

class AuthData implements grails.validation.Validateable {
	String user
	String password
	String connectionId

	String authDatabase
	String authMechanism

	def beforeValidate() {
		authDatabase = authDatabase ?: "admin"
		authMechanism = authMechanism ?: "scram-sha-1"
	}

	static constraints = {
		user nullable: false, blank: false
		password nullable: true, validator: { val, obj -> obj.connectionId != null ? true : (val != null && val != "")}
		connectionId nullable: true, blank: false
		authDatabase nullable: false, blank: false
		authMechanism nullable: false, inList: ["scram-sha-1", "mongodb-cr"]
	}

}

class ConnectionData implements grails.validation.Validateable {
	String hostname
	Integer port

	Boolean performAuth
	AuthData auth

	List<MongoCredential> createAuthList(){
		if(performAuth != true)
			return new ArrayList<MongoCredential>()

		if(auth.connectionId != null){
			auth.password = ConnectionPreset.getPasswordFromCache(auth.connectionId)
		}

		ArrayList<MongoCredential> credentials = new ArrayList<MongoCredential>()
		//TODO: check for encoding issues in password with toCharArray!
		switch(auth.authMechanism){
			case "scram-sha-1":
				credentials.push(MongoCredential.createScramSha1Credential(auth.user, auth.authDatabase, auth.password?.toCharArray()))
				break
			case "mongodb-cr":
				credentials.push(MongoCredential.createMongoCRCredential(auth.user, auth.authDatabase, auth.password?.toCharArray()))
				break
			default:
				throw new IllegalArgumentException("Unknown auth mechanism")
		}

		return credentials
	}

	def beforeValidate() {
		port = port ?: 27017
	}

	String toString(){
		return auth?.user + ":" + auth?.password + "@" + hostname + ":" + port
	}

	static constraints = {
		hostname nullable: false, blank: false
		port nullable: false, blank: false
		performAuth nullable: false, blank: false
		auth nullable: false, blank: false, validator: {val, obj -> obj.performAuth ? val.validate() : true }
	}
}

class CommandRequest implements grails.validation.Validateable {
	ConnectionData connection

	String database
	String command

	String toString(){
		return "DB[" + database + "].runCommand(" + command + ")"
	}

	static constraints = {
		connection nullable: false, blank: false, validator: {val, obj -> val.validate()}
		database nullable: false, blank: false
		command nullable: false, blank: false
	}
}

class CursorInitRequest implements grails.validation.Validateable {
	ConnectionData connection

	String query
	String ns

	Long nToReturn
	Integer nToSkip
	String fieldsToReturn

	def beforeValidate(){
		fieldsToReturn = fieldsToReturn ?: "{}"
	}

	String toString(){
		return ns + ".find(" + query + ", " + fieldsToReturn +").get(" + nToReturn + ")"
	}

	static constraints = {
		connection nullable: false, blank: false, validator: {val, obj -> val.validate()}
		query nullable: false, blank: false
		ns nullable: false, blank: false
		nToReturn nullable: false, blank: false
		fieldsToReturn nullable: false, blank: false
	}
}

class RequestMoreRequest implements grails.validation.Validateable {
	ConnectionData connection

	Map cursorId; //This will be converted to a Long in the getter! Ugly, but I see no other way :(
	Long cursorId_
	Long nToReturn

	def getCursorId(){
		if(this.cursorId_ == null){
			if(this.cursorId && this.cursorId instanceof String && this.cursorId.startsWith("NumberLong(\"") && this.cursorId.endsWith("\")")){
				this.cursorId_ = Long.valueOf(this.cursorId[12 .. -3]); //keeping this here, but should never be invoced
			}else if(this.cursorId && this.cursorId instanceof Map &&  '$numberLong' in this.cursorId){
				this.cursorId_ = Long.valueOf(this.cursorId['$numberLong'])
			}
		}

		return this.cursorId_
	}

	String toString(){
		return "Cursor(" + getCursorId() + ").get(" + nToReturn + ")"
	}

	static constraints = {
		connection nullable: false, blank: false, validator: {val, obj -> val.validate()}
		cursorId nullable: false, blank: false
		nToReturn nullable: false, blank: false
	}
}