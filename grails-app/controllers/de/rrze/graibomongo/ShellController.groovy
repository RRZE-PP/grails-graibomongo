package de.rrze.graibomongo

import com.mongodb.MongoClient
import com.mongodb.client.MongoDatabase
import org.jongo.Jongo
import org.jongo.RawResultHandler
import org.jongo.query.BsonQueryFactory
import org.jongo.marshall.jackson.JacksonEngine
import org.jongo.marshall.jackson.configuration.Mapping

import grails.converters.JSON

class ShellController {

	static cursors = [:];

    def index(){
    	render "some text"
    }

	def runCommand(){

		println params as JSON
		MongoClient mc = new MongoClient()
    	Jongo jong = new Jongo(mc.getDB("test"))
    	//TODO: Javascript Integer gehen nur bis 2^53-1 => eigentlich muessen wir ueberall BSON Longs verwenden

    	//This is a hellish hack to allow getlasterror.
    	//it would be way better to implement the command-version of update, etc!
    	if(params?.command?.find("getlasterror") != null && session?.lastError != null){
			println "===== COMMAND: lasterror ===="
    		def lastError = session["lastError"]
    		session["lastError"] = null
			println lastError.toString()
			println "====="
    		render([
    			n: lastError.getN(),
    			syncMillis: 0,
    			ok: 1.0,
    		] as JSON)
    	}

		def result = jong.runCommand(params.command).map(new RawResultHandler());

		println result as JSON
		render result as JSON
	}

	def update(){
		MongoClient mc = new MongoClient()
    	Jongo jong = new Jongo(mc.getDB("test"))

		def query = new BsonQueryFactory(new JacksonEngine(Mapping.defaultMapping())).createQuery('{"meh":4}').toDBObject()
		def obj = new BsonQueryFactory(new JacksonEngine(Mapping.defaultMapping())).createQuery('{"meh": 5}').toDBObject()
		mc.getDatabase("test").getCollection("foobar").updateOne(query, obj);

		def update = jong.getCollection("foobar").update(params.query)
		if(params.boolean("upsert"))
			update = update.upsert();
		if(params.boolean("multi"))
			update = update.multi();
		def result = update.with(params.obj);

		session["lastError"] = result;

		println "===== UPDATE ===="
		println params as JSON
		println params.boolean("multi")
		println result as JSON
		println session["lastError"]
		println "====="

		render ""
	}

	def initCursor(){
		def query = new BsonQueryFactory(new JacksonEngine(Mapping.defaultMapping())).createQuery(params.query).toDBObject()

		MongoClient mc = new MongoClient()
		def iterable = mc.getDatabase("test").getCollection("foobar").find(query)//TODO: Handle other options
		def cursor = iterable.iterator()

		def data = []
		for(int i=0; i<params.int('nToReturn'); i++){
			def item = cursor.tryNext()
			if(item != null){
				data.push(item)
			}else{
				break
			}
		}

		//TODO: Handle all iterated
		def scursor = cursor.getServerCursor()
		if(scursor != null)
			cursors[scursor.getId()] = cursor

		render([nReturned: data.size(),
				data: data,
				resultFlags: 0,
				cursorId: scursor.getId()] as JSON)
	}
}

