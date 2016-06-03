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

		def result = jong.runCommand(params.command).map(new RawResultHandler());

		println "===== COMMAND ===="
		println params as JSON
		println params.boolean("multi")
		println result as JSON
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

