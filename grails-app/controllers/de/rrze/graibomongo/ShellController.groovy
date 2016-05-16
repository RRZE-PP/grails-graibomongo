package de.rrze.graibomongo

import com.mongodb.MongoClient
import com.mongodb.client.MongoDatabase
import org.jongo.Jongo
import org.jongo.RawResultHandler

import grails.converters.JSON

class ShellController {;

    def index(){
    	render "some text"
    }

	def runCommand(){

		println params as JSON
		MongoClient mc = new MongoClient()
    	Jongo jong = new Jongo(mc.getDB("test"))

		def result = jong.runCommand(params.command).map(new RawResultHandler());

		println result as JSON
		render result as JSON
	}
}

