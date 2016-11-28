package de.rrze.graibomongo

class CmeditorTagLib {

	static namespace = "mongoBrowser"

	def block = { attrs, body ->
		def defaultPresets = []
		def defaultConnections = grails.util.Holders.grailsApplication?.config?.graibomongo?.defaultConnections
		if(defaultConnections){
			for(name in defaultConnections.keySet()){
				def conn = defaultConnections[name]
				if(!(conn.host && (!conn.auth || (conn.auth.username && conn.auth.password)))){
					//not all necesarry data was given
					continue
				}

				def preset = conn.port ? new ConnectionPreset(name, conn.host, conn.port) : new ConnectionPreset(name, conn.host)
				if(conn.auth){
					preset.auth(conn.auth.username, conn.auth.port)
				}
				defaultPresets.put(preset)
			}
		}
		out << render(template: "mongoBrowser", model: [windowMode: "resizable", presets: ([attrs.preset] + attrs.presets + defaultPresets)])
	}

	def window = { attrs, body ->
		def defaultPresets = []
		def defaultConnections = grails.util.Holders.grailsApplication?.config?.graibomongo?.defaultConnections
		if(defaultConnections){
			for(name in defaultConnections.keySet()){
				def conn = defaultConnections[name]
				if(!(conn.host && (!conn.auth || (conn.auth.username && conn.auth.password)))){
					println("Not all necesarry data was given for default preset: " + name)
					continue
				}

				def preset = conn.port ? new ConnectionPreset(name, conn.host, conn.port) : new ConnectionPreset(name, conn.host)
				if(conn.auth){
					preset.auth(conn.auth.username, conn.auth.password)
				}
				defaultPresets.add(preset)
			}
		}
		out << render(template: "mongoBrowser", model: [windowMode: "moveable", presets: ([attrs.preset] + attrs.presets + defaultPresets)])
	}
}
