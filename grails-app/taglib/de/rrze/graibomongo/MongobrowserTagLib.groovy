package de.rrze.graibomongo

class CmeditorTagLib {

	static namespace = "mongoBrowser"

	def loadDefaultPresets(){
		def defaultPresets = []
		def defaultConnections = grails.util.Holders.grailsApplication?.config?.graibomongo?.defaultConnections
		if(defaultConnections){
			for(name in defaultConnections.keySet()){
				def conn = defaultConnections[name]
				if(!(conn.host && (!conn.auth || (conn.auth.username && conn.auth.password)))){
					log.warn("Not all necesarry data was given for default preset: " + name + "(host missing or incomplete auth)")
					continue
				}

				def preset = conn.port ? new ConnectionPreset(name, conn.host, conn.port) : new ConnectionPreset(name, conn.host)
				if(conn.auth){
					def auth = conn.auth
					if(auth.database &&  auth.method && auth.method in ["scram-sha-1", "mongodb-cr"]){
						preset.auth(auth.username,
						            auth.password,
						            auth.database,
						            "scram-sha-1".equals(auth.method) ? ConnectionPreset.Method.SCRAM_SHA_1 : ConnectionPreset.Method.MONGODB_CR)
					}else if(auth.database){
						preset.auth(auth.username, auth.password, auth.database)
					}else if(auth.method && auth.method in ["scram-sha-1", "mongodb-cr"]){
						preset.auth(auth.username,
						            auth.password,
						            "admin",
						            "scram-sha-1".equals(auth.method) ? ConnectionPreset.Method.SCRAM_SHA_1 : ConnectionPreset.Method.MONGODB_CR)
					}else{
						preset.auth(auth.username, auth.password)
					}

					if(conn.hidePassword)
						preset.hidePassword()
				}

				defaultPresets.add(preset)
			}
		}
		return defaultPresets
	}

	def block = { attrs, body ->
		def presets = (attrs.presets ?: []) + (attrs.preset ? [attrs.preset] : []) + loadDefaultPresets()
		out << render(template: "/mongoBrowser", model: [windowMode: "resizable", presets: presets])
	}

	def window = { attrs, body ->
		def presets = (attrs.presets ?: []) + (attrs.preset ? [attrs.preset] : []) + loadDefaultPresets()
		out << render(template: "/mongoBrowser", model: [windowMode: "moveable", presets: presets])
	}
}
