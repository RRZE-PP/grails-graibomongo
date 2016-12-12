# Graibomongo

Graibomongo is a [robomongo](https://robomongo.com) clone in your browser based
on our [mongobrowser](https://github.com/RRZE-PP/mongobrowser).

It features the core functionality of robomongo including a port of the
non-javascript part of the Node.js mongo shell. Thus in its shell you can do
almost anything you could do in the mongo shell.

# Usage

## Standalone
The plugin can be run as a standalone app offering a sample application.

## Taglib
Simply include the plugin and use its taglib.

```html
<!-- Creates a stationary mongobrowser instance -->
<mongoBrowser:block></mongoBrowser:block>

<!-- Creates a mongobrowser instance in a window, e.g. for when editing
data which is reloaded in the same browser tab using ajax -->
<mongoBrowser:window></mongoBrowser:window>
```

Both variations offer the possibility to pre-fill the connections to which
the mongoBrowser instance can connect (the user can still add additional
connections).

Simply pass an instance of `ConnectionPreset` to the preset attribute or a
list of `ConnectionPreset`s to the presets attribute (or both, they will be
concatenated):

```gsp

<%@ page import="de.rrze.graibomongo.ConnectionPreset" %>
<mongoBrowser:block presets="${[
        new ConnectionPreset("dev-gsp-hiddenPW", "localhost").auth("root", "root").hidePassword(),
        new ConnectionPreset("dev-gsp-withAuth", "localhost").auth("root", "root", "admin", ConnectionPreset.Method.MONGODB_CR),
        new ConnectionPreset("dev-gsp-withoth", "localhost")]}"/>
```

There are issues with the mongodb java driver as grails ships it's own (possibly outdated)
version of it. If you get errors stating that the class `org.mongodb.MongoClient` or similar
include `compile "org.mongodb:mongo-java-driver:3.2.2"` (or newer) in your parent application's
dependencies. Even though the plugin requests this version it is not always served it.

## Connection Presets
### In a GSP

In individual GSPs you can pre-populate the available connections by passing an instance or a list of the following class in the
preset or the presets attribute respectively.

`class de.rrze.mongobrowser.ConnectionPreset`:

* **Constructor**: `ConnectionPreset(String name, String host, int port = 27017)`
	* `name`: The name to give this preset
	* `host`: The host where the mongodb runs (must be reachable from the backend, not the frontend!)
	* `port`: The port of the mongodb
* **Authentication method**: `auth(String username, String password, String database = "admin", Method method = Method.SCRAM_SHA_1)`
	* Call on an instance. Returns the instance for easy usage in a gsp (see above).
	* `username`: the username to authenticate with
	* `password`: the password to authenticate with
	* `database`: the admin database in this mongodb instance (usually `admin`)
	* `method`: the authentication method to use
* **Hiding the password from the user**: `hidePassword()`
	* Call on an instance. Returns the instance for chaining
	* When called, the connection will be cached locally on the server and the password will be raplaced by a string identifying the
	  connection. When connecting the client will send this string and the password will be loaded from the cache.

`enum de.rrze.mongobrowser.ConnectionPreset.Method`:
* SCRAM_SHA_1
* MONGODB_CR

### Globally in the config

Alternatively you can add a preset to all instances of the mongobrowser across all GSPs. To do so, add to your application.yml
a config like this:

```yaml
graibomongo:
    defaultConnections:
        dev-withAuth:
            host: localhost
            port: 27017            #optional, default: 27017
            hidePassword: true     #optional, calls the hidePassword Method (see above)
            auth:                  #optional, when left out, no authentication will be assumed
                username: foobar
                password: blafoo
                database: admin    #optional, default: admin
                method: mongodb-cr #optional, default: scram-sha-1
        dev-without:
            host: localhost
            port: 27017
```

## Configuration
To configure the plugin you can add a config like this to your application.yml:

```yaml
graibomongo:
    # Configuration of the plugin
    clientExpiration: 28800     # number of seconds, a client will be kept after the last interaction with it (default: 8h)
    maxCachedClients: 1000      # maximum number of clients to cache (default: 3000)
    maxCachedCursors: 5000000   # maximum number of cursors to cache (default: maxCachedClients * 500)
    pruneAtPercentage: 80       # filling level of the cache at which expired clients will be removed (default: 80)
    # Configuration of the mongodb driver instances used in the plugin
    socketTimeout: 10000        # this value is passed to Socket.setSoTimeout(int) (default: 60000)
    connectTimeout: 500         # connection timeout in ms; this is for establishing the socket connections (default: 3000)
    serverSelectionTimeout: 750 # how long to wait in ms for server selection to succeed (default: connectTimeout + 1000)
```

### Logging
To enable logging put this into your `conf/logback.groovy`:
```
logger("grails.app.taglib.de.rrze.graibomongo", DEBUG)
logger("grails.app.controllers.de.rrze.graibomongo", DEBUG)
logger("grails.app.services.de.rrze.graibomongo", DEBUG)
```
