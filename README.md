# Graibomongo

Graibomongo is a [robomongo](https://robomongo.com) clone in your browser based
on our [mongobrowser](https://github.com/RRZE-PP/mongobrowser).

It features the core functionality of robomongo including a port of the
non-javascript part of the Node.js mongo shell. Thus in its shell you can do
almost anything you could do in the mongo shell.

# Usage
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
<mongoBrowser:window
     preset="${new ConnectionPreset("name", "host").auth("user", "password", "admin", ConnectionPreset.Method.MONGODB_CR)}"
     presets="${[new ConnectionPreset("name2", "host2"),
                 new ConnectionPreset("name3", "host3")]}" />
```

## Connection Presets

`class de.rrze.mongobrowser.ConnectionPreset`:

* **Constructor**: `ConnectionPreset(String name, String host, int port = 27017)`
	* `name`: The name to give this preset
	* `host`: The host where the mongodb runs (must be reachable from the backend, not the frontend!)
	* `port`: The port of the mongodb
* **Authentication method**: `auth(String username, String password, String database = "admin", Method method = Method.SCRAM_SHA_1){`
	* Call on an instance. Returns the instance for easy usage in a gsp (see above).
	* `username`: the username to authenticate with
	* `password`: the password to authenticate with
	* `database`: the admin database in this mongodb instance (usually `admin`)
	* `method`: the authentication method to use

`enum de.rrze.mongobrowser.ConnectionPreset.Method`:
* SCRAM_SHA_1
* MONGODB_CR