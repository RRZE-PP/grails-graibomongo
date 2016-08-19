<asset:javascript src="mongobrowser.js" />
<asset:javascript src="mongoBrowser-src/mongo-shell/mongo-shell.unminified.js" />
<asset:stylesheet src="mongobrowser.css" />
<script>
    var m = MongoBrowser($("body")[0],
        {assetPrefix: "assets/mongoBrowser-src/",
        connectionPresets: [
            <g:each in="${presets}" var="p">
                ${raw(p.toJSON())},
            </g:each>],
        window:"${windowMode}"})
</script>
