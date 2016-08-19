<asset:javascript src="mongobrowser.js" />
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
