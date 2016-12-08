# Information about namespaces

It's good practice to not clutter the global namespace (aka window).
Mongo is NodeJs and doesn't care about this.
Therefore before deploying to a website, put everything into one large namespace.
To do so, execute `./namespaceify.sh namespaced.js` (or type `make`)
