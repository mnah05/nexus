package web

import "embed"

// Files contains the embedded web UI assets (index.html, app.css, app.js).
//
//go:embed *
var Files embed.FS
