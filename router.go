package main

import (
	_ "docs"
	"github.com/gorilla/mux"
	httpSwagger "github.com/swaggo/http-swagger"
)

// configRouter sets up http server router.
func configRouter() *mux.Router {
	router := mux.NewRouter()

	// Serve the Swagger UI with a dynamic URL for swagger.json

	router.PathPrefix("/swagger/").Handler(httpSwagger.Handler(
		httpSwagger.URL("https://app1276.dev.dittofi.link/swagger.json"),
	))

	return router
}
