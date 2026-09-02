package main

import (
	"log"

	"github.com/PixelCores/Eruun/cmd/server/app"
	"github.com/PixelCores/Eruun/pkg/apiserver/workflow/traits"
)

func main() {
	traits.RegisterAllProcessors()
	cmd := app.NewAPIServerCommand()
	if err := cmd.Execute(); err != nil {
		log.Fatalln(err)
	}
}
