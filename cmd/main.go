package main

import (
	"log"

	"github.com/PixelCores/Eruun/cmd/server/app"
)

func main() {
	cmd := app.NewAPIServerCommand()
	if err := cmd.Execute(); err != nil {
		log.Fatalln(err)
	}
}
