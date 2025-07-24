package main

import (
	"flag"
	"os"

	"github.com/therealbobo/benchpress/internal/app"

	"github.com/rs/zerolog/log"
)

func main() {

	filename := flag.String("config", "", "The configuration for the benchmark.")
	flag.Parse()

	if filename == nil {
		log.Fatal().Msg("please provide a config")
	}

	if _, err := os.Stat(*filename); os.IsNotExist(err) {
		log.Fatal().Err(err).Msg("please provide an existing config")
	}


	f, err := os.ReadFile(*filename)
	if err != nil {
		log.Fatal().Err(err).Msg("please provide a readable config")
	}

	err = app.Run(f)
	if err != nil {
		log.Fatal().Err(err)
	}
}
