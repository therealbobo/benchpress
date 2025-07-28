package app

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/therealbobo/benchpress/internal/cmdinfo"
	"github.com/therealbobo/benchpress/internal/ingestion"
	"github.com/therealbobo/benchpress/internal/processing"
	"github.com/therealbobo/benchpress/internal/utils"

	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

type config struct {
	Runs    int        `yaml:"runs"`
	OutDir  string     `yaml:"outdir"`
	PreReqs []*cmdinfo.CmdInfo `yaml:"prerequisites"`
	Loads   []*cmdinfo.CmdInfo `yaml:"loads"`
	Cases   []*cmdinfo.CmdInfo `yaml:"cases"`
}

func createDirFromCmdInfo(outdir string, case_, step *cmdinfo.CmdInfo) {
	dir := filepath.Join(outdir, utils.NormalizeName(case_.Name), utils.NormalizeName(step.Name))
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		log.Fatal().Err(err).Msg("")
	}
}

func waitForNCmds(targetNumWaits int, waitChan chan error) {
	numWaits := 0
	for {
		err := <-waitChan
		if err == nil {
			numWaits++
		} else {
			log.Fatal().Err(err).Msg("Error waiting commands")
		}
		if numWaits == targetNumWaits {
			break
		}
	}
}

func ingest(cmdInfo *cmdinfo.CmdInfo) error {
	if cmdInfo.IngestorConfig != nil {
		configPtr := cmdInfo.IngestorConfig.JsonIngestorConfig
		if configPtr != nil {
			ingestor := ingestion.NewJsonIngestor(*configPtr)
			var docs []string
			if ingestor.Source() == ingestion.StdoutSrc {
				docs = strings.Split(cmdInfo.Stdout, "\n")
			} else if ingestor.Source() == ingestion.StderrSrc {
				docs = strings.Split(cmdInfo.Stderr, "\n")
			}
			doc, err := ingestor.Select(docs)
			if err != nil {
				return err
			}
			out, err := ingestor.Standardize(doc)
			if err != nil {
				return err
			}

			cmdInfo.Data = append(cmdInfo.Data, out)
		}
	}

	return nil
}

func process(caseName string, cmdInfo *cmdinfo.CmdInfo) error {
	configPtr := cmdInfo.ProcessorConfig
	if configPtr != nil {
		processor := processing.NewProcessor(*configPtr)
		data, err := processor.Process(cmdInfo.Data)
		if err != nil {
			return err
		}

		jsonBytes, err := json.Marshal(data)
		if err != nil {
			return err
		}
		fmt.Printf("[%s] %s: %s\n", caseName, cmdInfo.Name, string(jsonBytes))

	}
	cmdInfo.Data = cmdInfo.Data[:0]

	return nil
}

func dumpOutputToFile(outdir string, runId int, _case, cmdInfo *cmdinfo.CmdInfo) error {
	baseOutPath := filepath.Join(outdir, utils.NormalizeName(_case.Name), utils.NormalizeName(cmdInfo.Name))
	stdoutPath := filepath.Join(baseOutPath, fmt.Sprintf("%d_stdout.txt", runId))
	stderrPath := filepath.Join(baseOutPath, fmt.Sprintf("%d_stderr.txt", runId))

	err := os.WriteFile(stdoutPath, []byte(cmdInfo.Stdout), 0755)
	if err != nil {
		return err
	}

	err = os.WriteFile(stderrPath, []byte(cmdInfo.Stderr), 0755)
	if err != nil {
		return err
	}

	return nil
}

func Run(confContent []byte) error {

	var conf config
	err := yaml.Unmarshal(confContent, &conf)
	if err != nil {
		return err
	}

	if _, err := os.Stat(conf.OutDir); os.IsNotExist(err) {
		err = os.MkdirAll(conf.OutDir, 0755)
		if err != nil {
			return err
		}
	}

	timestamp := time.Now().Format("2006-01-02_15-04-05")
	currentOutdir := filepath.Join(conf.OutDir, timestamp)

	for _, case_ := range conf.Cases {
		for _, step := range append(conf.PreReqs, conf.Loads...)   {
			createDirFromCmdInfo(currentOutdir, case_, step)
		}
		createDirFromCmdInfo(currentOutdir, case_, case_)
	}

	waitChan := make(chan error)


	for _, case_ := range conf.Cases {
		for i := 1 ; i < conf.Runs+1 ; i++ {
			log.Info().Int("run", i).Str("case", case_.Name).Msg("")

			var prereqWg sync.WaitGroup
			targetNumWaits := len(conf.PreReqs)
			prereqWg.Add(targetNumWaits)

			for _, prereq := range conf.PreReqs {
				go prereq.Exec(&prereqWg, waitChan)
			}

			waitForNCmds(targetNumWaits, waitChan)

			var caseWg sync.WaitGroup
			caseWg.Add(1)
			go case_.Exec(&caseWg, waitChan)

			waitForNCmds(1, waitChan)

			var loadsWg sync.WaitGroup
			targetNumWaits = len(conf.Loads)
			loadsWg.Add(targetNumWaits)

			for _, load := range conf.Loads {
				go load.Exec(&loadsWg, waitChan)
			}

			waitForNCmds(targetNumWaits, waitChan)

			loadsWg.Wait()

			case_.Signal(syscall.SIGINT)

			caseWg.Wait()

			for _, prereq := range conf.PreReqs {
				prereq.Signal(syscall.SIGINT)
			}
			prereqWg.Wait()

			for _, step := range append(conf.PreReqs, conf.Loads...)   {
				err = dumpOutputToFile(currentOutdir, i, case_, step)
				if err != nil {
					log.Fatal().Err(err).Msg("")
				}
				ingest(step)
			}
			err = dumpOutputToFile(currentOutdir, i, case_, case_)
			if err != nil {
				log.Fatal().Err(err).Msg("")
			}

		}

		for _, step := range append(conf.PreReqs, conf.Loads...)   {
			err := process(case_.Name, step)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
