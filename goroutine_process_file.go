package main

import (
	"bufio"
	"fmt"
	util "github.com/abeusher/dataprocessing"
	"github.com/sirupsen/logrus"
	//"github.com/spf13/viper"
	"os"
	"strings"
	"sync"
	"time"
)

//global variables
var (
	inputFile             string
	outputFile            string
	stepCount             = 50000
	nameAndAddressParts   []string
	debugMode             = false
	expectedNumberOfParts = 49
	//concurrency           = 100
)

func init() {
	//global configuration of variables
	/*
		viper.SetConfigName("config")
		viper.AddConfigPath(".")
		configFile, err := os.Open("/Users/abeusher/code/sdformat/config.yml")
		viper.ReadConfig(configFile)
		if err != nil {
			logrus.Fatal("Failed to read config file")
			logrus.Fatal(err)
		}
		//TODO: this doesn't work.  Fix parsing of these values
		//inputFile = viper.GetString("inputFilename")
		//outputFile = viper.GetString("outputFilename")
		//stepCount = viper.GetInt("stepCount")
		logrus.Info("Input file:", inputFile)
		logrus.Info("Output file:", outputFile)
		logrus.Info("stepCount:", stepCount)
		defer configFile.Close()
		logrus.SetLevel(logrus.InfoLevel)
		if debugMode {
			logrus.SetLevel(logrus.DebugLevel)
		}
	*/
}

//processLine takes a line of text and processes it into SD format
func processLine(inputLine string) (outputLine string) {
	outputLine = ""
	//logrus.Debug(inputLine)
	parts := strings.Split(inputLine, "\t")
	numberOfParts := len(parts)
	//fmt.Println("numberOfParts", numberOfParts)
	//logrus.Info("Number of parts: ", numberOfParts)
	if numberOfParts != expectedNumberOfParts {
		return outputLine
	}
	singleRecord := &util.SdFormat{}
	singleRecord.PopulateRecord(parts)
	singleRecord.ComputeGeohash()

	return outputLine
}

func processFile(inputFilename string, linesFromFileChannel chan string) {
	defer util.TimeTrack(time.Now(), "processFile()")
	inFile, err := os.Open(inputFilename)
	util.Check(err)
	logrus.Info("Processing inputFile:", inputFilename)

	startTime := time.Now()
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	lineCounter := 0
	stepCount = 100000
	for scanner.Scan() {
		//TODO: change this counter to an 'atomic' counter
		lineCounter++
		if lineCounter%stepCount == 0 {
			recordsPerSecond, secondsRemaining := util.CalculateTimeRemaining(startTime, lineCounter, 229848)
			logrus.Info("lines processed: ", lineCounter)
			logrus.Info("recordsPerSecond: ", recordsPerSecond)
			logrus.Info("secondsRemaining: ", secondsRemaining)
			//logrus.Info(msg)
		}
		inputLine := scanner.Text()
		linesFromFileChannel <- inputLine
	}
	close(linesFromFileChannel)
	logrus.Info("Done loading content via function processFile()")
}

/*

func lookupRoutine(source <-chan string, wg *sync.Waitgroup, results chan dnsLookup) {
    defer wg.Done()
    for name := range source {
        results <- lookup(name)
    }
}

*/

func doWork(linesFromFileChannel <-chan string, wg *sync.WaitGroup, results chan util.SdFormat) {
	defer wg.Done()

	for inputLine := range linesFromFileChannel {
		inputLineUpper := strings.ToUpper(inputLine)
		parts := strings.Split(inputLineUpper, "\t")
		numberOfParts := len(parts)
		if numberOfParts != expectedNumberOfParts {
			//TODO: add better error handling
			singleRecord := util.SdFormat{}
			results <- singleRecord
			continue
		}
		singleRecord := util.SdFormat{}
		singleRecord.PopulateRecord(parts)
		singleRecord.ComputeGeohash()
		results <- singleRecord
		//TODO: make this an atomic counter
		//lineCount++
		//TODO: return the enhanced line! not the inputLine :)
	}
}

func main() {
	logrus.Debug("Running main() in process_file.go")

	linesFromFileChannel := make(chan string, 1000)
	resultsChannel := make(chan util.SdFormat, 1000)
	//linesFromFileChannel := make(chan string)
	//resultsChannel := make(chan util.SdFormat)

	inputFilename := "e:/data/sample_people2018.tsv"
	outputFilename := "e:/data/output_people2018.tsv"

	processFile(inputFilename, linesFromFileChannel)

	concurrency := 1000
	wg := new(sync.WaitGroup)
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		// parallel routine for lookups
		//wg.Add(1)
		go doWork(linesFromFileChannel, wg, resultsChannel)
	}

	// close the results when all lookup routines complete:
	go func() {
		close(resultsChannel)
		wg.Wait()
	}() //protip: always add an empty function call after a closure body in golang
	// see https://stackoverflow.com/questions/16008604/why-add-after-closure-body-in-golang

	for values := range resultsChannel {
		fmt.Println(values)
	}

	logrus.Info("Results written to:", outputFilename)
	logrus.Info("All done.")
	fmt.Println()
}
