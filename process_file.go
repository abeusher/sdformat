package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"compress/gzip"

	util "github.com/abeusher/dataprocessing"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	//"path/filepath"
)

//global variables
var (
	configurationFilePath = `E:\goworkspace\src\github.com\abeusher\sdformat\`
	inputFile             string
	outputFile            string
	stepCount             int
	nameAndAddressParts   []string
	debugMode             = false
	expectedNumberOfParts = 49
	badAddress            = make(map[string]string)
)

func init() {
	//global configuration of variables
	viper.SetConfigName("config")
	fmt.Println(configurationFilePath)
	viper.AddConfigPath(configurationFilePath)
	err := viper.ReadInConfig()
	util.Check(err)
	//TODO: this doesn't work.  Fix parsing of these values
	inputFile = viper.GetString("inputFile")
	outputFile = viper.GetString("outputFile")
	stepCount = viper.GetInt("stepCount")
	logrus.Info("Input file:", inputFile)
	logrus.Info("Output file:", outputFile)
	logrus.Info("stepCount:", stepCount)
	logrus.SetLevel(logrus.InfoLevel)
	if debugMode {
		logrus.SetLevel(logrus.DebugLevel)
	}
}

//processLine takes a line of text and processes it into SD format
func processLine(inputLine string) util.SdFormat {
	//logrus.Debug(inputLine)
	inputLineUpperCase := strings.ToUpper(inputLine)
	parts := strings.Split(inputLineUpperCase, "\t")
	numberOfParts := len(parts)
	//fmt.Println("numberOfParts", numberOfParts)
	//logrus.Info("Number of parts: ", numberOfParts)
	if numberOfParts != expectedNumberOfParts {
		sd := util.SdFormat{}
		return sd
	}
	//TODO: is there an advantage to using util.SdFormat via a pointer?
	record := util.SdFormat{}
	record.PopulateRecord(parts)
	record.ComputeGeohash()
	//return inputLine
	return record
}

func processFile() {
	defer util.TimeTrack(time.Now(), "processFile()")
	//simple non compressed TSV file version
	inFile, err := os.Open(inputFile)
	defer inFile.Close()
	util.Check(err)

	scanner := bufio.NewScanner(inFile)

	//version for reading a compressed gzip file
	if strings.HasSuffix(inputFile, ".gz") {
		gr, err := gzip.NewReader(inFile)
		util.Check(err)
		defer gr.Close()
		scanner = bufio.NewScanner(gr)
	}

	outFile, err := os.Create(outputFile)
	util.Check(err)

	logrus.Info("Processing inputFile:", inputFile)
	startTime := time.Now()

	scanner.Split(bufio.ScanLines)
	lineCounter := 0
	stepCount = 200000
	for scanner.Scan() {
		lineCounter++
		if lineCounter%stepCount == 0 {
			recordsPerSecond, secondsRemaining := util.CalculateTimeRemaining(startTime, lineCounter, 229848)
			logrus.Info("lines processed: ", lineCounter)
			logrus.Info("recordsPerSecond: ", recordsPerSecond)
			logrus.Info("secondsRemaining: ", secondsRemaining)
			//logrus.Info(msg)
		}
		inputLine := scanner.Text()
		record := processLine(inputLine)
		outFile.WriteString(record.ToString() + "\n")
		if record.Zipcode == "2" {

		}
	}
	fmt.Println(lineCounter, " lines processed")
}

func main() {
	logrus.Debug("Running main() in process_file.go")
	processFile()
	logrus.Info("All done.")
}
