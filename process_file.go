package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	util "github.com/abeusher/dataprocessing"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	//"path/filepath"
)

/*

This is fun :)

TODO:
Create method to make a 'new' SdFormat struct


*/

//global variables
var (
	inputFile             string
	outputFile            string
	stepCount             = 100000
	nameAndAddressParts   []string
	debugMode             = false
	expectedNumberOfParts = 49
)

func init() {
	//global configuration of variables
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	configFile, err := os.Open("config.yml")
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
}

func processNameAddress(parts []string) {
	var nameAddressItems []string
	logrus.Debug("processNameAddress()")
	baseInt := 1000000000
	randomInt := rand.Intn(500000)
	totalInt := baseInt + randomInt
	/*
		uniqueID := string(totalInt)
		firstName := parts[2]
		middleName := parts[3]
		lastName := parts[4]
		namePrefix := parts[6]
		address1 := parts[14]
		address2 := parts[15]
		apartment := parts[15]

		var data []string
		data = append(data, []string{firstName, lastName, namePrefix}...)
	*/
	if false {
		fmt.Println(nameAddressItems)
		fmt.Println(totalInt)
	}

}

func processGeoHousehold(parts []string) {
	//TODO
}

func processAgePhoneEducation(parts []string) {
	//TODO
}

func processBusinessOwnerDataLoaded(parts []string) {
	//TODO
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

func processFile() {
	defer util.TimeTrack(time.Now(), "processFile()")
	inputFile := "e:/data/sample_people2018.tsv"
	outputFile := "e:/data/output_people2018.tsv"
	inFile, err := os.Open(inputFile)
	logrus.Info("Processing inputFile:", inputFile)
	startTime := time.Now()
	if err != nil {
		logrus.Fatal(err)
	}
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
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
		inputLine = strings.ToUpper(inputLine)
		//fmt.Println(lineCounter)
		//fmt.Println("_____________________")
		newLine := processLine(inputLine)
		if false {
			// false can never equal true.  I'm just using this to hold variables for future use.
			fmt.Println(outputFile)
			fmt.Println(newLine)

		}
	}
	fmt.Println(lineCounter, " lines processed")
}

func main() {
	logrus.Debug("Running main() in process_file.go")
	processFile()
	logrus.Info("All done.")
}
