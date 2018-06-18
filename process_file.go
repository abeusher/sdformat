package main

import (
	"bufio"
	"fmt"
	"github.com/abeusher/selectphone_processing/util"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"strings"
	"time"
)

//global variables
var (
	inputFile             string
	outputFile            string
	stepCount             int
	nameAndAddressParts   []string
	debugMode             = false
	expectedNumberOfParts = 413
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
	inputFile = viper.GetString("inputFilename")
	outputFile = viper.GetString("outputFilename")
	stepCount = viper.GetInt("stepCount")
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
	parts := strings.Split(inputLine, ",")
	numberOfParts := len(parts)
	//logrus.Info("Number of parts: ", numberOfParts)
	if numberOfParts != expectedNumberOfParts {
		return outputLine
	}
	//partsSlice := parts[:]
	processNameAddress(parts)
	processGeoHousehold(parts)
	processAgePhoneEducation(parts)
	processBusinessOwnerDataLoaded(parts)
	return outputLine
}

func processFile() {
	//TODO: remove this once the YAML parsing is working
	inputFile = "data/2017_4_USA_Cons_LF_DC_0.csv"
	stepCount = 25000
	inFile, err := os.Open(inputFile)
	startTime := time.Now()
	if err != nil {
		logrus.Fatal(err)
	}
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)
	lineCounter := 0
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
		newLine := processLine(inputLine)
		if newLine == "" {
			//error condion
		}
	}
	fmt.Println(lineCounter, " lines processed")
}

func main() {
	logrus.Debug("Running main() in selectphone_processing")
	processFile()
	logrus.Info("All done.")
}