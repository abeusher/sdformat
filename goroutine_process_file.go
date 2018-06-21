package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	util "github.com/abeusher/dataprocessing"
	"github.com/mmcloughlin/geohash"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

/*

This is fun :)

TODO:
Create method to make a 'new' SdFormat struct
Create method to populate SdFormat struct
Create method to compute geohash9 with given SdFormat object

*/

/*
headers = 'sd_unique_id,title,first_name,initial,last_name,
address1,address2,city,state,zipcode,zipcode_4,county_name,geo_level,
latitude,longitude,geohash8,geohash5,msa,cbsa,fips_state,fips_county,census_tract,
census_block_group,census_block,full_census_block_id,first_in_household,
child_present,age,home_phone,estimated_income,length_of_residence,dwelling_type,
homeowner_type,gender,marital_status,estimated_wealth,estimated_home_value,
cellphone,email1,email2,education,business_owner_status,conservative_political_donor,
liberal_political_donor,veterans_donor,do_not_call_list,timezone,birth_year,date_updated'.upper().replace(',','\t')
*/

//global variables
var (
	inputFile             string
	outputFile            string
	stepCount             = 50000
	nameAndAddressParts   []string
	debugMode             = false
	expectedNumberOfParts = 49
	concurrency           = 100
)

func init() {
	//global configuration of variables
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
	singleRecords := &util.SdFormat{}
	singleRecords.PopulateRecord(parts)
	return outputLine
}

func processFile() {
	defer util.TimeTrack(time.Now(), "processFile()")
	inputFile := "/Users/abeusher/Desktop/sdformat/sample_people2018.tsv"
	outputFile := "/Users/abeusher/Desktop/sdformat/output_people2018.tsv"
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
	stepCount = 5000
	//
	//
	//
	//
	//a work  channel
	workQueue := make(chan string)
	// We need to know when everyone is done so we can exit.
	complete := make(chan bool)
	//
	//
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
		//inputLine := scanner.Text()
		workQueue <- scanner.Text()
		/*
			inputLine = strings.ToUpper(inputLine)
			newLine := processLine(inputLine)
			if false {
				// false can never equal true.  I'm just using this to hold variables for future use.
				fmt.Println(outputFile)
				fmt.Println(newLine)

			}
		*/
	}
	for i := 0; i < concurrency; i++ {
		go doWork(workQueue, complete)
	}

	// Wait for everyone to finish.
	for i := 0; i < concurrency; i++ {
		<-complete
	}

	fmt.Println(lineCounter, " lines processed")
}

func doWork(queue chan string, complete chan bool) {
	for line := range queue {
		// Do the work with the line.
		result := strings.Split(line, ",")
		//uniqueID := result[0]
		latitude := result[13]
		longitude := result[14]
		lat, err := strconv.ParseFloat(latitude, 64)
		if err != nil {
			//skip this for now
			//logrus.Fatal(err)
		}
		lng, err := strconv.ParseFloat(longitude, 64)
		if err != nil {
			// skip this for now
			//logrus.Fatal(err)
		}
		geo8 := geohash.Encode(lat, lng)
		if geo8 == "" {
			continue
		}
		//TODO: make this an atomic counter
		//lineCount++
	}

	// Let the main process know we're done.
	complete <- true

}

func main() {
	logrus.Debug("Running main() in process_file.go")
	processFile()
	logrus.Info("All done.")
}
