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

//SdFormat is the struct for handing input data
type SdFormat struct {
	uniqueID                   string
	title                      string
	firstName                  string
	initial                    string
	lastName                   string
	address1                   string
	address2                   string
	city                       string
	state                      string
	zipcode                    string
	zipcode4                   string
	countyName                 string
	geoLevel                   string
	latitude                   string
	longitude                  string
	geohash9                   string
	geohash5                   string
	msa                        string
	cbsa                       string
	fipsState                  string
	fipsCounty                 string
	censusTract                string
	censusBlockGroup           string
	censusBlock                string
	fullCensusBlockID          string
	firstInHousehold           string
	childPresent               string
	age                        string
	homePhone                  string
	estimatedIncome            string
	lengthOfResidence          string
	dwellingType               string
	homeownerType              string
	gender                     string
	maritalStatus              string
	estimatedSealth            string
	estimatedHomeValue         string
	cellphone                  string
	email1                     string
	email2                     string
	education                  string
	businessOwnerStatus        string
	conservativePoliticalDonor string
	liberalPoliticalDonor      string
	veteransDonor              string
	doNotCallList              string
	timezone                   string
	birthYear                  string
	dateUpdated                string
}

//Populates an SdFormat struct using an array of length 49 with all string values as inputs
func (sd *SdFormat) PopulateRecord(stringArray []string) {
	sd.uniqueID = stringArray[0]
	sd.title = stringArray[1]
	sd.firstName = stringArray[2]
	sd.initial = stringArray[3]
	sd.lastName = stringArray[4]
	sd.address1 = stringArray[5]
	sd.address2 = stringArray[6]
	sd.city = stringArray[7]
	sd.state = stringArray[8]
	sd.zipcode = stringArray[9]
	sd.zipcode4 = stringArray[10]
	sd.countyName = stringArray[11]
	sd.geoLevel = stringArray[12]
	sd.latitude = stringArray[13]
	sd.longitude = stringArray[14]
	sd.geohash9 = stringArray[15]
	sd.geohash5 = stringArray[16]
	sd.msa = stringArray[17]
	sd.cbsa = stringArray[18]
	sd.fipsState = stringArray[19]
	sd.fipsCounty = stringArray[20]
	sd.censusTract = stringArray[21]
	sd.censusBlockGroup = stringArray[22]
	sd.censusBlock = stringArray[23]
	sd.fullCensusBlockID = stringArray[24]
	sd.firstInHousehold = stringArray[25]
	sd.childPresent = stringArray[26]
	sd.age = stringArray[27]
	sd.homePhone = stringArray[28]
	sd.estimatedIncome = stringArray[29]
	sd.lengthOfResidence = stringArray[30]
	sd.dwellingType = stringArray[31]
	sd.homeownerType = stringArray[32]
	sd.gender = stringArray[33]
	sd.maritalStatus = stringArray[34]
	sd.estimatedSealth = stringArray[35]
	sd.estimatedHomeValue = stringArray[36]
	sd.cellphone = stringArray[37]
	sd.email1 = stringArray[38]
	sd.email2 = stringArray[39]
	sd.education = stringArray[40]
	sd.businessOwnerStatus = stringArray[41]
	sd.conservativePoliticalDonor = stringArray[42]
	sd.liberalPoliticalDonor = stringArray[43]
	sd.veteransDonor = stringArray[44]
	sd.doNotCallList = stringArray[45]
	sd.timezone = stringArray[46]
	sd.birthYear = stringArray[47]
	sd.dateUpdated = stringArray[48]

}

//global variables
var (
	inputFile             string
	outputFile            string
	stepCount             int
	nameAndAddressParts   []string
	debugMode             = false
	expectedNumberOfParts = 49
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
	fmt.Println("numberOfParts", numberOfParts)
	//logrus.Info("Number of parts: ", numberOfParts)
	if numberOfParts != expectedNumberOfParts {
		return outputLine
	}
	//singleRecords := &SdFormat{}
	//singleRecords.PopulateRecord(parts)
	//partsSlice := parts[:]
	/*
		processNameAddress(parts)
		processGeoHousehold(parts)
		processAgePhoneEducation(parts)
		processBusinessOwnerDataLoaded(parts)
	*/
	return outputLine
}

func processFile() {
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
	stepCount = 10000
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
			fmt.Println(outputFile)
		}
	}
	fmt.Println(lineCounter, " lines processed")
}

func main() {
	defer util.TimeTrack(time.Now(), "main()")
	logrus.Debug("Running main() in process_file.go")
	processFile()
	logrus.Info("All done.")
}
