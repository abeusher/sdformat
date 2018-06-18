package util

import (
	//"fmt"
	"time"
)

/*CalculateTimeRemaining takes in a startTime and a numberOfRecordsProcessed, and
totalRecordsToProcess and provides and estimated time to completion. */
func CalculateTimeRemaining(startTime time.Time, numberOfRecordsProcessed int, totalRecordsToProcess int) (recordsPerSecond float64, secondsRemaining float64) {
	elapsed := time.Since(startTime)
	//fmt.Println("Number of seconds: ", elapsed.Seconds())
	recordsPerSecond = float64(numberOfRecordsProcessed) / float64(elapsed.Seconds())
	//fmt.Println("Records per second:", recordsPerSecond)
	secondsRemaining = float64(totalRecordsToProcess) / recordsPerSecond
	return recordsPerSecond, secondsRemaining
}
