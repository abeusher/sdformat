import (
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)


/*
See code and its explanation here:
https://codereview.stackexchange.com/questions/126765/reading-and-processing-a-big-csv-file

*/

func lookup(domain_name string) (string, error) {
	ip, err := net.LookupIP(domain_name)
	if err != nil {
		return "", err
	}
	var ip_addresses []string
	for i := range ip {
		address := ip[i]
		ip_addresses = append(ip_addresses, address.String())
	}
	row := domain_name + ",[" + strings.Join(ip_addresses, ":") + "]," + time.Now().String()
	fmt.Println(row)
	return row, nil
}

type dnsLookup struct {
    domain string
    ips    []string
    err    error
}

func lookupRoutine(source <-chan string, wg *sync.Waitgroup, results chan dnsLookup) {
    defer wg.Done()
    for name := range source {
        results <- lookup(name)
    }
}

func parseCSVData () {
	csvfile, err := os.Open("1-million-rows.csv")

	if err != nil {
		fmt.Println(err)
		return
	}

	defer csvfile.Close()
	reader := csv.NewReader(csvfile)
	reader.FieldsPerRecord = -1 // see the Reader struct information below
	row_count := 0
	for {
		
		record, err := reader.Read()
		row_count += 1
		domain_name := record[1]
		}(domain_name)

	}
}

func main() {

	concurrency := 1000
	
}

