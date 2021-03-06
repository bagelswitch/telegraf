package influxdb

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/outputs"

	"github.com/influxdata/influxdb/client/v2"
)

// maintain a frequency table by measurement and tag value
// this will allow us to configure service protection on influx outputs
//
// {
// 	"http.response" : {
// 		"method": {
// 			"GET": 4,
//			"POST": 7
//		}.
// 		"path": {
// 			"foo": 73,
//			"bar": 12
//		}
// 	}
// }
//

var lastDump = time.Now()
var freqTable = map[string]map[string]map[string]int64{}

func DumpFreqTable(frequencyTable map[string]map[string]map[string]int64) {
	log.Printf("D! %75s | %10s | %15s | %15s | %15s | %15s\n", "Measurement", "tag count", "series cardinality", "total datapoints", "worst tag", "worst tag cardinality")
	for measurementName, measurement := range frequencyTable {
		var tagCount = 0
		var tagValueCount = 0
		var dataPointCount int64 = 0
		var maxTagLen = 0
		var maxTagName = ""
		for tagName, tag := range measurement {
			tagCount += 1
			var tagLen = len(tag)
			if tagLen > maxTagLen {
				maxTagLen = tagLen
				maxTagName = tagName
			}
			for _, count := range tag {
				tagValueCount += 1
				dataPointCount += count
			}
		}
		log.Printf("D! %75s | %10d | %15d | %15d | %15s | %15d\n", measurementName, tagCount, tagValueCount, dataPointCount, maxTagName, maxTagLen)
	}
}


type InfluxDB struct {
	DebugFilter    string `toml:"debug_filter"`
	// URL is only for backwards compatability
	URL              string
	URLs             []string `toml:"urls"`
	Username         string
	Password         string
	Database         string
	UserAgent        string
	RetentionPolicy  string
	WriteConsistency string
	Timeout          internal.Duration
	UDPPayload       int `toml:"udp_payload"`

	// Path to CA file
	SSLCA string `toml:"ssl_ca"`
	// Path to host cert file
	SSLCert string `toml:"ssl_cert"`
	// Path to cert key file
	SSLKey string `toml:"ssl_key"`
	// Use SSL but skip chain & host verification
	InsecureSkipVerify bool

	// Precision is only here for legacy support. It will be ignored.
	Precision string

	conns []client.Client
}

var sampleConfig = `
  debug_filter = "fos"
  ## The full HTTP or UDP endpoint URL for your InfluxDB instance.
  ## Multiple urls can be specified as part of the same cluster,
  ## this means that only ONE of the urls will be written to each interval.
  # urls = ["udp://localhost:8089"] # UDP endpoint example
  urls = ["http://localhost:8086"] # required
  ## The target database for metrics (telegraf will create it if not exists).
  database = "telegraf" # required

  ## Retention policy to write to. Empty string writes to the default rp.
  retention_policy = ""
  ## Write consistency (clusters only), can be: "any", "one", "quorum", "all"
  write_consistency = "any"

  ## Write timeout (for the InfluxDB client), formatted as a string.
  ## If not provided, will default to 5s. 0s means no timeout (not recommended).
  timeout = "5s"
  # username = "telegraf"
  # password = "metricsmetricsmetricsmetrics"
  ## Set the user agent for HTTP POSTs (can be useful for log differentiation)
  # user_agent = "telegraf"
  ## Set UDP payload size, defaults to InfluxDB UDP Client default (512 bytes)
  # udp_payload = 512

  ## Optional SSL Config
  # ssl_ca = "/etc/telegraf/ca.pem"
  # ssl_cert = "/etc/telegraf/cert.pem"
  # ssl_key = "/etc/telegraf/key.pem"
  ## Use SSL but skip chain & host verification
  # insecure_skip_verify = false
`

func (i *InfluxDB) Connect() error {
	var urls []string
	for _, u := range i.URLs {
		urls = append(urls, u)
	}

	// Backward-compatability with single Influx URL config files
	// This could eventually be removed in favor of specifying the urls as a list
	if i.URL != "" {
		urls = append(urls, i.URL)
	}

	tlsCfg, err := internal.GetTLSConfig(
		i.SSLCert, i.SSLKey, i.SSLCA, i.InsecureSkipVerify)
	if err != nil {
		return err
	}

	var conns []client.Client
	for _, u := range urls {
		switch {
		case strings.HasPrefix(u, "udp"):
			parsed_url, err := url.Parse(u)
			if err != nil {
				return err
			}

			if i.UDPPayload == 0 {
				i.UDPPayload = client.UDPPayloadSize
			}
			c, err := client.NewUDPClient(client.UDPConfig{
				Addr:        parsed_url.Host,
				PayloadSize: i.UDPPayload,
			})
			if err != nil {
				return err
			}
			conns = append(conns, c)
		default:
			// If URL doesn't start with "udp", assume HTTP client
			c, err := client.NewHTTPClient(client.HTTPConfig{
				Addr:      u,
				Username:  i.Username,
				Password:  i.Password,
				UserAgent: i.UserAgent,
				Timeout:   i.Timeout.Duration,
				TLSConfig: tlsCfg,
			})
			if err != nil {
				return err
			}

			err = createDatabase(c, i.Database)
			if err != nil {
				log.Println("E! Database creation failed: " + err.Error())
				continue
			}

			conns = append(conns, c)
		}
	}

	i.conns = conns
	rand.Seed(time.Now().UnixNano())
	return nil
}

func createDatabase(c client.Client, database string) error {
	// Create Database if it doesn't exist
	_, err := c.Query(client.Query{
		Command: fmt.Sprintf("CREATE DATABASE \"%s\"", database),
	})
	return err
}

func (i *InfluxDB) Close() error {
	var errS string
	for j, _ := range i.conns {
		if err := i.conns[j].Close(); err != nil {
			errS += err.Error()
		}
	}
	if errS != "" {
		return fmt.Errorf("output influxdb close failed: %s", errS)
	}
	return nil
}

func (i *InfluxDB) SampleConfig() string {
	return sampleConfig
}

func (i *InfluxDB) Description() string {
	return "Configuration for influxdb server to send metrics to"
}

// Choose a random server in the cluster to write to until a successful write
// occurs, logging each unsuccessful. If all servers fail, return error.
func (i *InfluxDB) Write(metrics []telegraf.Metric) error {
	if len(i.conns) == 0 {
		err := i.Connect()
		if err != nil {
			return err
		}
	}
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:         i.Database,
		RetentionPolicy:  i.RetentionPolicy,
		WriteConsistency: i.WriteConsistency,
	})
	if err != nil {
		return err
	}

	var doDebug = len(i.DebugFilter) != 0
	for _, metric := range metrics {
		if doDebug {
			var metricString = metric.String()
			doDebug = doDebug && strings.Contains(metricString, i.DebugFilter)
			if doDebug {
				log.Printf("D! InfluxDB Output Debug Filter matched outgoing metric: %s\n", metricString)
			}
			// collect meta-metrics for use in service protection
			var measurementName = metric.Name()
			if _, measureok := freqTable[measurementName]; !measureok {
				freqTable[measurementName] = map[string]map[string]int64{}
			}
			var tags = metric.Tags()
			for tagName, tagVal := range tags {
				if _, tagok := freqTable[measurementName][tagName]; !tagok {
					freqTable[measurementName][tagName] = map[string]int64{}
				}
				if _, ok := freqTable[measurementName][tagName][tagVal]; !ok {
					freqTable[measurementName][tagName][tagVal] = int64(1)
				}
				freqTable[measurementName][tagName][tagVal] += int64(1)
			}
		}
		bp.AddPoint(metric.Point())
	}

	// This will get set to nil if a successful write occurs
	err = errors.New("Could not write to any InfluxDB server in cluster")

	p := rand.Perm(len(i.conns))
	for _, n := range p {
		if e := i.conns[n].Write(bp); e != nil {
			// Log write failure
			log.Printf("E! InfluxDB Output Error: %s", e)
			// If the database was not found, try to recreate it
			if strings.Contains(e.Error(), "database not found") {
				if errc := createDatabase(i.conns[n], i.Database); errc != nil {
					log.Printf("E! Error: Database %s not found and failed to recreate\n",
						i.Database)
				}
			}
		} else {
			err = nil
			break
		}
	}

	if doDebug {
		// emit the current frequency table for service protection info
		var elapsed = time.Since(lastDump)
		if elapsed.Seconds() > float64(60) {
			lastDump = time.Now()
			DumpFreqTable(freqTable)
		}
	}

	return err
}

func init() {
	outputs.Add("influxdb", func() telegraf.Output {
		return &InfluxDB{
			Timeout: internal.Duration{Duration: time.Second * 5},
		}
	})
}
