package graphite

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
)

type Graphite struct {
	DebugFilter string `toml:"debug_filter"`
	// URL is only for backwards compatability
	Servers  []string
	Prefix   string
	Template string
	Timeout  int
	conns    []net.Conn
}

var sampleConfig = `
  debug_filter = "fos"
  ## TCP endpoint for your graphite instance.
  ## If multiple endpoints are configured, output will be load balanced.
  ## Only one of the endpoints will be written to with each iteration.
  servers = ["localhost:2003"]
  ## Prefix metrics name
  prefix = ""
  ## Graphite output template
  ## see https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  template = "host.tags.measurement.field"
  ## timeout in seconds for the write connection to graphite
  timeout = 2
`

func (g *Graphite) Connect() error {
	return nil
}

func (g *Graphite) Close() error {
	return nil
}

func (g *Graphite) SampleConfig() string {
	return sampleConfig
}

func (g *Graphite) Description() string {
	return "Configuration for Graphite server to send metrics to"
}

// Choose a random server in the cluster to write to until a successful write
// occurs, logging each unsuccessful. If all servers fail, return error.
func (g *Graphite) Write(metrics []telegraf.Metric) error {
	// Prepare data
	s, err := serializers.NewGraphiteSerializer(g.Prefix, g.Template)
	if err != nil {
		return err
	}

	// Send data to a random server
	p := rand.Perm(len(g.Servers))
	for _, n := range p {
		conn, err := net.Dial("udp", g.Servers[n])
		if err != nil {
			log.Println("E! Graphite UDP Connection Error: " + err.Error())
		} else {
			// This will get set to nil if a successful write occurs
			err = errors.New("Could not write to any Graphite server in cluster\n")

			var batch []byte
			var doDebug = len(g.DebugFilter) != 0
			for _, metric := range metrics {
				if doDebug {
					var metricString = metric.String()
					doDebug = doDebug && strings.Contains(metricString, g.DebugFilter)
					if doDebug {
						log.Printf("D! Graphite Output Debug Filter matched outgoing metric: %s\n", metricString)
					}
				}
				buf, err := s.Serialize(metric)
				if err != nil {
					log.Printf("E! Error serializing some metrics to graphite: %s", err.Error())
				}
				if doDebug {
					var graphiteString = string(buf[:])
					if strings.Contains(graphiteString, g.DebugFilter) {
						log.Printf("D! Graphite Output Debug metric line: %s\n", graphiteString)
					}
				}
				if (len(batch) + len(buf)) > 64000 {
					if _, err := conn.Write(batch); err != nil {
						// Error
						log.Println("E! Graphite UDP Write Error: " + err.Error())
					} else {
						// Success
						batch = nil
						batch = append(batch, buf...)
					}
				} else {
					batch = append(batch, buf...)
				}
			}
			if len(batch) > 0 {
				if _, err := conn.Write(batch); err != nil {
					// Error
					log.Println("E! Graphite UDP Write Error: " + err.Error())
				}
			}
		}

		conn.Close()
		if err == nil {
			break
		}
	}

	return err
}

func init() {
	outputs.Add("graphite", func() telegraf.Output {
		return &Graphite{}
	})
}
