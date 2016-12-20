package graphite

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers"
	"github.com/influxdata/telegraf/metric"
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
	// Set default values
	if g.Timeout <= 0 {
		g.Timeout = 2
	}
	if len(g.Servers) == 0 {
		g.Servers = append(g.Servers, "localhost:2003")
	}
	// Get Connections
	var conns []net.Conn
	for _, server := range g.Servers {
		conn, err := net.DialTimeout("tcp", server, time.Duration(g.Timeout)*time.Second)
		if err == nil {
			conns = append(conns, conn)
		}
	}
	g.conns = conns
	return nil
}

func (g *Graphite) Close() error {
	// Closing all connections
	for _, conn := range g.conns {
		conn.Close()
	}
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
	var batch []byte
	s, err := serializers.NewGraphiteSerializer(g.Prefix, g.Template)
	if err != nil {
		return err
	}

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
			for _, graphiteBytes := range buf {
				var graphiteString = string(graphiteBytes[:])
				if strings.Contains(graphiteString, g.DebugFilter) {
					log.Printf("D! Graphite Output Debug metric line: %s\n", graphiteString)
				}
			}
		}
		batch = append(batch, buf...)
	}

	// This will get set to nil if a successful write occurs
	err = errors.New("Could not write to any Graphite server in cluster\n")

	// Send data to a random server
	p := rand.Perm(len(g.conns))
	for _, n := range p {
		if g.Timeout > 0 {
			g.conns[n].SetWriteDeadline(time.Now().Add(time.Duration(g.Timeout) * time.Second))
		}
		if _, e := g.conns[n].Write(batch); e != nil {
			// Error
			log.Println("E! Graphite Error: " + e.Error())
			// Let's try the next one
		} else {
			// Success
			err = nil
			break
		}
	}
	// try to reconnect
	if err != nil {
		g.Connect()
	}
	return err
}

func init() {
	outputs.Add("graphite", func() telegraf.Output {
		return &Graphite{}
	})
}
