package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/baetyl/baetyl-broker/utils"
	"github.com/baetyl/baetyl-broker/utils/log"
	_ "github.com/mattn/go-sqlite3"
)

var (
	h bool
	c string
)

func init() {
	flag.BoolVar(&h, "h", false, "this help")
	flag.StringVar(&c, "c", "etc/baetyl/service.yml", "set configuration file")
}

func main() {

	// f, err := os.Create("trace.out")
	// if err != nil {
	// 	panic(err)
	// }
	// defer f.Close()

	// err = trace.Start(f)
	// if err != nil {
	// 	panic(err)
	// }
	// defer trace.Stop()

	flag.Parse()

	if h {
		flag.Usage()
		return
	}

	// baetyl.Run(func(ctx baetyl.Context) error {
	var cfg config
	if utils.FileExists(c) {
		utils.LoadYAML(c, &cfg)
	} else {
		log.Warn("configuration file not found, to use default configuration", nil)
		utils.SetDefaults(&cfg)
	}
	b, err := newBroker(cfg)
	if err != nil {
		log.Fatal("failed to create broker", err)
	}
	defer b.close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	signal.Ignore(syscall.SIGPIPE)
	<-sig
	// })
}
