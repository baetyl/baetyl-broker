package main

import (
	"flag"
	// "net/http"
	// _ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/baetyl/baetyl-go/log"
	"github.com/baetyl/baetyl-go/utils"
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
	utils.Version()

	// // go tool pprof http://localhost:6060/debug/pprof/profile
	// go func() {
	// 	panic(http.ListenAndServe("localhost:6060", nil))
	// }()

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
	var cfg Config
	if utils.FileExists(c) {
		utils.LoadYAML(c, &cfg)
	} else {
		utils.SetDefaults(&cfg)
	}
	l, err := log.Init(cfg.Logger)
	if err != nil {
		panic(err)
	}
	defer l.Sync()
	b, err := NewBroker(cfg)
	if err != nil {
		l.Fatal("failed to create broker", log.Error(err))
	}
	defer b.Close()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	signal.Ignore(syscall.SIGPIPE)
	<-sig
	// })
}
