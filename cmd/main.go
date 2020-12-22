package main

import (
	"net/http"
	_ "net/http/pprof"

	"github.com/baetyl/baetyl-go/v2/context"

	"github.com/baetyl/baetyl-broker/v2/broker"
	_ "github.com/baetyl/baetyl-broker/v2/store/pebble"
	// rocksdb compile on different arch haven't support
	_ "github.com/baetyl/baetyl-broker/v2/store/rocksdb"
)

func main() {
	// go tool pprof http://localhost:6060/debug/pprof/profile
	go func() {
		panic(http.ListenAndServe(":8005", nil))
	}()

	context.Run(func(ctx context.Context) error {
		//if err := ctx.CheckSystemCert(); err != nil {
		//	return err
		//}

		var cfg broker.Config
		err := ctx.LoadCustomConfig(&cfg)
		if err != nil {
			return err
		}

		//cert := ctx.SystemConfig().Certificate
		//cfg.Listeners = append(cfg.Listeners, listener.Listener{
		//	Address:     "ssl://0.0.0.0:" + ctx.BrokerPort(),
		//	Anonymous:   true,
		//	Certificate: cert,
		//})

		b, err := broker.NewBroker(cfg)
		if err != nil {
			return err
		}
		defer b.Close()
		ctx.Wait()
		return nil
	})
}
