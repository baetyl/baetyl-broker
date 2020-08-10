package main

import (
	// "net/http"
	// _ "net/http/pprof"

	"github.com/baetyl/baetyl-broker/broker"
	"github.com/baetyl/baetyl-go/v2/context"
)

func main() {
	// // go tool pprof http://localhost:6060/debug/pprof/profile
	// go func() {
	// 	panic(http.ListenAndServe(":6060", nil))
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

	context.Run(func(ctx context.Context) error {
		var cfg broker.Config
		err := ctx.LoadCustomConfig(&cfg)
		if err != nil {
			return err
		}
		b, err := broker.NewBroker(cfg)
		if err != nil {
			return err
		}
		defer b.Close()
		ctx.Wait()
		return nil
	})
}