package main

import (
	"context"
	"fmt"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/metrics"
	"github.com/gorilla/mux"
	logging "github.com/ipfs/go-log/v2"
	"github.com/luluup777/wdkeep/api"
	"github.com/luluup777/wdkeep/build"
	wdcli "github.com/luluup777/wdkeep/cli"
	"github.com/luluup777/wdkeep/extern/worker"
	"github.com/urfave/cli/v2"
	"go.opencensus.io/tag"
	"golang.org/x/xerrors"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

var log = logging.Logger("wdkeep-worker")

func main() {
	_ = logging.SetLogLevel("*", "INFO")

	local := []*cli.Command{
		runCmd,
	}

	var app = &cli.App{
		Name:                 "wdkeep-worker",
		Usage:                "Remote wdpost worker",
		Version:              build.WdKeepWorkerAPIVersion(),
		EnableBashCompletion: true,
		Commands:             local,
	}
	app.Setup()

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}

var runCmd = &cli.Command{
	Name:  "run",
	Usage: "Start wdpost worker",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "workerId",
			Usage: "wdpost worker id",
		},
		&cli.StringFlag{
			Name:  "listen",
			Usage: "host address and port the wdpost worker api will listen on",
			Value: "0.0.0.0:3434",
		},
		&cli.IntFlag{
			Name:  "parallel",
			Usage: "maximum operations to run in parallel",
			Value: 1,
		},
	},
	Before: func(c *cli.Context) error {
		// todo check env
		return nil
	},
	Action: func(cctx *cli.Context) error {
		log.Info("Starting wdpost worker")

		ctx := wdcli.ReqContext(cctx)
		var keeperApi api.WdKeepManager
		var closer func()
		var err error
		for {
			keeperApi, closer, err = wdcli.GetWdKeepManagerAPI(cctx, wdcli.WdKeepManagerUseHttp)
			if err == nil {
				_, err = keeperApi.Version(ctx)
				if err == nil {
					break
				}
			}
			fmt.Printf("\r\x1b[0KConnecting to wdpost keeper API... (%s)", err)
			time.Sleep(time.Second)
			continue
		}

		defer closer()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		v, err := keeperApi.Version(ctx)
		if err != nil {
			return err
		}

		log.Infof("Remote version %s", v)

		log.Info("Opening local storage; connecting to master")
		address := cctx.String("listen")
		addressSlice := strings.Split(address, ":")
		if ip := net.ParseIP(addressSlice[0]); ip == nil {
			return xerrors.Errorf("listen: %s is err", address)
		}

		runLimit := cctx.Int("parallel")
		workerId := cctx.String("workerId")
		if workerId == "" {
			workerId, err = os.Hostname()
			if err != nil {
				log.Errorw("os.Hostname", "err", err)
				return xerrors.New("get Hostname err")
			}
		}

		workerApi := worker.NewLocalWorker(workerId, keeperApi, runLimit)

		mux := mux.NewRouter()

		log.Info("Setting up control endpoint at " + address)

		rpcServer := jsonrpc.NewServer()
		rpcServer.Register("Wdpost", workerApi)

		mux.Handle("/rpc/v0", rpcServer)
		mux.PathPrefix("/").Handler(http.DefaultServeMux) // pprof

		// todo add auth Verify

		srv := &http.Server{
			Handler: mux,
			BaseContext: func(listener net.Listener) context.Context {
				ctx, _ := tag.New(context.Background(), tag.Upsert(metrics.APIInterface, "wdkeep-worker"))
				return ctx
			},
		}

		go func() {
			<-ctx.Done()
			log.Warn("Shutting down...")
			if err := srv.Shutdown(context.TODO()); err != nil {
				log.Errorf("shutting down RPC server failed: %s", err)
			}
			log.Warn("Graceful shutdown successful")
		}()

		nl, err := net.Listen("tcp", address)
		if err != nil {
			return err
		}

		keeperSessID, err := keeperApi.Session(ctx)
		if err != nil {
			return xerrors.Errorf("getting miner session: %w", err)
		}

		go func() {
			heartbeats := time.NewTicker(time.Minute * 1)
			defer heartbeats.Stop()

			var readyCh chan struct{}
			for {

				if readyCh == nil {
					log.Info("Making sure no local tasks are running")
					readyCh = make(chan struct{})
					close(readyCh)
				}

				for {
					curSession, err := keeperApi.Session(ctx)
					if err != nil {
						log.Errorf("heartbeat: checking remote session failed: %+v", err)
					} else {
						if curSession != keeperSessID {
							keeperSessID = curSession
							break
						}
					}

					select {
					case <-readyCh:
						if err := keeperApi.WorkerConnect(ctx, "http://"+address+"/rpc/v0"); err != nil {
							log.Errorf("Registering worker failed: %+v", err)
							cancel()
							return
						}
						log.Info("Worker registered successfully, waiting for tasks")

						readyCh = nil

					case <-heartbeats.C:
					case <-ctx.Done():
						return // graceful shutdown
					}
				}

				log.Errorf("WDPOST-KEEPER CONNECTION LOST")
			}
		}()

		return srv.Serve(nl)
	},
}
