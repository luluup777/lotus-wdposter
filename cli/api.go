package cli

import (
	"context"
	"fmt"
	util "github.com/filecoin-project/lotus/cli/util"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

type ApiType int

const (
	_                    = iota // Default is invalid
	WdKeepManager ApiType = iota
	WdKeepWorker
)

func EnvsForAPIInfos(t ApiType) (primary string) {
	switch t {
	case WdKeepManager:
		return "WD_POST_KEEPER_API_INFO"
	case WdKeepWorker:
		return "WD_POST_WORKER_API_INFO"
	default:
		panic(fmt.Sprintf("Unknown repo type: %v", t))
	}
}

func GetAPIInfo(t ApiType) (util.APIInfo, error) {
	primaryEnv := EnvsForAPIInfos(t)
	env, ok := os.LookupEnv(primaryEnv)
	if ok {
		return util.ParseApiInfo(env), nil
	}

	return util.APIInfo{}, fmt.Errorf("please set environment variables: %s", primaryEnv)
}

func GetRawAPI(t ApiType, version string) (string, http.Header, error) {
	ainfo, err := GetAPIInfo(t)
	if err != nil {
		return "", nil, xerrors.Errorf("could not get API info for %s: %w", t, err)
	}

	addr, err := ainfo.DialArgs(version)
	if err != nil {
		return "", nil, xerrors.Errorf("could not get DialArgs: %w", err)
	}

	return addr, ainfo.AuthHeader(), nil
}

func ReqContext(cctx *cli.Context) context.Context {
	ctx, done := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 2)
	go func() {
		<-sigChan
		done()
	}()
	signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)

	return ctx
}