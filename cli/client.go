package cli

import (
	"context"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/lib/rpcenc"
	"github.com/luluup777/wdkeep/api"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"net/http"
	"net/url"
	"path"
	"time"
)

type GetWdKeepManagerOptions struct {
	PreferHttp bool
}

type GetWdKeepManagerOption func(*GetWdKeepManagerOptions)

func WdKeepManagerUseHttp(opts *GetWdKeepManagerOptions) {
	opts.PreferHttp = true
}

func GetWdKeepManagerAPI(ctx *cli.Context, opts ...GetWdKeepManagerOption) (api.WdKeepManager, jsonrpc.ClientCloser, error) {
	var options GetWdKeepManagerOptions
	for _, opt := range opts {
		opt(&options)
	}

	addr, headers, err := GetRawAPI(WdKeepManager, "v0")
	if err != nil {
		return nil, nil, err
	}

	if options.PreferHttp {
		u, err := url.Parse(addr)
		if err != nil {
			return nil, nil, xerrors.Errorf("parsing miner api URL: %w", err)
		}

		switch u.Scheme {
		case "ws":
			u.Scheme = "http"
		case "wss":
			u.Scheme = "https"
		}

		addr = u.String()
	}

	return NewWdKeepManagerRPCV0(ctx.Context, addr, headers)
}

func NewWdKeepManagerRPCV0(ctx context.Context, addr string, requestHeader http.Header, opts ...jsonrpc.Option) (api.WdKeepManager, jsonrpc.ClientCloser, error) {
	pushUrl, err := getPushUrl(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.WdKeepManagerStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "Wdpost",
		GetInternalStructs(&res), requestHeader,
		append([]jsonrpc.Option{
			rpcenc.ReaderParamEncoder(pushUrl),
		}, opts...)...)

	return &res, closer, err
}

func GetWdKeepWorkerAPI(ctx *cli.Context) (api.WdKeepWorker, jsonrpc.ClientCloser, error) {
	addr, headers, err := GetRawAPI(WdKeepWorker, "v0")
	if err != nil {
		return nil, nil, err
	}

	return NewWdKeepWorkerRPCV0(ctx.Context, addr, headers)
}

func NewWdKeepWorkerRPCV0(ctx context.Context, addr string, requestHeader http.Header) (api.WdKeepWorker, jsonrpc.ClientCloser, error) {
	pushUrl, err := getPushUrl(addr)
	if err != nil {
		return nil, nil, err
	}

	var res api.WdKeepWorkerStruct
	closer, err := jsonrpc.NewMergeClient(ctx, addr, "Wdpost",
		GetInternalStructs(&res),
		requestHeader,
		rpcenc.ReaderParamEncoder(pushUrl),
		jsonrpc.WithNoReconnect(),
		jsonrpc.WithTimeout(30*time.Second),
	)

	return &res, closer, err
}

func getPushUrl(addr string) (string, error) {
	pushUrl, err := url.Parse(addr)
	if err != nil {
		return "", err
	}
	switch pushUrl.Scheme {
	case "ws":
		pushUrl.Scheme = "http"
	case "wss":
		pushUrl.Scheme = "https"
	}

	pushUrl.Path = path.Join(pushUrl.Path, "../streams/v0/push")
	return pushUrl.String(), nil
}
