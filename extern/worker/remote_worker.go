package worker

import (
	"context"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/luluup777/wdkeep/api"
	"github.com/luluup777/wdkeep/cli"
	"golang.org/x/xerrors"
	"net/http"
)

type remoteWorker struct {
	api.WdKeepWorker
	closer jsonrpc.ClientCloser
}

func ConnectRemoteWorker(ctx context.Context, url string) (*remoteWorker, error) {
	headers := http.Header{}

	wapi, closer, err := cli.NewWdKeepWorkerRPCV0(context.TODO(), url, headers)
	if err != nil {
		return nil, xerrors.Errorf("creating jsonrpc client: %w", err)
	}

	return &remoteWorker{wapi, closer}, nil
}

func (r *remoteWorker) Close() error {
	r.closer()
	return nil
}

var _ api.WdKeepWorker = &remoteWorker{}
