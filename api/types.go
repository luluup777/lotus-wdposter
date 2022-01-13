package api

import "golang.org/x/xerrors"

var ErrNotSupported = xerrors.New("method not supported")