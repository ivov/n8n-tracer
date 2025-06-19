package ingestion

import "context"

type Ingester interface {
	Start(ctx context.Context) (<-chan interface{}, <-chan error)
	Stop()
}
