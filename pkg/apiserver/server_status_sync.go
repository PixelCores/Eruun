package apiserver

import (
	"context"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
	"github.com/PixelCores/Eruun/pkg/apiserver/infrastructure/informer"
)

func (s *restServer) syncComponentStatus(update *informer.ComponentStatusUpdate) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	application.SyncComponentStatus(ctx, s.dataStore, s.cache, update)
}
