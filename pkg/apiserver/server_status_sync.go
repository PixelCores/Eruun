package apiserver

import (
	"context"
	"time"

	"github.com/PixelCores/Eruun/pkg/apiserver/domain/model"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/service/application"
)

func (s *restServer) syncComponentStatus(update *model.ComponentStatusUpdate) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	application.SyncComponentStatus(ctx, s.dataStore, s.cache, update)
}
