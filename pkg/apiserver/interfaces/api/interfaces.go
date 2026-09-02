package api

import (
	"reflect"
	"sync"

	"github.com/gin-gonic/gin"
	"k8s.io/klog/v2"
)

var versionPrefix = "/api/v1"

// GetAPIPrefix return the prefix of the api route path
func GetAPIPrefix() []string {
	return []string{versionPrefix}
}

// The Interface API should define the http route
type Interface interface {
	RegisterRoutes(group *gin.RouterGroup)
}

var (
	registeredAPI      []Interface
	registeredAPIKinds = map[reflect.Type]struct{}{}
	registeredAPIMu    sync.Mutex
	initAPIOnce        sync.Once
)

// RegisterAPI register API handler
func RegisterAPI(ws Interface) {
	if ws == nil {
		klog.InfoS("skip registering nil api handler")
		return
	}
	kind := reflect.TypeOf(ws)
	registeredAPIMu.Lock()
	defer registeredAPIMu.Unlock()
	if _, exists := registeredAPIKinds[kind]; exists {
		klog.InfoS("api handler already registered, skip duplicate", "kind", kind.String())
		return
	}
	registeredAPI = append(registeredAPI, ws)
	registeredAPIKinds[kind] = struct{}{}
}

// GetRegisteredAPI return all API handlers
func GetRegisteredAPI() []Interface {
	registeredAPIMu.Lock()
	defer registeredAPIMu.Unlock()
	apis := make([]Interface, len(registeredAPI))
	copy(apis, registeredAPI)
	return apis
}

func registerBuiltinAPIs() {
	RegisterAPI(NewApplications())
	RegisterAPI(NewSettings())
	RegisterAPI(NewProgrammingLanguages())
	RegisterAPI(NewOAuth())
	RegisterAPI(&health{})
}

// ResetAPIRegistryForTest clears API registry globals. Intended for tests and
// controlled re-initialization scenarios.
func ResetAPIRegistryForTest() {
	registeredAPIMu.Lock()
	defer registeredAPIMu.Unlock()
	registeredAPI = nil
	registeredAPIKinds = map[reflect.Type]struct{}{}
	initAPIOnce = sync.Once{}
}

// InitAPIBean inits all API handlers, pass in the required parameter object.
// It can be implemented using the idea of dependency injection.
func InitAPIBean() []interface{} {
	initAPIOnce.Do(registerBuiltinAPIs)
	apis := GetRegisteredAPI()
	var beans []interface{}
	for i := range apis {
		beans = append(beans, apis[i])
	}
	return beans
}
