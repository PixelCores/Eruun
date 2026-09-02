package conversion

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"github.com/PixelCores/Eruun/pkg/apiserver/config"
	"github.com/PixelCores/Eruun/pkg/apiserver/domain/spec"
	apis "github.com/PixelCores/Eruun/pkg/apiserver/interfaces/api/dto/v1"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils"
	"github.com/PixelCores/Eruun/pkg/apiserver/utils/bcode"
)

func TestConvertKubeResources_StatefulSetWithService(t *testing.T) {
	yamlText := `
apiVersion: v1
kind: ConfigMap
metadata:
  name: test-config
data:
  my.cnf: "[mysqld]\nmax_connections=1000\n"
---
apiVersion: v1
kind: Secret
metadata:
  name: test-secret
data:
  PASSWORD: dGVzdA==
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mysql
  labels:
    app: mysql
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mysql
  template:
    metadata:
      labels:
        app: mysql
    spec:
      containers:
        - name: mysql
          image: mysql:5.7
          ports:
            - containerPort: 3306
          env:
            - name: MYSQL_DATABASE
              value: game
            - name: MYSQL_ROOT_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: test-secret
                  key: PASSWORD
          volumeMounts:
            - name: data
              mountPath: /var/lib/mysql
            - name: conf
              mountPath: /etc/mysql/conf.d
        - name: xtrabackup
          image: xtrabackup:latest
          command:
            - sh
            - -c
            - "echo ok"
      volumes:
        - name: conf
          configMap:
            name: test-config
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes:
          - ReadWriteOnce
        resources:
          requests:
            storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: mysql
spec:
  selector:
    app: mysql
  ports:
    - port: 3306
      targetPort: 3306
`
	svc := &conversionServiceImpl{
		ValidationService: NewValidationService(),
		Cfg:               &config.Config{AllowPrivateURLTargets: true},
	}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)
	require.Empty(t, resp.Warnings)
	require.Len(t, resp.Components, 3)

	mysql := findComponent(resp.Components, "mysql")
	require.NotNil(t, mysql)
	require.Equal(t, config.StoreJob, mysql.ComponentType)
	require.Equal(t, "mysql:5.7", mysql.Image)
	require.Equal(t, int32(1), mysql.Replicas)
	require.Equal(t, "game", mysql.Properties.Env["MYSQL_DATABASE"])
	require.Len(t, mysql.Properties.Ports, 1)
	require.Equal(t, int32(3306), mysql.Properties.Ports[0].Port)
	require.Len(t, mysql.Traits.Service, 1)
	require.Equal(t, "mysql", mysql.Traits.Service[0].Name)
	require.Equal(t, "internal", mysql.Traits.Service[0].Type)
	require.Equal(t, map[string]string{"app": "mysql"}, mysql.Traits.Service[0].Selector)
	require.Len(t, mysql.Traits.Service[0].Ports, 1)
	require.Equal(t, int32(3306), mysql.Traits.Service[0].Ports[0].Port)
	require.Equal(t, int32(3306), mysql.Traits.Service[0].Ports[0].TargetPort)

	require.Len(t, mysql.Traits.Envs, 1)
	require.Equal(t, "MYSQL_ROOT_PASSWORD", mysql.Traits.Envs[0].Name)
	require.NotNil(t, mysql.Traits.Envs[0].ValueFrom.Secret)
	require.Equal(t, "test-secret", mysql.Traits.Envs[0].ValueFrom.Secret.Name)
	require.Equal(t, "PASSWORD", mysql.Traits.Envs[0].ValueFrom.Secret.Key)

	require.Len(t, mysql.Traits.Sidecar, 1)
	require.Equal(t, "xtrabackup", mysql.Traits.Sidecar[0].Name)
	require.Equal(t, "xtrabackup:latest", mysql.Traits.Sidecar[0].Image)

	storageData := findStorage(mysql.Traits.Storage, "data")
	require.NotNil(t, storageData)
	require.Equal(t, config.StorageTypePersistent, storageData.Type)
	require.True(t, storageData.TmpCreate)
	require.Equal(t, "1Gi", storageData.Size)
	storageConf := findStorage(mysql.Traits.Storage, "conf")
	require.NotNil(t, storageConf)
	require.Equal(t, config.StorageTypeConfig, storageConf.Type)
	require.Equal(t, "test-config", storageConf.SourceName)

	configComp := findComponent(resp.Components, "test-config")
	require.NotNil(t, configComp)
	require.Equal(t, config.ConfJob, configComp.ComponentType)
	require.Equal(t, "[mysqld]\nmax_connections=1000\n", configComp.Properties.Conf["my.cnf"])

	secretComp := findComponent(resp.Components, "test-secret")
	require.NotNil(t, secretComp)
	require.Equal(t, config.SecretJob, secretComp.ComponentType)
	require.Equal(t, "test", secretComp.Properties.Secret["PASSWORD"])
}

func TestConvertKubeResources_StripsReservedLabelsFromHelmMetadata(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  labels:
    app.kubernetes.io/name: api
    app.kubernetes.io/managed-by: Helm
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: api
  template:
    metadata:
      labels:
        app.kubernetes.io/name: api
        app.kubernetes.io/managed-by: Helm
    spec:
      containers:
        - name: api
          image: nginx:1.27
---
apiVersion: v1
kind: Service
metadata:
  name: api
  labels:
    app.kubernetes.io/name: api
    app.kubernetes.io/managed-by: Helm
spec:
  selector:
    app.kubernetes.io/name: api
    app.kubernetes.io/managed-by: Helm
  ports:
    - port: 80
      targetPort: 80
`
	svc := &conversionServiceImpl{
		ValidationService: NewValidationService(),
		Cfg:               &config.Config{AllowPrivateURLTargets: true},
	}

	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})

	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)
	component := findComponent(resp.Components, "api")
	require.NotNil(t, component)
	require.Equal(t, "api", component.Properties.Labels["app.kubernetes.io/name"])
	require.NotContains(t, component.Properties.Labels, config.LabelManagedBy)
	require.Len(t, component.Traits.Service, 1)
	require.Equal(t, "api", component.Traits.Service[0].Labels["app.kubernetes.io/name"])
	require.NotContains(t, component.Traits.Service[0].Labels, config.LabelManagedBy)
	require.Equal(t, config.ManagedByEruun, component.Traits.Service[0].Selector[config.LabelManagedBy])
}

func TestConvertKubeResources_PreservesStorageSubPathExpr(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: backend
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: backend
  template:
    metadata:
      labels:
        app: backend
    spec:
      containers:
        - name: backend
          image: nginx:latest
          volumeMounts:
            - name: logs
              mountPath: /app/log
              subPathExpr: $(TZ)/game/$(INSTANCE_ID)/$(SERVER_NAME)/$(POD_IP)
      volumes:
        - name: logs
          persistentVolumeClaim:
            claimName: developer-pvc
`
	svc := &conversionServiceImpl{
		ValidationService: NewValidationService(),
		Cfg:               &config.Config{AllowPrivateURLTargets: true},
	}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)

	backend := findComponent(resp.Components, "backend")
	require.NotNil(t, backend)
	storage := findStorage(backend.Traits.Storage, "logs")
	require.NotNil(t, storage)
	require.Equal(t, config.StorageTypePersistent, storage.Type)
	require.Equal(t, "/app/log", storage.MountPath)
	require.Equal(t, "developer-pvc", storage.ClaimName)
	require.Empty(t, storage.SubPath)
	require.Equal(t, "$(TZ)/game/$(INSTANCE_ID)/$(SERVER_NAME)/$(POD_IP)", storage.SubPathExpr)
}

func TestConvertKubeResources_WorkloadRolloutTraits(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: default
  labels:
    app: api
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 25%
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      containers:
        - name: api
          image: nginx:1.25
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: mysql
  namespace: default
  labels:
    app: mysql
spec:
  replicas: 3
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      partition: 2
      maxUnavailable: 1
  selector:
    matchLabels:
      app: mysql
  template:
    metadata:
      labels:
        app: mysql
    spec:
      containers:
        - name: mysql
          image: mysql:8
`
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)

	api := findComponent(resp.Components, "api")
	require.NotNil(t, api)
	require.NotNil(t, api.Traits.Rollout)
	require.Equal(t, "RollingUpdate", api.Traits.Rollout.Type)
	require.NotNil(t, api.Traits.Rollout.RollingUpdate)
	require.Equal(t, int32(1), api.Traits.Rollout.RollingUpdate.MaxSurge.IntVal)
	require.Equal(t, "25%", api.Traits.Rollout.RollingUpdate.MaxUnavailable.StrVal)

	mysql := findComponent(resp.Components, "mysql")
	require.NotNil(t, mysql)
	require.NotNil(t, mysql.Traits.Rollout)
	require.Equal(t, "RollingUpdate", mysql.Traits.Rollout.Type)
	require.NotNil(t, mysql.Traits.Rollout.RollingUpdate)
	require.Equal(t, int32(2), *mysql.Traits.Rollout.RollingUpdate.Partition)
	require.Equal(t, int32(1), mysql.Traits.Rollout.RollingUpdate.MaxUnavailable.IntVal)
	require.Nil(t, mysql.Traits.Rollout.RollingUpdate.MaxSurge)
}

func TestConvertKubeResources_OmitsIncompleteDeploymentRolloutTraits(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  namespace: default
  labels:
    app: api
spec:
  strategy:
    type: RollingUpdate
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      containers:
        - name: api
          image: nginx:1.25
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker
  namespace: default
  labels:
    app: worker
spec:
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
  selector:
    matchLabels:
      app: worker
  template:
    metadata:
      labels:
        app: worker
    spec:
      containers:
        - name: worker
          image: busybox:1.36
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: web
  namespace: default
  labels:
    app: web
spec:
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 25%
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
        - name: web
          image: nginx:1.25
`
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)

	for _, name := range []string{"api", "worker", "web"} {
		component := findComponent(resp.Components, name)
		require.NotNil(t, component)
		require.Nil(t, component.Traits.Rollout)
	}
}

func TestConvertKubeYAMLToComponentsRejectsBinarySecretData(t *testing.T) {
	yamlText := `
apiVersion: v1
kind: Secret
metadata:
  name: test-secret
data:
  CERT: //4=
`

	_, _, err := convertKubeYAMLToComponents(yamlText)
	require.Error(t, err)
	require.ErrorContains(t, err, "non-UTF-8 binary data")
}

func TestConvertKubeResources_ServiceTraits(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mysql-master
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mysql-master
  template:
    metadata:
      labels:
        app: mysql-master
    spec:
      containers:
        - name: mysql
          image: mysql:8.0
          ports:
            - containerPort: 3306
---
apiVersion: v1
kind: Service
metadata:
  name: mysql-master
  labels:
    layer: db
spec:
  selector:
    app: mysql-master
  ports:
    - name: mysql
      port: 3306
      targetPort: 3306
---
apiVersion: v1
kind: Service
metadata:
  name: mysql-master-headless
  labels:
    layer: db
    role: headless
spec:
  clusterIP: None
  type: ClusterIP
  selector:
    app: mysql-master
  ports:
    - name: mysql
      port: 3306
      targetPort: 3306
`
	svc := &conversionServiceImpl{
		ValidationService: NewValidationService(),
		Cfg:               &config.Config{AllowPrivateURLTargets: true},
	}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)
	require.Empty(t, resp.Warnings)

	component := findComponent(resp.Components, "mysql-master")
	require.NotNil(t, component)
	require.Len(t, component.Traits.Service, 2)

	normalSvc := findServiceTrait(component.Traits.Service, "mysql-master")
	require.NotNil(t, normalSvc)
	require.False(t, normalSvc.Headless)
	require.Equal(t, "internal", normalSvc.Type)
	require.Equal(t, "db", normalSvc.Labels["layer"])
	require.Equal(t, "mysql-master", normalSvc.Selector["app"])
	require.Len(t, normalSvc.Ports, 1)
	require.Equal(t, int32(3306), normalSvc.Ports[0].Port)
	require.Equal(t, int32(3306), normalSvc.Ports[0].TargetPort)

	headlessSvc := findServiceTrait(component.Traits.Service, "mysql-master-headless")
	require.NotNil(t, headlessSvc)
	require.True(t, headlessSvc.Headless)
	require.Equal(t, "internal", headlessSvc.Type)
	require.Equal(t, "headless", headlessSvc.Labels["role"])
}

func TestConvertKubeResources_ServiceDefaultNamespaceMatchesEmptyWorkloadNamespace(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      containers:
        - name: api
          image: api:v1
          ports:
            - containerPort: 8080
---
apiVersion: v1
kind: Service
metadata:
  name: api
  namespace: default
spec:
  selector:
    app: api
  ports:
    - port: 8080
      targetPort: 8080
`
	svc := &conversionServiceImpl{
		ValidationService: NewValidationService(),
		Cfg:               &config.Config{AllowPrivateURLTargets: true},
	}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	component := findComponent(resp.Components, "api")
	require.NotNil(t, component)
	require.Len(t, component.Traits.Service, 1)
	require.Equal(t, "api", component.Traits.Service[0].Name)
	require.NotContains(t, resp.Warnings, "service api has no matching workload; skipped")
}

func TestBuildServiceTrait_ExternalName(t *testing.T) {
	svc := &corev1.Service{
		Spec: corev1.ServiceSpec{
			Type:         corev1.ServiceTypeExternalName,
			ExternalName: "example.org",
		},
	}
	trait := buildServiceTrait(svc)
	require.Equal(t, "external", trait.Type)
	require.Equal(t, "example.org", trait.ExternalName)
}

func TestConvertKubeResources_SecurityPolicy(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: security-demo
spec:
  replicas: 1
  selector:
    matchLabels:
      app: security-demo
  template:
    metadata:
      labels:
        app: security-demo
    spec:
      containers:
        - name: security-demo
          image: nginx:1.23
          securityContext:
            runAsUser: 1000
            runAsGroup: 1000
            allowPrivilegeEscalation: false
      initContainers:
        - name: init-perms
          image: busybox:1.36
          command: ["sh", "-c", "echo ok"]
          securityContext:
            runAsUser: 0
            runAsGroup: 0
`
	svc := &conversionServiceImpl{
		ValidationService: NewValidationService(),
		Cfg:               &config.Config{AllowPrivateURLTargets: true},
	}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)

	component := findComponent(resp.Components, "security-demo")
	require.NotNil(t, component)
	require.NotNil(t, component.Traits.SecurityPolicy)
	require.Equal(t, int64(1000), *component.Traits.SecurityPolicy.RunAsUser)
	require.Equal(t, int64(1000), *component.Traits.SecurityPolicy.RunAsGroup)
	require.NotNil(t, component.Traits.SecurityPolicy.AllowPrivilegeEscalation)
	require.False(t, *component.Traits.SecurityPolicy.AllowPrivilegeEscalation)

	require.Len(t, component.Traits.Init, 1)
	require.NotNil(t, component.Traits.Init[0].Traits.SecurityPolicy)
	require.Equal(t, int64(0), *component.Traits.Init[0].Traits.SecurityPolicy.RunAsUser)
	require.Equal(t, int64(0), *component.Traits.Init[0].Traits.SecurityPolicy.RunAsGroup)
}

func TestConvertKubeResources_InitAndSidecarShareContainerTraitBuilder(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: helper-demo
spec:
  replicas: 1
  selector:
    matchLabels:
      app: helper-demo
  template:
    metadata:
      labels:
        app: helper-demo
    spec:
      volumes:
        - name: init-work
          emptyDir: {}
        - name: sidecar-conf
          configMap:
            name: sidecar-config
      initContainers:
        - image: busybox:1.36
          command: ["sh", "-c", "echo init"]
          env:
            - name: INIT_STATIC
              value: ready
          envFrom:
            - secretRef:
                name: init-secret
          resources:
            requests:
              cpu: 50m
          volumeMounts:
            - name: init-work
              mountPath: /work
          securityContext:
            runAsUser: 0
      containers:
        - name: app
          image: nginx:1.23
          resources:
            requests:
              cpu: 100m
              memory: 128Mi
            limits:
              cpu: 250m
              memory: 512Mi
        - image: helper:v1
          command: ["helper"]
          args: ["--serve"]
          env:
            - name: SIDE_STATIC
              value: enabled
            - name: SIDE_SECRET
              valueFrom:
                secretKeyRef:
                  name: sidecar-secret
                  key: token
          envFrom:
            - configMapRef:
                name: sidecar-env
          resources:
            limits:
              memory: 64Mi
          volumeMounts:
            - name: sidecar-conf
              mountPath: /etc/helper
              readOnly: true
          securityContext:
            allowPrivilegeEscalation: false
`
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)
	require.Contains(t, resp.Warnings, "init container name missing for component helper-demo; using helper-demo-init-1")
	require.Contains(t, resp.Warnings, "sidecar container name missing for component helper-demo; using helper-demo-sidecar-1")

	component := findComponent(resp.Components, "helper-demo")
	require.NotNil(t, component)
	require.NotNil(t, component.Traits.Resources)
	require.Equal(t, "100m", component.Traits.Resources.CPU)
	require.Equal(t, "128Mi", component.Traits.Resources.Memory)
	require.Equal(t, "250m", component.Traits.Resources.CPULimit)
	require.Equal(t, "512Mi", component.Traits.Resources.MemoryLimit)

	require.Len(t, component.Traits.Init, 1)
	init := component.Traits.Init[0]
	require.Equal(t, "helper-demo-init-1", init.Name)
	require.Equal(t, "busybox:1.36", init.Image)
	require.Equal(t, map[string]string{"INIT_STATIC": "ready"}, init.Properties.Env)
	require.Len(t, init.Traits.EnvFrom, 1)
	require.Equal(t, spec.EnvFromSourceSpec{Type: "secret", SourceName: "init-secret"}, init.Traits.EnvFrom[0])
	require.NotNil(t, init.Traits.Resources)
	require.Equal(t, "50m", init.Traits.Resources.CPU)
	require.NotNil(t, init.Traits.SecurityPolicy)
	require.Equal(t, int64(0), *init.Traits.SecurityPolicy.RunAsUser)
	initStorage := findStorage(init.Traits.Storage, "init-work")
	require.NotNil(t, initStorage)
	require.Equal(t, config.StorageTypeEphemeral, initStorage.Type)
	require.Equal(t, "/work", initStorage.MountPath)

	require.Len(t, component.Traits.Sidecar, 1)
	sidecar := component.Traits.Sidecar[0]
	require.Equal(t, "helper-demo-sidecar-1", sidecar.Name)
	require.Equal(t, "helper:v1", sidecar.Image)
	require.Equal(t, []string{"helper"}, sidecar.Command)
	require.Equal(t, []string{"--serve"}, sidecar.Args)
	require.Equal(t, map[string]string{"SIDE_STATIC": "enabled"}, sidecar.Env)
	require.Len(t, sidecar.Traits.EnvFrom, 1)
	require.Equal(t, spec.EnvFromSourceSpec{Type: "configMap", SourceName: "sidecar-env"}, sidecar.Traits.EnvFrom[0])
	require.Len(t, sidecar.Traits.Envs, 1)
	require.Equal(t, "SIDE_SECRET", sidecar.Traits.Envs[0].Name)
	require.NotNil(t, sidecar.Traits.Envs[0].ValueFrom.Secret)
	require.Equal(t, "sidecar-secret", sidecar.Traits.Envs[0].ValueFrom.Secret.Name)
	require.Equal(t, "token", sidecar.Traits.Envs[0].ValueFrom.Secret.Key)
	require.NotNil(t, sidecar.Traits.Resources)
	require.Empty(t, sidecar.Traits.Resources.Memory)
	require.Equal(t, "64Mi", sidecar.Traits.Resources.MemoryLimit)
	require.NotNil(t, sidecar.Traits.SecurityPolicy)
	require.NotNil(t, sidecar.Traits.SecurityPolicy.AllowPrivilegeEscalation)
	require.False(t, *sidecar.Traits.SecurityPolicy.AllowPrivilegeEscalation)
	sidecarStorage := findStorage(sidecar.Traits.Storage, "sidecar-conf")
	require.NotNil(t, sidecarStorage)
	require.Equal(t, config.StorageTypeConfig, sidecarStorage.Type)
	require.Equal(t, "sidecar-config", sidecarStorage.SourceName)
	require.True(t, sidecarStorage.ReadOnly)
}

func TestConvertKubeResources_WarnsOnUnmatchedService(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      containers:
        - name: api
          image: api:v1
---
apiVersion: v1
kind: Service
metadata:
  name: orphan
spec:
  selector:
    app: missing
  ports:
    - port: 80
`
	svc := &conversionServiceImpl{
		ValidationService: NewValidationService(),
		Cfg:               &config.Config{AllowPrivateURLTargets: true},
	}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Warnings)
	require.Len(t, resp.Components, 1)
}

func TestConvertKubeResources_BestEffortWarningsDoNotFailConversion(t *testing.T) {
	yamlText := `
apiVersion: v1
kind: ConfigMap
metadata: {}
data:
  app.conf: "value"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: empty-worker
spec:
  selector:
    matchLabels:
      app: empty-worker
  template:
    metadata:
      labels:
        app: empty-worker
    spec: {}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      containers:
        - name: api
          image: api:v1
          env:
            - value: missing-name
            - name: POD_CPU_LIMIT
              valueFrom:
                resourceFieldRef:
                  resource: limits.cpu
          envFrom:
            - secretRef: {}
          volumeMounts:
            - name: missing
              mountPath: /missing
            - name: host
              mountPath: /host
      volumes:
        - name: host
          hostPath:
            path: /var/log
---
apiVersion: v1
kind: Service
metadata:
  name: no-selector
spec:
  ports:
    - port: 80
---
apiVersion: v1
kind: Service
metadata:
  name: orphan
spec:
  selector:
    app: missing
  ports:
    - port: 80
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: orphan-ingress
spec:
  rules:
    - host: orphan.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: orphan
                port:
                  number: 80
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: bad-binding
subjects:
  - kind: ServiceAccount
    name: api
`
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)
	require.Len(t, resp.Components, 1)
	require.Equal(t, "api", resp.Components[0].Name)

	require.Contains(t, resp.Warnings, "configmap missing metadata.name; skipped")
	require.Contains(t, resp.Warnings, "deployment empty-worker has no containers; skipped")
	require.Contains(t, resp.Warnings, "env name missing in component api; skipped")
	require.Contains(t, resp.Warnings, "env POD_CPU_LIMIT in component api uses unsupported valueFrom; skipped")
	require.Contains(t, resp.Warnings, "envFrom secret in component api missing name; skipped")
	require.Contains(t, resp.Warnings, "volume missing for container api in component api not found; skipped")
	require.Contains(t, resp.Warnings, "volume host for container api in component api uses unsupported source; skipped")
	require.Contains(t, resp.Warnings, "service no-selector has no selector; skipped")
	require.Contains(t, resp.Warnings, "service orphan has no matching workload; skipped")
	require.Contains(t, resp.Warnings, "ingress orphan-ingress has no matching component; skipped")
	require.Contains(t, resp.Warnings, "rolebinding bad-binding missing roleRef; skipped")
}

func TestConvertKubeResources_UsesFileURL(t *testing.T) {
	yamlText := `
apiVersion: v1
kind: ConfigMap
metadata:
  name: from-url
data:
  app.conf: "value"
`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(yamlText))
	}))
	defer server.Close()

	svc := &conversionServiceImpl{
		ValidationService:         NewValidationService(),
		URLSecurityPolicyProvider: newTestURLSecurityPolicyProvider(t, spec.URLSecurityPolicySpec{AllowPrivateByDefault: true}),
	}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{
		YAML:    "kind: ConfigMap\napiVersion: v1\nmetadata:\n  name: local",
		FileURL: server.URL,
	})
	require.NoError(t, err)
	require.Len(t, resp.Components, 1)
	require.Equal(t, "from-url", resp.Components[0].Name)
}

func TestConvertKubeResources_FileURLRejectsPrivateByDefault(t *testing.T) {
	yamlText := `
apiVersion: v1
kind: ConfigMap
metadata:
  name: from-url
data:
  app.conf: "value"
`
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(yamlText))
	}))
	defer server.Close()

	svc := &conversionServiceImpl{
		ValidationService:         NewValidationService(),
		URLSecurityPolicyProvider: newTestURLSecurityPolicyProvider(t, spec.DefaultURLSecurityPolicy()),
	}
	_, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{
		FileURL: server.URL,
	})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestConvertKubeResources_FileURLFailsWhenURLSecurityPolicyUnavailable(t *testing.T) {
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}

	_, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{
		FileURL: "https://example.com/app.yaml",
	})

	require.ErrorIs(t, err, bcode.ErrURLSecurityPolicyUnavailable)
}

func TestConvertKubeResources_InvalidFileURLScheme(t *testing.T) {
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	_, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{
		FileURL: "ftp://example.com/file.yaml",
	})
	require.Error(t, err)
}

func TestConvertKubeResources_FileURLSizeLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(make([]byte, utils.ConvertYAMLMaxSize+1))
	}))
	defer server.Close()

	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	_, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{
		FileURL: server.URL,
	})
	require.Error(t, err)
}

func TestConvertKubeResources_InlineYAMLSizeLimit(t *testing.T) {
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	_, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{
		YAML: strings.Repeat("a", utils.ConvertYAMLMaxSize+1),
	})
	require.ErrorIs(t, err, bcode.ErrApplicationConfig)
}

func TestConvertKubeResources_IngressByName(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      containers:
        - name: api
          image: api:v1
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: api
spec:
  rules:
    - host: example.com
      http:
        paths:
          - path: /healthz
            pathType: Prefix
            backend:
              service:
                name: api-svc
                port:
                  number: 80
`
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	api := findComponent(resp.Components, "api")
	require.NotNil(t, api)
	require.Len(t, api.Traits.Ingress, 1)
	require.Equal(t, "api", api.Traits.Ingress[0].Name)
	require.Len(t, api.Traits.Ingress[0].Routes, 1)
	require.Equal(t, "/healthz", api.Traits.Ingress[0].Routes[0].Path)
	require.Equal(t, "example.com", api.Traits.Ingress[0].Routes[0].Host)
	require.Equal(t, "api-svc", api.Traits.Ingress[0].Routes[0].Backend.ServiceName)
}

func TestConvertKubeResources_RBACSharedServiceAccount(t *testing.T) {
	yamlText := `
apiVersion: v1
kind: ServiceAccount
metadata:
  name: app-sa
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: app-role
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: app-binding
subjects:
  - kind: ServiceAccount
    name: app-sa
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: app-role
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
spec:
  replicas: 1
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      serviceAccountName: app-sa
      containers:
        - name: api
          image: api:v1
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: worker
spec:
  replicas: 1
  selector:
    matchLabels:
      app: worker
  template:
    metadata:
      labels:
        app: worker
    spec:
      serviceAccountName: app-sa
      containers:
        - name: worker
          image: worker:v1
`
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)

	api := findComponent(resp.Components, "api")
	require.NotNil(t, api)
	require.Len(t, api.Traits.RBAC, 1)
	policy := api.Traits.RBAC[0]
	require.Equal(t, "app-sa", policy.ServiceAccount)
	require.NotNil(t, policy.ServiceAccountLabels)
	require.Equal(t, "app-sa", policy.ServiceAccountLabels[config.LabelShareName])
	require.Equal(t, string(spec.ShareStrategyDefault), policy.ServiceAccountLabels[config.LabelShareStrategy])
	require.NotNil(t, policy.RoleLabels)
	require.Equal(t, "app-role", policy.RoleLabels[config.LabelShareName])
	require.Equal(t, string(spec.ShareStrategyDefault), policy.RoleLabels[config.LabelShareStrategy])
	require.NotNil(t, policy.BindingLabels)
	require.Equal(t, "app-binding", policy.BindingLabels[config.LabelShareName])
	require.Equal(t, string(spec.ShareStrategyDefault), policy.BindingLabels[config.LabelShareStrategy])

	worker := findComponent(resp.Components, "worker")
	require.NotNil(t, worker)
	require.Len(t, worker.Traits.RBAC, 1)
	require.Equal(t, "app-sa", worker.Traits.RBAC[0].ServiceAccount)
}

func TestConvertKubeResources_ResourcesSharePVC(t *testing.T) {
	yamlText := `
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 2Gi
  storageClassName: fast
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api
  labels:
    eruun.io/share-strategy: ignore
spec:
  replicas: 1
  selector:
    matchLabels:
      app: api
  template:
    metadata:
      labels:
        app: api
    spec:
      containers:
        - name: api
          image: api:v1
          resources:
            limits:
              cpu: "500m"
              memory: "256Mi"
          volumeMounts:
            - name: data
              mountPath: /data
      volumes:
        - name: data
          persistentVolumeClaim:
            claimName: data
`
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)

	api := findComponent(resp.Components, "api")
	require.NotNil(t, api)
	require.NotNil(t, api.Traits.Resources)
	require.Empty(t, api.Traits.Resources.CPU)
	require.Empty(t, api.Traits.Resources.Memory)
	require.Equal(t, "500m", api.Traits.Resources.CPULimit)
	require.Equal(t, "256Mi", api.Traits.Resources.MemoryLimit)
	require.NotNil(t, api.Traits.Share)
	require.Equal(t, string(spec.ShareStrategyIgnore), api.Traits.Share.Strategy)

	storage := findStorage(api.Traits.Storage, "data")
	require.NotNil(t, storage)
	require.Equal(t, "2Gi", storage.Size)
	require.Equal(t, "fast", storage.StorageClass)
}

func TestConvertKubeResources_JobAndCronJob(t *testing.T) {
	yamlText := `
apiVersion: batch/v1
kind: Job
metadata:
  name: once-job
  annotations:
    eruun.job/runPolicy: Recreate
    eruun.job/startTime: "1710000000"
spec:
  template:
    metadata:
      labels:
        app: once-job
    spec:
      containers:
        - name: once
          image: busybox:1.36
          command: ["sh","-c","echo ok"]
      restartPolicy: Never
---
apiVersion: batch/v1
kind: CronJob
metadata:
  name: cron-job
spec:
  schedule: "*/5 * * * *"
  successfulJobsHistoryLimit: 2
  failedJobsHistoryLimit: 1
  jobTemplate:
    spec:
      template:
        metadata:
          labels:
            app: cron-job
        spec:
          containers:
            - name: cron
              image: busybox:1.36
              command: ["sh","-c","echo hi"]
          restartPolicy: Never
`
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)

	once := findComponent(resp.Components, "once-job")
	require.NotNil(t, once)
	require.Equal(t, config.InstantJob, once.ComponentType)
	require.Equal(t, "busybox:1.36", once.Image)
	require.Equal(t, "recreate", once.Properties.RunPolicy)
	require.Equal(t, int64(1710000000), once.Properties.StartTime)

	cron := findComponent(resp.Components, "cron-job")
	require.NotNil(t, cron)
	require.Equal(t, config.ScheduledJob, cron.ComponentType)
	require.Equal(t, "busybox:1.36", cron.Image)
	require.Equal(t, "*/5 * * * *", cron.Properties.Schedule)
	if cron.Properties.SuccessfulJobsHistoryLimit != nil {
		require.Equal(t, int32(2), *cron.Properties.SuccessfulJobsHistoryLimit)
	}
	if cron.Properties.FailedJobsHistoryLimit != nil {
		require.Equal(t, int32(1), *cron.Properties.FailedJobsHistoryLimit)
	}
}

func TestConvertKubeResources_TargetWorkEnv(t *testing.T) {
	yamlText := `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: backend
spec:
  replicas: 1
  selector:
    matchLabels:
      app: backend
  template:
    metadata:
      labels:
        app: backend
    spec:
      nodeSelector:
        app: lab
      containers:
        - name: backend
          image: nginx:1.27
          ports:
            - containerPort: 8080
`
	svc := &conversionServiceImpl{ValidationService: NewValidationService()}
	resp, err := svc.ConvertKubeResources(context.Background(), apis.ConvertApplicationsRequest{YAML: yamlText})
	require.NoError(t, err)
	require.True(t, resp.Valid)
	require.Empty(t, resp.Errors)
	require.Len(t, resp.Components, 1)

	backend := findComponent(resp.Components, "backend")
	require.NotNil(t, backend)
	require.Equal(t, map[string]string{"app": "lab"}, backend.Traits.TargetWorkEnv)
}

func findComponent(components []apis.CreateComponentRequest, name string) *apis.CreateComponentRequest {
	for i := range components {
		if components[i].Name == name {
			return &components[i]
		}
	}
	return nil
}

func findStorage(storages []spec.StorageTraitSpec, name string) *spec.StorageTraitSpec {
	for i := range storages {
		if storages[i].Name == name {
			return &storages[i]
		}
	}
	return nil
}

func findServiceTrait(services []spec.ServiceTraitSpec, name string) *spec.ServiceTraitSpec {
	for i := range services {
		if services[i].Name == name {
			return &services[i]
		}
	}
	return nil
}
