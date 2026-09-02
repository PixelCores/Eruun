# MySQL 与 Redis 模板示例

> 状态：Current。本文提供模板实例化请求示例，行为以当前模板克隆与初始化环境变量代码为准。

本文档给出四个完整请求体：

- 创建一个可被 `tmp.id` 引用的 MySQL 模板应用。
- 基于该模板创建一个新 MySQL 实例，并覆盖 `init-mysql` 的环境变量。
- 创建一个可被 `tmp.id` 引用的 Redis 模板应用。
- 基于该模板创建一个新 Redis 实例，并覆盖 Redis Secret。

关键规则：

- `init` 容器的 `image` 必须写在 `traits.init[].image`，不能写在 `traits.init[].properties.image`。
- 主容器 `properties.env` 不会自动共享给 init 容器。`init-mysql` 脚本需要的变量必须写在 `traits.init[].properties.env`，或通过该 init 容器自己的 `traits.envs` / `envFrom` 注入。
- Redis 的 Secret 和 ConfigMap 引用会在模板实例化时按目标组件名重写；Redis 是否启用密码取决于镜像是否读取 `REDIS_PASSWORD`。

## 创建 MySQL 模板应用

保存为 `mysql-template.json` 后执行：

```bash
curl -sS -X POST "${ERUUN_API_URL:-http://127.0.0.1:8000}/api/v1/applications" \
  -H "Content-Type: application/json" \
  --data-binary -template.json
```

```json
{
  "name": "mysql",
  "alias": "mysql",
  "version": "5.7.2",
  "project": "Mysql 5.7.2",
  "description": "Mysql:5.7.2",
  "component": [
    {
      "name": "mysql-config",
      "type": "config",
      "properties": {
        "conf": {
          "master.cnf": "[mysqld]\nuser=mysql\nport=3306\nskip-name-resolve\ndefault_storage_engine=InnoDB\ncharacter_set_server=utf8mb4\ncollation_server=utf8mb4_unicode_ci\n\nmax_connections=1000\nmax_allowed_packet=32M\nopen_files_limit=65535\n\ntable_open_cache=200\ntable_definition_cache=100\n\ninnodb_buffer_pool_size=256M\ninnodb_buffer_pool_instances=1\ninnodb_log_file_size=64M\ninnodb_log_buffer_size=8M\ninnodb_flush_log_at_trx_commit=2\ninnodb_flush_method=O_DIRECT\ninnodb_thread_concurrency=0\ninnodb_read_io_threads=2\ninnodb_write_io_threads=2\n\ninnodb_flush_neighbors=0\ninnodb_io_capacity=2000\ninnodb_io_capacity_max=4000\n\ntmp_table_size=32M\nmax_heap_table_size=32M\njoin_buffer_size=1M\nsort_buffer_size=1M\nread_buffer_size=1M\nread_rnd_buffer_size=1M\n\ngtid_mode=on\nenforce_gtid_consistency=1\n\nlog_bin=mysql-bin\nlog-bin-index=mysql-bin.index\nbinlog_format=ROW\nsync_binlog=0\nlog_slave_updates=1\nbinlog_cache_size=1M\nexpire_logs_days=3\nbinlog_row_image=FULL\n\nplugin_load=\"rpl_semi_sync_master=semisync_master.so;rpl_semi_sync_slave=semisync_slave.so\"\nloose_rpl_semi_sync_master_enabled=1\nloose_rpl_semi_sync_slave_enabled=1\nloose_rpl_semi_sync_master_timeout=5000\n\nslow_query_log=1\nslow_query_log_file=/var/lib/mysql/mysql-slow.log\nlong_query_time=1\n\nlog_error=/var/lib/mysql/mysql-error.log\n\nquery_cache_type=0\nquery_cache_size=0\n",
          "slave.cnf": "[mysqld]\nuser=mysql\nport=3306\nskip-name-resolve\ndefault_storage_engine=InnoDB\ncharacter_set_server=utf8mb4\ncollation_server=utf8mb4_unicode_ci\n\nmax_connections=1000\nmax_allowed_packet=16M\nopen_files_limit=65535\n\ntable_open_cache=200\ntable_definition_cache=100\n\ninnodb_buffer_pool_size=256M\ninnodb_buffer_pool_instances=1\ninnodb_log_file_size=64M\ninnodb_log_buffer_size=8M\ninnodb_flush_log_at_trx_commit=2\ninnodb_flush_method=O_DIRECT\ninnodb_thread_concurrency=0\ninnodb_read_io_threads=2\ninnodb_write_io_threads=2\n\ntmp_table_size=32M\nmax_heap_table_size=32M\njoin_buffer_size=1M\nsort_buffer_size=1M\nread_buffer_size=1M\nread_rnd_buffer_size=1M\n\ngtid_mode=on\nenforce_gtid_consistency=1\n\nrelay-log=relay-bin\nrelay-log-index=relay-bin.index\nlog_slave_updates=1\nbinlog_cache_size=3M\nbinlog_format=row\nexpire_logs_days=3\nread_only=1\nsuper_read_only=1\nbinlog_row_image=FULL\n\nslave-parallel-type=LOGICAL_CLOCK\nslave-parallel-workers=2\nmaster_info_repository=TABLE\nrelay_log_info_repository=TABLE\nrelay_log_recovery=ON\n\nslow_query_log=1\nslow_query_log_file=/var/lib/mysql/mysql-slow.log\nlong_query_time=1\nlog_error=/var/lib/mysql/mysql-error.log\n\nquery_cache_type=0\nquery_cache_size=0\n"
        }
      }
    },
    {
      "name": "mysql-secret",
      "type": "secret",
      "properties": {
        "secret": {
          "MYSQL_ROOT_PASSWORD": "__REPLACE_WITH_SECRET__"
        }
      }
    },
    {
      "name": "mysql",
      "type": "store",
      "replicas": 1,
      "image": "mysql:8.0.37",
      "properties": {
        "ports": [
          {
            "port": 3306
          }
        ],
        "env": {
          "MYSQL_DATABASE": "game",
          "TZ": "Asia/Bangkok"
        },
        "labels": {
          "name": "mysql"
        }
      },
      "traits": {
        "resources": {
          "cpu": "300m",
          "memory": "600Mi"
        },
        "service": [
          {
            "name": "mysql-master",
            "type": "internal",
            "labels": {
              "layer": "db",
              "name": "mysql-master"
            },
            "selector": {
              "mysql-pod-role": "mysql-master"
            },
            "ports": [
              {
                "name": "mysql",
                "port": 3306,
                "targetPort": 3306,
                "protocol": "TCP"
              }
            ]
          },
          {
            "name": "mysql-slave",
            "type": "internal",
            "labels": {
              "layer": "db",
              "name": "mysql-slave"
            },
            "selector": {
              "mysql-pod-role": "mysql-slave"
            },
            "ports": [
              {
                "name": "mysql",
                "port": 3306,
                "targetPort": 3306,
                "protocol": "TCP"
              }
            ]
          },
          {
            "name": "mysql",
            "type": "internal",
            "headless": true,
            "labels": {
              "layer": "db",
              "name": "mysql"
            },
            "selector": {
              "name": "mysql"
            },
            "ports": [
              {
                "name": "mysql",
                "port": 3306,
                "targetPort": 3306,
                "protocol": "TCP"
              }
            ]
          }
        ],
        "envs": [
          {
            "name": "MYSQL_ROOT_PASSWORD",
            "valueFrom": {
              "secret": {
                "name": "mysql-secret",
                "key": "MYSQL_ROOT_PASSWORD"
              }
            }
          }
        ],
        "probes": [
          {
            "type": "liveness",
            "initialDelaySeconds": 30,
            "periodSeconds": 10,
            "timeoutSeconds": 5,
            "failureThreshold": 3,
            "successThreshold": 1,
            "exec": {
              "command": [
                "sh",
                "-c",
                "mysqladmin ping -h127.0.0.1 -uroot -p\"${MYSQL_ROOT_PASSWORD}\""
              ]
            }
          },
          {
            "type": "readiness",
            "initialDelaySeconds": 5,
            "periodSeconds": 2,
            "timeoutSeconds": 1,
            "failureThreshold": 3,
            "successThreshold": 1,
            "exec": {
              "command": [
                "sh",
                "-c",
                "mysqladmin ping -h127.0.0.1 -uroot -p\"${MYSQL_ROOT_PASSWORD}\""
              ]
            }
          }
        ],
        "storage": [
          {
            "name": "data",
            "type": "persistent",
            "mountPath": "/var/lib/mysql",
            "subPath": "mysql",
            "tmpCreate": true,
            "size": "30Gi"
          },
          {
            "name": "conf",
            "type": "ephemeral",
            "mountPath": "/etc/mysql/conf.d"
          },
          {
            "name": "init-scripts",
            "type": "ephemeral",
            "mountPath": "/docker-entrypoint-initdb.d"
          }
        ],
        "sidecar": [
          {
            "name": "xtrabackup",
            "image": "percona/percona-xtrabackup:8.0",
            "command": [
              "bash",
              "-c",
              "set -ex\n[[ $HOSTNAME =~ ^(.*?)-([0-9]+)$ ]] || exit 1\nprefix_name=${BASH_REMATCH[1]}\n\ncd /var/lib/mysql\n\nif [[ -f xtrabackup_slave_info ]]; then\n  mv xtrabackup_slave_info change_gtid.sql.in\n  rm -f xtrabackup_binlog_info\nelif [[ -f xtrabackup_binlog_info ]]; then\n  [[ $(cat xtrabackup_binlog_info) =~ ^(.*?)[[:space:]]+(.*?)[[:space:]]+(.*)$ ]] || exit 1\n  rm -f xtrabackup_binlog_info\n  echo \"SET GLOBAL gtid_purged='${BASH_REMATCH[3]}';\" > change_gtid.sql.in\nfi\n\nif [[ -f change_gtid.sql.in ]]; then\n  echo \"Waiting for mysqld to be ready (accepting connections)\"\n  until mysql -h 127.0.0.1 -uroot -p${MYSQL_ROOT_PASSWORD} -e \"SELECT 1\"; do sleep 1; done\n\n  echo \"Initializing replication with GTID\"\n  mv change_gtid.sql.in change_gtid.sql.orig\n  set_gtid_purged=$(grep \"gtid_purged=\" change_gtid.sql.orig)\n  mysql -h 127.0.0.1 -uroot -p${MYSQL_ROOT_PASSWORD} <<EOF\nSTOP SLAVE;\nRESET SLAVE ALL;\nRESET MASTER;\n${set_gtid_purged};\nCHANGE MASTER TO\n  MASTER_HOST='${prefix_name}-0.${prefix_name}',\n  MASTER_PORT=3306,\n  MASTER_USER='root',\n  MASTER_PASSWORD='${MYSQL_ROOT_PASSWORD}',\n  MASTER_AUTO_POSITION=1;\nSTART SLAVE;\nEOF\nfi\n\nexec ncat --listen --keep-open --send-only --max-conns=1 3307 -c \"xtrabackup --backup --slave-info --galera-info --safe-slave-backup --stream=xbstream --host=127.0.0.1 --user=root --password=${MYSQL_ROOT_PASSWORD}\"\n"
            ],
            "traits": {
              "envs": [
                {
                  "name": "MYSQL_ROOT_PASSWORD",
                  "valueFrom": {
                    "secret": {
                      "name": "mysql-secret",
                      "key": "MYSQL_ROOT_PASSWORD"
                    }
                  }
                }
              ],
              "storage": [
                {
                  "name": "data",
                  "type": "persistent",
                  "mountPath": "/var/lib/mysql",
                  "subPath": "mysql"
                },
                {
                  "name": "conf",
                  "type": "ephemeral",
                  "mountPath": "/etc/mysql/conf.d"
                }
              ]
            }
          }
        ],
        "init": [
          {
            "name": "init-mysql",
            "image": "bitnami/kubectl:1.28.5",
            "properties": {
              "env": {
                "MYSQL_DATABASE": "game",
                "SQL_URL": "https://paas-3os.oss-cn-shanghai.aliyuncs.com/uploads/2025/06/27/2506271630choUDT.sql",
                "MASTER_ROLE_NAME": "mysql-master",
                "SLAVE_ROLE_NAME": "mysql-slave",
                "MEMORY_REQUESTS": "260Mi",
                "CPU_REQUESTS": "160m"
              },
              "command": [
                "bash",
                "-c",
                "set -ex\n[[ $HOSTNAME =~ ^(.*?)-([0-9]+)$ ]] || exit 1\nprefix_name=${BASH_REMATCH[1]}\nordinal=${BASH_REMATCH[2]}\necho [mysqld] > /mnt/conf.d/server-id.cnf\necho server-id=$((100 + $ordinal)) >> /mnt/conf.d/server-id.cnf\nif [[ ${ordinal} -eq 0 ]]; then\n  cp /mnt/config-map/master.cnf /mnt/conf.d\n  kubectl label pod $HOSTNAME mysql-pod-role=$MASTER_ROLE_NAME --namespace $POD_NAMESPACE --overwrite\nelse\n  cp /mnt/config-map/slave.cnf /mnt/conf.d\n  kubectl label pod $HOSTNAME mysql-pod-role=$SLAVE_ROLE_NAME --namespace $POD_NAMESPACE --overwrite\nfi\n\n[[ -d /var/lib/mysql/mysql ]] && exit 0\noutput_dir=/docker-entrypoint-initdb.d\necho \"use $MYSQL_DATABASE\" > $output_dir/00-init.sql\nfor i in $(seq 1 5); do\n  echo \"Downloading init script... attempt $i\"\n  curl -f --connect-timeout 10 --max-time 60 -o \"$output_dir/01-init.sql\" --retry 3 --retry-delay 5 \"$SQL_URL\" && break || sleep 5\ndone\n[ -f \"$output_dir/01-init.sql\" ] || { echo \"download failed\"; exit 1; }\n"
              ]
            },
            "traits": {
              "envs": [
                {
                  "name": "MYSQL_ROOT_PASSWORD",
                  "valueFrom": {
                    "secret": {
                      "name": "mysql-secret",
                      "key": "MYSQL_ROOT_PASSWORD"
                    }
                  }
                },
                {
                  "name": "POD_NAMESPACE",
                  "valueFrom": {
                    "field": "metadata.namespace"
                  }
                }
              ],
              "storage": [
                {
                  "name": "conf",
                  "type": "ephemeral",
                  "mountPath": "/mnt/conf.d"
                },
                {
                  "name": "config-map",
                  "type": "config",
                  "mountPath": "/mnt/config-map",
                  "sourceName": "mysql-config"
                },
                {
                  "name": "init-scripts",
                  "type": "ephemeral",
                  "mountPath": "/docker-entrypoint-initdb.d"
                }
              ],
              "securityPolicy": {
                "allowPrivilegeEscalation": false,
                "readOnlyRootFilesystem": true,
                "capabilities": {
                  "drop": [
                    "ALL"
                  ]
                }
              }
            }
          },
          {
            "name": "clone-mysql",
            "image": "percona/percona-xtrabackup:8.0",
            "properties": {
              "command": [
                "bash",
                "-c",
                "set -ex\n[[ -d /var/lib/mysql/mysql ]] && exit 0\n[[ $HOSTNAME =~ ^(.*?)-([0-9]+)$ ]] || exit 1\nprefix_name=${BASH_REMATCH[1]}\nordinal=${BASH_REMATCH[2]}\n[[ $ordinal == 0 ]] && exit 0\nncat --recv-only ${prefix_name}-$(($ordinal-1)).${prefix_name} 3307 | xbstream -x -C /var/lib/mysql\nxtrabackup --prepare --target-dir=/var/lib/mysql\n"
              ]
            },
            "traits": {
              "envs": [
                {
                  "name": "MYSQL_ROOT_PASSWORD",
                  "valueFrom": {
                    "secret": {
                      "name": "mysql-secret",
                      "key": "MYSQL_ROOT_PASSWORD"
                    }
                  }
                }
              ],
              "storage": [
                {
                  "name": "data",
                  "type": "persistent",
                  "mountPath": "/var/lib/mysql",
                  "subPath": "mysql"
                },
                {
                  "name": "conf",
                  "type": "ephemeral",
                  "mountPath": "/etc/mysql/conf.d"
                }
              ]
            }
          }
        ],
        "rbac": [
          {
            "serviceAccount": "pod-labeler-sa",
            "roleName": "pod-labeler-role",
            "bindingName": "pod-labeler-binding",
            "rules": [
              {
                "apiGroups": [
                  ""
                ],
                "resources": [
                  "pods"
                ],
                "verbs": [
                  "get",
                  "patch"
                ]
              }
            ],
            "roleLabels": {
              "app": "labeler"
            }
          }
        ]
      }
    }
  ],
  "templateEnabled": true
}
```

创建完成后获取模板 ID：

```bash
curl -sS "${ERUUN_API_URL:-http://127.0.0.1:8000}/api/v1/applications/templates"
```

## 基于模板创建实例并覆盖 init env

将 `<MYSQL_TEMPLATE_APP_ID>` 替换为上一步得到的模板应用 ID。保存为 `tenant-a-mysql.json` 后执行：

```bash
curl -sS -X POST "${ERUUN_API_URL:-http://127.0.0.1:8000}/api/v1/applications" \
  -H "Content-Type: application/json" \
  --data-binary -a-mysql.json
```

```json
{
  "name": "tenant-a-mysql-app",
  "namespace": "mysql",
  "alias": "tenant-a-mysql",
  "version": "1.0.7",
  "description": "mysql cloned from template",
  "component": [
    {
      "name": "tenant-a-secret",
      "type": "secret",
      "tmp": {
        "id": "<MYSQL_TEMPLATE_APP_ID>",
        "target": "mysql-secret"
      },
      "properties": {
        "secret": {
          "MYSQL_ROOT_PASSWORD": "__REPLACE_WITH_SECRET__"
        }
      }
    },
    {
      "name": "tenant-a-mysql",
      "type": "store",
      "tmp": {
        "id": "<MYSQL_TEMPLATE_APP_ID>",
        "target": "mysql"
      },
      "properties": {
        "env": {
          "MYSQL_DATABASE": "tenant_a_game",
          "TZ": "Asia/Shanghai"
        }
      },
      "traits": {
        "init": [
          {
            "name": "init-mysql",
            "properties": {
              "env": {
                "MYSQL_DATABASE": "tenant_a_game",
                "SQL_URL": "https://example.com/tenant-a-init.sql"
              }
            }
          }
        ]
      }
    }
  ]
}
```

上面的覆盖规则是：

- `properties.env.MYSQL_DATABASE` 覆盖主 MySQL 容器的数据库名。
- `traits.init[].properties.env.MYSQL_DATABASE` 覆盖 `init-mysql` 中执行 `echo "use $MYSQL_DATABASE"` 时使用的数据库名。
- `traits.init[].properties.env.SQL_URL` 覆盖 `init-mysql` 下载初始化 SQL 时使用的地址。
- `traits.init[].name` 建议显式写为 `init-mysql`，这样覆盖会精确作用到模板中的同名 init 容器。

## 创建 Redis 模板应用

保存为 `redis-template.json` 后执行：

```bash
curl -sS -X POST "${ERUUN_API_URL:-http://127.0.0.1:8000}/api/v1/applications" \
  -H "Content-Type: application/json" \
  --data-binary -template.json
```

```json
{
  "name": "tmp-redis",
  "alias": "redis",
  "version": "6.2.17",
  "project": "redis 6.2.17",
  "description": "Redis 6.2.17",
  "component": [
    {
      "name": "tmp-redis-config",
      "type": "config",
      "properties": {
        "conf": {
          "redis.conf": "bind 0.0.0.0\nprotected-mode yes\nport 6379\ntcp-backlog 511\ntimeout 0\ntcp-keepalive 300\ndaemonize no\npidfile /var/run/redis_6379.pid\nloglevel notice\nlogfile \"\"\ndatabases 16\nalways-show-logo no\nset-proc-title yes\nproc-title-template \"{title} {listen-addr} {server-mode}\"\nstop-writes-on-bgsave-error yes\nrdbcompression yes\nrdbchecksum yes\ndbfilename dump.rdb\nrdb-del-sync-files no\ndir /data/\n\nuser default on\n\nreplica-serve-stale-data yes\nreplica-read-only yes\nrepl-diskless-sync yes\nrepl-diskless-sync-delay 5\nrepl-diskless-sync-max-replicas 0\nrepl-diskless-load disabled\nrepl-disable-tcp-nodelay no\nreplica-priority 100\nacllog-max-len 128\nlazyfree-lazy-eviction no\nlazyfree-lazy-expire no\nlazyfree-lazy-server-del no\nreplica-lazy-flush no\nlazyfree-lazy-user-del no\nlazyfree-lazy-user-flush no\noom-score-adj no\noom-score-adj-values 0 200 800\ndisable-thp yes\nappendonly no\nappendfilename \"appendonly.aof\"\nappenddirname \"appendonlydir\"\nappendfsync everysec\nno-appendfsync-on-rewrite no\nauto-aof-rewrite-percentage 100\nauto-aof-rewrite-min-size 64mb\naof-load-truncated yes\naof-use-rdb-preamble yes\naof-timestamp-enabled no\n\t\nslowlog-log-slower-than 10000\nslowlog-max-len 128\nlatency-monitor-threshold 0\nnotify-keyspace-events \"\"\nhash-max-listpack-entries 512\nhash-max-listpack-value 64\nlist-max-listpack-size -2\nlist-compress-depth 0\nset-max-intset-entries 512\nzset-max-listpack-entries 128\nzset-max-listpack-value 64\nhll-sparse-max-bytes 3000\nstream-node-max-bytes 4096\nstream-node-max-entries 100\nactiverehashing yes\nclient-output-buffer-limit normal 0 0 0\nclient-output-buffer-limit replica 256mb 64mb 60\nclient-output-buffer-limit pubsub 32mb 8mb 60\nhz 10\ndynamic-hz yes\naof-rewrite-incremental-fsync yes\nrdb-save-incremental-fsync yes\njemalloc-bg-thread yes\n"
        }
      }
    },
    {
      "name": "tmp-redis-secret",
      "type": "secret",
      "properties": {
        "secret": {
          "REDIS_DEFAULT_PASSWORD": "__REPLACE_WITH_SECRET__"
        }
      }
    },
    {
      "name": "redis",
      "type": "store",
      "image": "redis:7.2.4",
      "replicas": 1,
      "properties": {
        "ports": [
          {
            "port": 6379
          }
        ],
        "env": {
          "TZ": "Asia/Shanghai"
        }
      },
      "traits": {
        "envs": [
          {
            "name": "REDIS_PASSWORD",
            "valueFrom": {
              "secret": {
                "name": "tmp-redis-secret",
                "key": "REDIS_DEFAULT_PASSWORD"
              }
            }
          }
        ],
        "storage": [
          {
            "name": "v-redis-data",
            "type": "persistent",
            "mountPath": "/data",
            "tmpCreate": true,
            "size": "4Gi"
          },
          {
            "name": "v-redis-conf",
            "type": "config",
            "mountPath": "/usr/local/etc/redis/redis.conf",
            "subPath": "redis.conf",
            "readOnly": true,
            "sourceName": "tmp-redis-config"
          }
        ]
      }
    }
  ],
  "templateEnabled": true
}
```

创建完成后获取模板 ID：

```bash
curl -sS "${ERUUN_API_URL:-http://127.0.0.1:8000}/api/v1/applications/templates"
```

## 基于模板创建 Redis 实例

将 `<REDIS_TEMPLATE_APP_ID>` 替换为上一步得到的模板应用 ID。保存为 `tenant-a-redis.json` 后执行：

```bash
curl -sS -X POST "${ERUUN_API_URL:-http://127.0.0.1:8000}/api/v1/applications" \
  -H "Content-Type: application/json" \
  --data-binary -a-redis.json
```

```json
{
  "name": "tenant-a-redis-app",
  "namespace": "redis",
  "alias": "tenant-a-redis",
  "version": "1.0.0",
  "description": "redis cloned from template",
  "component": [
    {
      "name": "tenant-a-redis-config",
      "type": "config",
      "tmp": {
        "id": "<REDIS_TEMPLATE_APP_ID>",
        "target": "tmp-redis-config"
      }
    },
    {
      "name": "tenant-a-redis-secret",
      "type": "secret",
      "tmp": {
        "id": "<REDIS_TEMPLATE_APP_ID>",
        "target": "tmp-redis-secret"
      },
      "properties": {
        "secret": {
          "REDIS_DEFAULT_PASSWORD": "__REPLACE_WITH_SECRET__"
        }
      }
    },
    {
      "name": "tenant-a-redis",
      "type": "store",
      "tmp": {
        "id": "<REDIS_TEMPLATE_APP_ID>",
        "target": "redis"
      },
      "properties": {
        "env": {
          "TZ": "Asia/Shanghai"
        }
      }
    }
  ]
}
```

上面的覆盖规则是：

- `tmp.target: "tmp-redis-config"` 克隆模板中的 Redis ConfigMap。
- `tmp.target: "tmp-redis-secret"` 克隆模板中的 Redis Secret，并用请求侧 `properties.secret.REDIS_DEFAULT_PASSWORD` 覆盖模板密码。
- `tmp.target: "redis"` 克隆模板中的 Redis store 组件。
- store 组件中 `traits.envs[].valueFrom.secret.name` 和 `traits.storage[].sourceName` 会被模板克隆逻辑重写到新的 Secret / ConfigMap 名称。
- 如果需要改变 Redis 配置内容，可以在 `tenant-a-redis-config.properties.conf.redis.conf` 中提供新的配置内容。
