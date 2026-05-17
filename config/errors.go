package config

import "errors"

var (
	errRedisHostOrCluster = errors.New("redis: host gerekli veya cluster/sentinel tanımlı olmalı")
	errTableMapping       = errors.New("redis.tableKeyMapping: her kayıt için table ve keyColumn zorunlu")
)
