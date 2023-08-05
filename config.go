package propagation_tx

import "sync"

const DefaultGroup = "DEFAULT"

// ConnConfig is config for connected to db
type ConnConfig struct {
	Host               string `json:"host"`
	Port               int    `json:"port"`
	User               string `json:"user"`
	Password           string `json:"password"`
	Database           string `json:"database"`
	MaxIdleConns       int    `json:"maxIdleConns"`
	MaxOpenConns       int    `json:"maxOpenConns"`
	ConnMaxLifetimeSec int    `json:"connMaxLifetimeSec"`
	DbLog              bool   `json:"dbLog"`
	Dialect            string `json:"dialect"`
}

var connConfigMap = map[string]ConnConfig{}
var connConfigLock = sync.RWMutex{}
