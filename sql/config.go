package sql

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

var DefaultConfig = ConnConfig{
	Port:               5432,
	MaxIdleConns:       5,
	MaxOpenConns:       20,
	ConnMaxLifetimeSec: 3600,
	DbLog:              false,
	Dialect:            "mysql",
}

func PatchDefaultConfig(config *ConnConfig) {
	if config.Port == 0 {
		config.Port = DefaultConfig.Port
	}
	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = DefaultConfig.MaxIdleConns
	}
	if config.MaxOpenConns == 0 {
		config.MaxOpenConns = DefaultConfig.MaxOpenConns
	}
	if config.ConnMaxLifetimeSec == 0 {
		config.ConnMaxLifetimeSec = DefaultConfig.ConnMaxLifetimeSec
	}
	if config.Dialect == "" {
		config.Dialect = DefaultConfig.Dialect
	}
	if config.Database == "" {
		panic("database must be set")
	}
}
