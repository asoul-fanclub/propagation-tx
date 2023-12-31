package sql

import (
	"context"
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"log"
	"strconv"
	"time"
)

var cache = dbCache{dbConns: make(map[string]map[string]*gorm.DB)}

// DBCreator create db object
type DBCreator interface {
	CreateDB() (*gorm.DB, error)
}

// CacheableDBCreator is cacheable DBCreator
type CacheableDBCreator interface {
	DBCreator
	CacheKey() string
	// CacheSource return user define key to separate the kind of creator
	CacheSource() string
}

// configDBCreator create db by config
type configDBCreator struct {
	config *ConnConfig
}

func (c *configDBCreator) CreateDB() (*gorm.DB, error) {
	return createDB(c.config)
}

func (c *configDBCreator) CacheKey() string {
	return c.config.Host + "#" + strconv.Itoa(c.config.Port) + "#" + c.config.Database
}

func (c *configDBCreator) CacheSource() string {
	return "config_db"
}

// simpleDBCreator create db by simple params
type simpleDBCreator struct {
	host     string
	port     int
	database string
	user     string
	password string
}

func (s *simpleDBCreator) CreateDB() (*gorm.DB, error) {
	connConfig := &ConnConfig{
		Host:     s.host,
		Port:     s.port,
		Database: s.database,
		User:     s.user,
		Password: s.password,
	}

	return createDB(connConfig)
}

func (s *simpleDBCreator) CacheKey() string {
	return s.host + "#" + strconv.Itoa(s.port) + "#" + s.database
}

func (s *simpleDBCreator) CacheSource() string {
	return "simple_db"
}

// GetSimpleDB get DB conn with specified context by some simple params
func GetSimpleDB(host string, port int, database string, user string, password string, ctx context.Context) (*gorm.DB, error) {
	factory, err := NewSimpleDBFactory(host, port, database, user, password)
	if err != nil {
		return nil, err
	}
	return factory.GetDB(ctx), nil
}

// GetConfigDB get DB conn with specified context by connConfig
func GetConfigDB(connConfig *ConnConfig, ctx context.Context) (*gorm.DB, error) {
	factory, err := NewConfigDBFactory(connConfig)
	if err != nil {
		return nil, err
	}
	return factory.GetDB(ctx), nil
}

// DBFactory get a db object with ctx
type DBFactory interface {
	// GetDB return gorm.DB with ctx
	GetDB(ctx context.Context) *gorm.DB
	// GetOriginDB return original gorm.DB object
	GetOriginDB() *gorm.DB
}

// GlobalCachedDBFactory implement DBFactory with internal cache
type GlobalCachedDBFactory struct {
	creator CacheableDBCreator
	db      *gorm.DB
}

func (g GlobalCachedDBFactory) GetDB(ctx context.Context) *gorm.DB {
	// TODO: Report
	return g.db.WithContext(ctx)
}

func (g GlobalCachedDBFactory) GetOriginDB() *gorm.DB {
	return g.db
}

// NewCachedDBFactory return a new DBFactory by a given CacheableDBCreator
func NewCachedDBFactory(creator CacheableDBCreator) (DBFactory, error) {
	source := creator.CacheSource()
	key := creator.CacheKey()

	db, err := cache.GetOrCreate(source, key, creator.CreateDB)
	if err != nil {
		return nil, err
	}
	return &GlobalCachedDBFactory{
		creator: creator,
		db:      db,
	}, nil
}

// NewSimpleDBFactory return a new DBFactory by some simple params
func NewSimpleDBFactory(host string, port int, database string, user string, password string) (DBFactory, error) {
	return NewCachedDBFactory(
		&simpleDBCreator{
			host:     host,
			port:     port,
			database: database,
			user:     user,
			password: password,
		})
}

func NewConfigDBFactory(connConfig *ConnConfig) (DBFactory, error) {
	return NewCachedDBFactory(&configDBCreator{config: connConfig})
}

func createDB(connConfig *ConnConfig) (*gorm.DB, error) {
	PatchDefaultConfig(connConfig)
	log.Println("[DB] create db")
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", connConfig.User, connConfig.Password, connConfig.Host, connConfig.Port, connConfig.Database)
	// TODO: support other type of db
	db, err := gorm.Open(mysql.Open(connStr))
	if err != nil {
		return nil, err
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalln("get sql db error: ", err)
	}
	sqlDB.SetMaxIdleConns(connConfig.MaxIdleConns)                                       // 打开空闲连接数
	sqlDB.SetMaxOpenConns(connConfig.MaxOpenConns)                                       // 最大打开连接数
	sqlDB.SetConnMaxLifetime(time.Duration(connConfig.ConnMaxLifetimeSec) * time.Second) // 连接可重用的最大时间长度，默认可一直复用
	// TODO: log

	// TODO：relevant metrics collection

	// TODO: trace
	return db, nil
}
