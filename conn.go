package propagation_tx

import (
	"context"
	"gorm.io/gorm"
	"strconv"
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

func createDB(connConfig *ConnConfig) (*gorm.DB, error) {
	panic("implement me")
}
