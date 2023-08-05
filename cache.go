package propagation_tx

import (
	"gorm.io/gorm"
	"sync"
)

type dbCache struct {
	dbConns map[string]map[string]*gorm.DB
	sync.RWMutex
}

func (d *dbCache) Get(source string, key string) *gorm.DB {
	d.RLock()
	defer d.RUnlock()
	if conns, exist := d.dbConns[source]; exist {
		if conn, ok := conns[key]; ok {
			return conn
		}
	}
	return nil
}

func (d *dbCache) Set(source, key string, db *gorm.DB) {
	d.Lock()
	defer d.Unlock()
	conns, exist := d.dbConns[source]
	if !exist {
		conns = make(map[string]*gorm.DB)
		d.dbConns[source] = conns
	}
	conns[key] = db
}

func (d *dbCache) GetOrCreate(source, key string, createFunc func() (*gorm.DB, error)) (*gorm.DB, error) {
	d.Lock()
	defer d.Unlock()
	conns, exist := d.dbConns[source]
	if !exist {
		conns = make(map[string]*gorm.DB)
		d.dbConns[source] = conns
	}

	conn, exist := conns[key]
	var err error = nil

	if !exist {
		if conn, err = createFunc(); err == nil {
			conns[key] = conn
		}
	}

	return conn, err
}
