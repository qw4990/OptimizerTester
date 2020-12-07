package tidb

import (
	"database/sql"
	"fmt"
	
	"github.com/pingcap/errors"
)

type Option struct {
	addr       string
	statusPort string
	port       string
	user       string
	password   string
	version    string
	defaultDB  string
}

type Instance interface {
	Exec(sql string) error
	Query(query string) (*sql.Rows, error)
}

type instance struct {
	db  *sql.DB
	opt Option
}

func (ins *instance) Exec(sql string) error {
	_, err := ins.db.Exec(sql)
	return errors.Trace(err)
}

func (ins *instance) Query(query string) (*sql.Rows, error) {
	rows, err := ins.db.Query(query)
	return rows, errors.Trace(err)
}

func ConnectTo(opt Option) (Instance, error) {
	dns := fmt.Sprintf("%s:%s@tcp(%s:%s)/%v", opt.user, opt.password, opt.addr, opt.port, opt.defaultDB)
	if opt.password == "" {
		dns = fmt.Sprintf("%s@tcp(%s:%s)/%v", opt.user, opt.addr, opt.port, opt.defaultDB)
	}
	db, err := sql.Open("mysql", dns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := db.Ping(); err != nil {
		return nil, errors.Trace(err)
	}
	return &instance{db, opt}, nil
}

func validateOpt(opt *Option) error {
	// TODO
	return nil
}
