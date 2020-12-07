package tidb

import (
	"database/sql"
	"fmt"

	"github.com/pingcap/errors"
)

type Option struct {
	Addr     string `toml:"addr"`
	Port     string `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Label    string `toml:"label"`
}

type Instance interface {
	Exec(sql string) error
	Query(query string) (*sql.Rows, error)
	Opt() Option
	Close() error
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

func (ins *instance) Opt() Option {
	return ins.opt
}

func (ins *instance) Close() error {
	return ins.db.Close()
}

func ConnectToInstances(opts []Option) (xs []Instance, err error) {
	xs = make([]Instance, 0, len(opts))
	defer func() {
		if err != nil {
			for _, x := range xs {
				x.Close()
			}
		}
	}()
	for _, opt := range opts {
		var ins Instance
		ins, err = ConnectTo(opt)
		if err != nil {
			return
		}
		xs = append(xs, ins)
	}
	return
}

func ConnectTo(opt Option) (Instance, error) {
	dns := fmt.Sprintf("%s:%s@tcp(%s:%s)/%v", opt.User, opt.Password, opt.Addr, opt.Port, "mysql")
	if opt.Password == "" {
		dns = fmt.Sprintf("%s@tcp(%s:%s)/%v", opt.User, opt.Addr, opt.Port, "mysql")
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
