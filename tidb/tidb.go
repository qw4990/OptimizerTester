package tidb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
)

type Option struct {
	Addr     string `toml:"addr"`
	Port     int    `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Label    string `toml:"label"`
}

type Instance interface {
	Exec(sql string) error
	MustExec(sql string)
	ExecInNewSession(sql string) error
	MustQuery(query string) *sql.Rows
	Query(query string) (*sql.Rows, error)
	Version() string
	Opt() Option
	Close() error
}

type instance struct {
	db  *sql.DB
	opt Option
	ver string
}

func (ins *instance) ExecInNewSession(sql string) error {
	begin := time.Now()
	c, err := ins.db.Conn(context.Background())
	if err != nil {
		return nil
	}
	defer c.Close()
	_, err = c.ExecContext(context.Background(), sql)
	if time.Since(begin) > time.Second*3 {
		fmt.Printf("[SLOW-QUERY] access %v with SQL %v cost %v\n", ins.opt.Label, sql, time.Since(begin))
	}
	return errors.Trace(err)
}

func (ins *instance) MustExec(sql string) {
	if err := ins.Exec(sql); err != nil {
		panic(err)
	}
}

func (ins *instance) Exec(sql string) error {
	begin := time.Now()
	_, err := ins.db.Exec(sql)
	name := ins.opt.Label
	if name == "" {
		name = fmt.Sprintf("%v:%v", ins.opt.Addr, ins.opt.Port)
	}
	if time.Since(begin) > time.Second*3 {
		fmt.Printf("[SLOW-QUERY] access %v with SQL %v cost %v\n", name, sql, time.Since(begin))
	}
	return errors.Trace(err)
}

func (ins *instance) MustQuery(query string) *sql.Rows {
	ret, err := ins.Query(query)
	if err != nil {
		panic(fmt.Sprintf("%v: %v", query, err))
	}
	return ret
}

func (ins *instance) Query(query string) (*sql.Rows, error) {
	begin := time.Now()
	rows, err := ins.db.Query(query)
	name := ins.opt.Label
	if name == "" {
		name = fmt.Sprintf("%v:%v", ins.opt.Addr, ins.opt.Port)
	}
	if time.Since(begin) > time.Second*3 {
		fmt.Printf("[SLOW-QUERY]access %v with SQL %v cost %v\n", name, query, time.Since(begin))
	}
	return rows, errors.Trace(err)
}

func (ins *instance) Version() string {
	return ins.ver
}

func (ins *instance) Opt() Option {
	return ins.opt
}

func (ins *instance) Close() error {
	return ins.db.Close()
}

func (ins *instance) initVersion() error {
	rows, err := ins.Query(`SELECT VERSION()`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var version string
	rows.Next()
	if err := rows.Scan(&version); err != nil {
		return err
	}
	tmp := strings.Split(version, "-")
	ins.ver = tmp[2]
	return nil
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
	dns := fmt.Sprintf("%s:%s@tcp(%s:%v)/%v", opt.User, opt.Password, opt.Addr, opt.Port, "mysql")
	if opt.Password == "" {
		dns = fmt.Sprintf("%s@tcp(%s:%v)/%v", opt.User, opt.Addr, opt.Port, "mysql")
	}
	db, err := sql.Open("mysql", dns)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := db.Ping(); err != nil {
		return nil, errors.Trace(err)
	}
	ins := &instance{db: db, opt: opt}
	db.SetMaxOpenConns(256)
	return ins, ins.initVersion()
}
