package cebench

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"go.uber.org/atomic"
	"time"
)

type SQLContainer interface {
	SQL() string
}

type PlainSQL string

func (s PlainSQL) SQL() string {
	return string(s)
}

type QueryTask struct {
	payload SQLContainer
	dest    chan<- *QueryResult
	finish  chan<- struct{}
	exited  bool
}

type QueryResult struct {
	payload SQLContainer
	result  [][]interface{}
}

func StartQueryRunner(dsn string, inChan chan *QueryTask, concurrency, nTaskSender, dsnID uint) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errors.Trace(err)
	}
	if err := db.Ping(); err != nil {
		return errors.Trace(err)
	}
	for i := uint(0); i < concurrency; i++ {
		go queryRunner(db, inChan, nTaskSender, dsnID, i)
	}
	return nil
}

var nExitedTaskSender atomic.Uint64

func queryRunner(db *sql.DB, inChan chan *QueryTask, nTaskSender, dsnID, runnerID uint) {
	for task := range inChan {
		// This task sender has exited, so there will be no more tasks sent from the sender and no more results to the dest.
		if task.exited {
			nAfterInc := nExitedTaskSender.Inc()
			close(task.dest)
			// All task senders have exited, there will not be more tasks, so close the inChan and exit.
			// This should only run once among all query runners.
			if nAfterInc == uint64(nTaskSender) {
				close(inChan)
				break
			}
			continue
		}
		// Run the SQL.
		pl := task.payload
		sqlStr := pl.SQL()
		begin := time.Now()
		rows, err := db.Query(sqlStr)
		if time.Since(begin) > time.Second*3 {
			fmt.Printf("[%s] [SLOW-QUERY] Time cost: %v. SQL: %s\n", logTime(), time.Since(begin), sqlStr)
		}
		if err != nil {
			// TODO
			panic(err)
		}
		colNames, err := rows.Columns()
		if err != nil {
			// TODO
			panic(err)
		}
		nCols := len(colNames)
		res := make([][]interface{}, 0, 1)
		for rows.Next() {
			rowContainer := make([]interface{}, nCols)
			args := make([]interface{}, nCols)
			for i := range rowContainer {
				args[i] = &rowContainer[i]
			}
			if err = rows.Scan(args...); err != nil {
				return
			}
			res = append(res, rowContainer)
		}
		if err = rows.Close(); err != nil {
			//TODO
			panic(err)
		}

		// Send the query result.
		task.dest <- &QueryResult{task.payload, res}

		// Notify that this task has completed.
		if task.finish != nil {
			task.finish <- struct{}{}
		}
	}
	fmt.Printf("[%s] Query runner %d#%d exited.\n", logTime(), dsnID, runnerID)
}
