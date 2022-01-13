package cebench

import (
	"encoding/json"
	"fmt"
	"github.com/qw4990/OptimizerTester/tidb"
)

type CETraceRecord struct {
	TableName string `json:"table_name"`
	Type      string `json:"type"`
	Expr      string `json:"expr"`
	RowCount  uint64 `json:"row_count"`
}

func (record *CETraceRecord) SQL() string {
	return "SELECT COUNT(*) FROM " + record.TableName + " WHERE " + record.Expr
}

func TraceResultProvider(inChan <-chan *tidb.QueryResult, queryTaskChan chan<- *tidb.QueryTask, destChan chan<- *tidb.QueryResult) {
	finishChan := make(chan struct{}, 100)
	taskCnt := 0
	tasks := make([]*tidb.QueryTask, 0, 100)
FORLOOP:
	for {
		var nextTaskToSend *tidb.QueryTask
		var tmpQueryTaskChan chan<- *tidb.QueryTask
		if len(tasks) > 0 {
			nextTaskToSend = tasks[0]
			tmpQueryTaskChan = queryTaskChan
		}
		select {
		case <-finishChan:
			taskCnt--
		case tracePlanRes, ok := <-inChan:
			if !ok {
				inChan = nil
				continue
			}
			source := tracePlanRes.Payload.(*originalSQL)
			if source.noTrace {
				continue
			}
			ceTraceStr := tracePlanRes.Result[0][0].([]byte)
			var records []*CETraceRecord
			err := json.Unmarshal(ceTraceStr, &records)
			if err != nil {
				// TODO
				panic(err)
			}
			for _, record := range records {
				taskCnt++
				tasks = append(tasks, &tidb.QueryTask{record, destChan, finishChan, false})
			}
		case tmpQueryTaskChan <- nextTaskToSend:
			tasks = tasks[1:]
			if len(tasks) == 0 && inChan == nil {
				break FORLOOP
			}
		}
	}
	if taskCnt > 0 {
		for range finishChan {
			taskCnt--
			if taskCnt == 0 {
				break
			}
		}
	}
	queryTaskChan <- &tidb.QueryTask{nil, destChan, nil, true}
	fmt.Printf("[%s] Trace result provider exited.\n", logTime())
}
