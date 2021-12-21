package cebench

import (
	"encoding/json"
	"fmt"
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

func TraceResultProvider(inChan <-chan *QueryResult, queryTaskChan chan<- *QueryTask, destChan chan<- *QueryResult) {
	finishChan := make(chan struct{}, 100)
	taskCnt := 0
FORLOOP:
	for {
		select {
		case <-finishChan:
			taskCnt--
		case tracePlanRes, ok := <-inChan:
			if !ok {
				break FORLOOP
			}
			source := tracePlanRes.payload.(*originalSQL)
			if source.noTrace {
				continue
			}
			ceTraceStr := tracePlanRes.result[0][0].([]byte)
			var records []*CETraceRecord
			err := json.Unmarshal(ceTraceStr, &records)
			if err != nil {
				// TODO
				panic(err)
			}
			for _, record := range records {
				taskCnt++
				queryTaskChan <- &QueryTask{record, destChan, finishChan, false}
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
	queryTaskChan <- &QueryTask{nil, destChan, nil, true}
	fmt.Printf("[%s] Trace result provider exited.\n", logTime())
}
