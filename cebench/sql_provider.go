package cebench

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

type originalSQL struct {
	sql     string
	noTrace bool
}

func (s *originalSQL) SQL() string {
	return s.sql
}

func SQLProvider(paths []string, queryTaskChan chan<- *QueryTask, destChan chan<- *QueryResult) {
	isTracePlanStmt := regexp.MustCompile("(?i)^trace plan")
	isSelectStmt := regexp.MustCompile("(?i)^select")
	isDropStmt := regexp.MustCompile("(?i)^drop")
	isCreateStmt := regexp.MustCompile("(?i)^create")
	finishChan := make(chan struct{}, 100)
	taskCnt := 0
	var lastPayloads []*originalSQL
	for _, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			// TODO
			panic(err)
		}
		scanner := bufio.NewScanner(file)
		onSemiColon := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
			for i := 0; i < len(data); i++ {
				if data[i] == ';' {
					return i + 1, data[:i], nil
				}
			}
			if !atEOF {
				return 0, nil, nil
			}
			return 0, data, bufio.ErrFinalToken
		}
		// TODO: splitting by semicolon is incorrect for some cases.
		scanner.Split(onSemiColon)
		for scanner.Scan() {
			sql := scanner.Text()
			sql = strings.TrimSpace(sql)
			if len(sql) == 0 {
				continue
			}
			payload := originalSQL{}
			needWait := false
			if isTracePlanStmt.MatchString(sql) {
				taskCnt++
				payload.sql = sql
			} else if isSelectStmt.MatchString(sql) {
				taskCnt++
				sql = "TRACE PLAN TARGET = 'estimation' " + sql
				payload.sql = sql
			} else if isDropStmt.MatchString(sql) {
				payload.sql = sql
				payload.noTrace = true
				lastPayloads = append(lastPayloads, &payload)
				continue
			} else if isCreateStmt.MatchString(sql) {
				payload.sql = sql
				payload.noTrace = true
				needWait = true
			} else {
				taskCnt++
				payload.sql = sql
				payload.noTrace = true
			}
			if needWait {
				tmpFinishChan := make(chan struct{}, 1)
				queryTaskChan <- &QueryTask{&payload, destChan, tmpFinishChan, false}

			} else {
				queryTaskChan <- &QueryTask{&payload, destChan, finishChan, false}
			}
		FORLOOP:
			for {
				select {
				case <-finishChan:
					taskCnt--
				default:
					break FORLOOP
				}
			}
		}
		if err = scanner.Err(); err != nil {
			// TODO
			panic(err)
		}
	}
	for _, payload := range lastPayloads {
		tmpFinishChan := make(chan struct{}, 1)
		queryTaskChan <- &QueryTask{payload, destChan, tmpFinishChan, false}
		<-tmpFinishChan
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
	fmt.Printf("[%s] SQL provider exited.\n", logTime())
}
