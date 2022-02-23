package querygen

import (
	"fmt"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/qw4990/OptimizerTester/tidb"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func logTime() string {
	str, err := time.Now().MarshalText()
	if err != nil {
		panic(err)
	}
	return string(str)
}

const concurrencyForEachDSN = uint(1)

func RunQueryGen(dsns []string, outputFile, dbName, tableName string, n uint) error {
	var err error
	queryTaskChan := make(chan *tidb.QueryTask, 100)
	for i, dsn := range dsns {
		setMemQuotaSQL := "set @@tidb_mem_quota_query = 3221225472;"
		err = tidb.StartQueryRunner(dsn, queryTaskChan, concurrencyForEachDSN, 1, uint(i), setMemQuotaSQL)
		if err != nil {
			return err
		}
		fmt.Printf("[%s] %d query runners started for DSN#%d: %s.\n", logTime(), concurrencyForEachDSN, i, dsn)
	}
	p := parser.New()
	fullTableName := dbName + "." + tableName

	// 1. collect and set table schema info
	showSQL := "SHOW CREATE TABLE " + fullTableName
	showResult := runQuery(showSQL, queryTaskChan)
	createTableText := showResult[0][1].([]byte)
	stmts, _, err := p.ParseSQL(string(createTableText))
	if err != nil {
		panic(err)
	}
	showStmt := stmts[0].(*ast.CreateTableStmt)

	colArray := make([]column, len(showStmt.Cols))
	cols := make([]*column, len(showStmt.Cols))
	for i := range cols {
		cols[i] = &colArray[i]
	}
	for i, col := range showStmt.Cols {
		colArray[i].Name = col.Name.Name.L
		colArray[i].TP = *col.Tp
	}

	singleColIdxs := make([]*index, 0)
	multiColIdxs := make([]*index, 0)
	for _, con := range showStmt.Constraints {
		tp := con.Tp
		if tp != ast.ConstraintIndex &&
			tp != ast.ConstraintKey &&
			tp != ast.ConstraintUniq &&
			tp != ast.ConstraintUniqIndex &&
			tp != ast.ConstraintUniqKey {
			continue
		}
		idxCols := make([]*column, len(con.Keys))
		for i, key := range con.Keys {
			col := findColByName(cols, key.Column.Name.L)
			idxCols[i] = col
		}
		idx := index{
			Name: con.Name,
			Cols: idxCols,
		}
		if len(idxCols) > 1 {
			multiColIdxs = append(multiColIdxs, &idx)
		} else {
			singleColIdxs = append(singleColIdxs, &idx)
		}
	}

	tbl := table{
		Name:           tableName,
		Cols:           cols,
		singleColIdxes: singleColIdxs,
		multiColIdxes:  multiColIdxs,
	}

	// 2. collect and set table data distribution info
	queryDataSQL := "select "
	for _, col := range cols {
		queryDataSQL += "max(" + col.Name + "), min(" + col.Name + "), count(distinct " + col.Name + "),"
	}
	queryDataSQL += "count(*) from " + fullTableName
	result := runQuery(queryDataSQL, queryTaskChan)[0]
	for i, col := range cols {
		col.Max = queryResultToStr(result[3*i+0], col.TP)
		col.Min = queryResultToStr(result[3*i+1], col.TP)
		ndvBytes := result[3*i+2].([]uint8)
		ndv, err := strconv.ParseUint(string(ndvBytes), 10, 64)
		if err != nil {
			panic(err)
		}
		col.NDV = uint(ndv)
	}
	ndvBytes := result[len(result)-1].([]uint8)
	ndv, err := strconv.ParseUint(string(ndvBytes), 10, 64)
	if err != nil {
		panic(err)
	}
	tbl.RowCount = uint(ndv)

	sampleSQLTemplate := "select %s from " + fullTableName + " where rand() < %f order by %s"
	sampleDistinctSQLTemplate := "select * from (select distinct %s from " + fullTableName + ") n where rand() < %f order by %s"
	for _, col := range cols {
		sampleRate := float64(n) / float64(tbl.RowCount)
		if sampleRate > 1 {
			sampleRate = 1
		}
		sampleSQL := fmt.Sprintf(sampleSQLTemplate, col.Name, sampleRate, col.Name)
		result := runQuery(sampleSQL, queryTaskChan)
		for _, val := range result {
			col.RandVals = append(col.RandVals, queryResultToStr(val[0], col.TP))
		}

		distinctSampleRate := float64(n) / float64(col.NDV)
		if distinctSampleRate > 1 {
			distinctSampleRate = 1
		}
		sampleDistinctSQL := fmt.Sprintf(sampleDistinctSQLTemplate, col.Name, distinctSampleRate, col.Name)
		result = runQuery(sampleDistinctSQL, queryTaskChan)
		for _, val := range result {
			col.RandDistinctVals = append(col.RandDistinctVals, queryResultToStr(val[0], col.TP))
		}
	}
	queryTaskChan <- &tidb.QueryTask{Exited: true}

	// 3. generate query patterns
	if len(tbl.multiColIdxes) == 0 {
		fmt.Println("Table " + fullTableName + " doesn't contain multi-col index")
		return nil
	}
	patterns := make([]*pattern, 0, len(multiColIdxs)*2)
	for _, idx := range tbl.multiColIdxes {
		for i := range idx.Cols {
			colPatterns := make([]*colPattern, 0, len(idx.Cols))
			for j := 0; j < i; j++ {
				colPatterns = append(colPatterns, &colPattern{
					col: idx.Cols[j],
					tp:  equal,
				})
			}
			colPatterns = append(colPatterns, &colPattern{
				col: idx.Cols[i],
				tp:  interval,
			})
			patterns = append(patterns, &pattern{cols: colPatterns})
		}
	}

	// 4. for loop: generate query
	file, err := os.Create(outputFile)
	if err != nil {
		panic(err)
	}
	defer func() {
		err = file.Close()
		if err != nil {
			panic(err)
		}
	}()
	dedupMap := make(map[string]struct{})
	for i := 0; i < int(n); {
		sql := "select * from " + fullTableName + " where "
		pt := patterns[rand.Intn(len(patterns))]
		expr := pt.generate()
		if _, ok := dedupMap[expr]; ok {
			continue
		}
		dedupMap[expr] = struct{}{}
		sql += expr
		sql += ";\n"
		_, err = file.WriteString(sql)
		if err != nil {
			panic(err)
		}
		i++
	}

	return nil
}

func runQuery(sql string, c chan *tidb.QueryTask) [][]interface{} {
	resultChan := make(chan *tidb.QueryResult, 1)
	task := &tidb.QueryTask{
		Payload: tidb.PlainSQL(sql),
		Dest:    resultChan,
		Finish:  nil,
		Exited:  false,
	}
	c <- task
	result := <-resultChan
	return result.Result
}

func queryResultToStr(value interface{}, ft types.FieldType) string {
	if value == nil {
		return "NULL"
	}
	result := string(value.([]uint8))
	if ft.Tp != mysql.TypeTiny &&
		ft.Tp != mysql.TypeShort &&
		ft.Tp != mysql.TypeLong &&
		ft.Tp != mysql.TypeFloat &&
		ft.Tp != mysql.TypeDouble &&
		ft.Tp != mysql.TypeLonglong &&
		ft.Tp != mysql.TypeInt24 &&
		ft.Tp != mysql.TypeNewDecimal {
		if ft.Tp == mysql.TypeBlob ||
			ft.Tp == mysql.TypeTinyBlob ||
			ft.Tp == mysql.TypeMediumBlob ||
			ft.Tp == mysql.TypeLongBlob ||
			ft.Tp == mysql.TypeString ||
			ft.Tp == mysql.TypeVarchar ||
			ft.Tp == mysql.TypeVarString {
			if len(result) > 20 {
				result = result[:21]
			}
		}
		result = strings.ReplaceAll(result, "\"", "\\\"")
		result = "\"" + result + "\""
	}
	return result
}
