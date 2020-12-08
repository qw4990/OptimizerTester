package cetest

import (
	"fmt"
	"math/rand"

	"github.com/pingcap/errors"
)

/*
	datasetMock is used for testing.
	MockDataset's schema is:
		CREATE TABLE t (a INT PRIMARY KEY, b INT, KEY(b), KEY(a, b));
*/
type datasetMock struct {
}

func (ds *datasetMock) Name() string {
	return "Mock"
}

func (ds *datasetMock) GenCases(n int, qt QueryType) ([]string, error) {
	sqls := make([]string, 0, n)
	switch qt {
	case QTSingleColPointQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randSingleColPointQuery())
		}
	case QTSingleColRangeQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randSingleColRangeQuery())
		}
	case QTMultiColsPointQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randMultiColsPointQuery())
		}
	case QTMultiColsRangeQuery:
		for i := 0; i < n; i++ {
			sqls = append(sqls, ds.randMultiColsRangeQuery())
		}
	default:
		return nil, errors.Errorf("unsupported query-type=%v", qt.String())
	}
	return sqls, nil
}

func (ds *datasetMock) randSingleColPointQuery() string {
	cols := []string{"a", "b"}
	return fmt.Sprintf("select * from t where %v = %v", cols[rand.Intn(2)], rand.Int())
}

func (ds *datasetMock) randSingleColRangeQuery() string {
	cols := []string{"a", "b"}
	if rand.Intn(2) == 0 {
		return fmt.Sprintf("select * from t where %v >= %v", cols[rand.Intn(2)], rand.Int())
	} else {
		col := cols[rand.Intn(2)]
		begin := rand.Intn(1000000)
		return fmt.Sprintf("select * from t where %v >= %v and %v <= %v", col, begin, col, begin+rand.Intn(1000000))
	}
}

func (ds *datasetMock) randMultiColsPointQuery() string {
	return fmt.Sprintf("select * from t where a = %v and b = %v", rand.Int(), rand.Int())
}

func (ds *datasetMock) randMultiColsRangeQuery() string {
	return fmt.Sprintf("select * from t where a > %v and b > %v", rand.Int(), rand.Int())
}
