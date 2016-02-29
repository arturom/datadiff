package datasource

import (
	"database/sql"
	"fmt"

	"github.com/arturom/datadiff/histogram"
	_ "github.com/go-sql-driver/mysql"
)

type MysqlDataSource struct {
	DB        *sql.DB
	Tablename string
	FieldName string
}

func (s MysqlDataSource) FetchHistogramAll(interval int) (histogram.Histogram, error) {
	q := fmt.Sprintf(
		`SELECT
    	   floor(%[3]s / %[1]d) * %[1]d AS BinKey,
    	   count(%[3]s) AS Count
        FROM
	       %[2]s
        GROUP BY
	       BinKey`, interval, s.Tablename, s.FieldName)

	rows, err := s.DB.Query(q)

	if err != nil {
		return histogram.Histogram{}, err
	}

	bins := make(histogram.Bins, 0)

	for rows.Next() {
		var key, count int
		rows.Scan(&key, &count)
		bins = append(bins, histogram.Bin{
			Key:   key,
			Count: count,
		})
	}

	return histogram.Histogram{
		BinCapacity: interval,
		Bins:        bins,
	}, nil
}

func (s MysqlDataSource) FetchHistogramRange(gte, lt, interval int) (histogram.Histogram, error) {
	q := fmt.Sprintf(
		`SELECT
    	   floor(%[3]s / %[1]d) * %[1]d AS BinKey,
    	   count(%[3]s) AS Count
        FROM
	       %[2]s
        WHERE
            %[3]s >= %[4]d
            AND %[3]s < %[5]d
        GROUP BY
	       BinKey`, interval, s.Tablename, s.FieldName, gte, lt)
	rows, err := s.DB.Query(q)

	if err != nil {
		return histogram.Histogram{}, err
	}

	bins := make(histogram.Bins, 0)

	for rows.Next() {
		var key, count int
		rows.Scan(&key, &count)
		bins = append(bins, histogram.Bin{
			Key:   key,
			Count: count,
		})
	}

	return histogram.Histogram{
		BinCapacity: interval,
		Bins:        bins,
	}, nil
}

func (s MysqlDataSource) FetchIdRange(gte, lt int) ([]int, error) {
	q := fmt.Sprintf(
		`SELECT
    	   %[2]s
        FROM
	       %[1]s
        WHERE
            %[2]s >= %[3]d
            AND %[2]s < %[4]d`, s.Tablename, s.FieldName, gte, lt)

	rows, err := s.DB.Query(q)

	if err != nil {
		return nil, err
	}

	ids := []int{}

	for rows.Next() {
		var id int
		rows.Scan(&id)
		ids = append(ids, id)
	}

	return ids, nil
}
