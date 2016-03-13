package datasource

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/arturom/datadiff/histogram"
	// Import the sql driver but use the sql interfaces
	_ "github.com/go-sql-driver/mysql"
)

type MysqlDataSource struct {
	DB         *sql.DB
	Tablename  string
	FieldName  string
	Conditions []string
}

func (s MysqlDataSource) FetchHistogramAll(interval int) (histogram.Histogram, error) {
	q := query{}
	q.selectField(fmt.Sprintf("FLOOR(%[1]s / %[2]d) * %[2]d AS `BinKey`", s.FieldName, interval)).
		selectField("COUNT(*) AS `Count`").
		from(s.Tablename).
		where(s.Conditions...).
		group("`BinKey`")

	rows, err := s.DB.Query(q.string())

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
	q := query{}
	q.selectField(fmt.Sprintf("FLOOR(%[1]s / %[2]d) * %[2]d AS `BinKey`", s.FieldName, interval)).
		selectField("COUNT(*) AS `Count`").
		from(s.Tablename).
		where(fmt.Sprintf("`%s` >= %d", s.FieldName, gte), fmt.Sprintf("`%s` < %d", s.FieldName, lt)).
		where(s.Conditions...).
		group("`BinKey`")

	rows, err := s.DB.Query(q.string())

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

func (s MysqlDataSource) FetchIDRange(gte, lt int) ([]int, error) {
	q := query{}
	q.selectField(fmt.Sprintf("`%s`", s.FieldName)).
		from(s.Tablename).
		where(fmt.Sprintf("`%s` >= %d", s.FieldName, gte), fmt.Sprintf("`%s` < %d", s.FieldName, lt)).
		where(s.Conditions...)

	rows, err := s.DB.Query(q.string())

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

type query struct {
	Fields      []string
	Table       string
	Conditions  []string
	GroupClause string
}

func (q *query) selectField(f string) *query {
	q.Fields = append(q.Fields, f)
	return q
}

func (q *query) from(t string) *query {
	q.Table = t
	return q
}

func (q *query) where(c ...string) *query {
	for _, c := range c {
		q.Conditions = append(q.Conditions, c)
	}
	return q
}

func (q *query) group(g string) *query {
	q.GroupClause = g
	return q
}

func (q query) string() string {
	ret := fmt.Sprintf(
		"SELECT %s FROM %s",
		strings.Join(q.Fields, ", "),
		q.Table)
	if len(q.Conditions) != 0 {
		ret += fmt.Sprintf(" WHERE %s", strings.Join(q.Conditions, " AND "))
	}
	if q.GroupClause != "" {
		ret += fmt.Sprintf(" GROUP BY %s", q.GroupClause)
	}
	return ret
}
