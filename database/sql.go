package database

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

var placeholderValue = "(?)"
var placeholderKeyValue = "(?,?)"
var schema = map[string][]string{
	"sqlite3": []string{
		`CREATE TABLE IF NOT EXISTS t (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			value TEXT,
			ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)`,
		`CREATE TABLE IF NOT EXISTS kv (
			key TEXT PRIMARY KEY,
			value TEXT,
			ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP) WITHOUT ROWID`,
	},
}

func init() {
	// Factories["mysql"] = _new
	Factories["sqlite3"] = _new
}

// sqldb the backend SQL DB to persist values
type sqldb struct {
	*sql.DB
	conf    Conf
	encoder Encoder
}

// New creates a new sql database
func _new(conf Conf, encoder Encoder) (DB, error) {
	db, err := sql.Open(conf.Driver, conf.Source)
	if err != nil {
		return nil, err
	}
	for _, v := range schema[conf.Driver] {
		if _, err = db.Exec(v); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &sqldb{DB: db, conf: conf, encoder: encoder}, nil
}

// Conf returns the configuration
func (d *sqldb) Conf() Conf {
	return d.conf
}

// * t

// Put puts values into SQL DB
func (d *sqldb) Put(values []interface{}) error {
	l := len(values)
	if l == 0 {
		return nil
	}
	phs := make([]string, l)
	for i := 0; i < l; i++ {
		phs[i] = placeholderValue
		if d.encoder != nil {
			values[i] = d.encoder.Encode(values[i])
		}
	}
	query := fmt.Sprintf(
		"insert into t(value) values %s",
		strings.Join(phs, ", "),
	)
	stmt, err := d.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(values...)
	return err
}

// Get gets values from SQL DB
func (d *sqldb) Get(offset uint64, length int) ([]interface{}, error) {
	query := fmt.Sprintf("select id, value from t where id >= %d order by id limit %d", offset, length)
	rows, err := d.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []interface{}{}
	for rows.Next() {
		var id uint64
		var value []byte
		err = rows.Scan(&id, &value)
		if err != nil {
			return nil, err
		}
		if d.encoder != nil {
			values = append(values, d.encoder.Decode(value, id))
		} else {
			values = append(values, value)
		}
	}
	return values, nil
}

// Del deletes values by IDs from SQL DB
func (d *sqldb) Del(ids []uint64) error {
	args := []string{}
	for _, id := range ids {
		args = append(args, strconv.FormatUint(id, 10))
	}
	query := fmt.Sprintf("delete from t where id in (%s)", strings.Join(args, ","))
	_, err := d.Exec(query)
	return err
}

// * kv

// SetKV sets key and value into SQL DB
func (d *sqldb) SetKV(key, value interface{}) error {
	if key == nil || value == nil {
		return nil
	}
	stmt, err := d.Prepare("insert into kv(key,value) values (?,?) on conflict(key) do update set value=excluded.value")
	if err != nil {
		return err
	}
	defer stmt.Close()
	if d.encoder != nil {
		value = d.encoder.Encode(value)
	}
	_, err = stmt.Exec(key, value)
	return err
}

// GetKV gets value by key from SQL DB
func (d *sqldb) GetKV(key interface{}) (interface{}, error) {
	rows, err := d.Query("select value from kv where key=?", key)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var value []byte
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		if d.encoder != nil {
			return d.encoder.Decode(value), nil
		}
		return value, nil
	}
	return nil, nil
}

// Del deletes key and value from SQL DB
func (d *sqldb) DelKV(key interface{}) error {
	_, err := d.Exec("delete from kv where key=?", key)
	return err
}

// ListKV list all values
func (d *sqldb) ListKV() (vs []interface{}, err error) {
	rows, err := d.Query("select value from kv")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := []interface{}{}
	for rows.Next() {
		var value []byte
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		if d.encoder != nil {
			values = append(values, d.encoder.Decode(value))
		} else {
			values = append(values, value)
		}
	}
	return values, nil
}
