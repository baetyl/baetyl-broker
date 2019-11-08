package sqldb

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/baetyl/baetyl-broker/db"
	"github.com/baetyl/baetyl-broker/msg"
	"github.com/golang/protobuf/proto"
	"github.com/mattn/go-sqlite3"
)

var placeholderValue = "(?)"
var checkSQLite3Exists = sqlite3.SQLITE_OK

// SQLDB the backend SQL DB to persist messages
type SQLDB struct {
	*sql.DB
}

// NewSQLDB creates a new SQL DB
func NewSQLDB(dataSourceName string) (db.DB, error) {
	// TODO: make SQL driver configurable
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}
	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS queue (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		value TEXT)`)

	if err != nil {
		db.Close()
		return nil, err
	}
	return &SQLDB{DB: db}, nil
}

// Put put messages into SQL DB
func (d *SQLDB) Put(msgs []*message.Message) error {
	l := len(msgs)
	if l == 0 {
		return nil
	}
	vs := make([]string, l)
	args := make([]interface{}, l)
	for i := 0; i < l; i++ {
		vs[i] = placeholderValue
		args[i], _ = proto.Marshal(msgs[i])
	}
	query := fmt.Sprintf(
		"insert into queue(value) values %s",
		strings.Join(vs, ", "),
	)
	stmt, err := d.Prepare(query)
	if err != nil {
		return err
	}
	_, err = stmt.Exec(args...)
	return err
}

// Get gets messages from SQL DB
func (d *SQLDB) Get(offset uint64, length int) ([]*message.Message, error) {
	query := fmt.Sprintf("select id, value from queue where id >= %d order by id limit %d", offset, length)
	rows, err := d.Query(query)
	if err != nil {
		return nil, err
	}
	msgs := []*message.Message{}
	for rows.Next() {
		var id uint64
		var v []byte
		err = rows.Scan(&id, &v)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		}
		m := new(message.Message)
		err = proto.Unmarshal(v, m)
		if err != nil {
			return nil, err
		}
		m.Context.ID = id
		msgs = append(msgs, m)
	}
	return msgs, nil
}

// Del deletes message from SQL DB
func (d *SQLDB) Del(ids []uint64) error {
	args := []string{}
	for _, id := range ids {
		args = append(args, strconv.FormatUint(id, 10))
	}
	query := fmt.Sprintf("delete from queue where id in (%s)", strings.Join(args, ","))
	_, err := d.Exec(query)
	return err
}
