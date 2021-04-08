package gopg

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
	"github.com/go-pg/pg/v10/types"
	"github.com/gopsql/db"
)

var (
	// These functions are copied from the go-pg package, so that you don't
	// have to import "github.com/go-pg/pg/v10/types".
	TypesScan        = types.Scan
	TypesScanString  = types.ScanString
	TypesScanBytes   = types.ScanBytes
	TypesReadBytes   = types.ReadBytes
	TypesScanInt     = types.ScanInt
	TypesScanInt64   = types.ScanInt64
	TypesScanUint64  = types.ScanUint64
	TypesScanFloat32 = types.ScanFloat32
	TypesScanFloat64 = types.ScanFloat64
	TypesScanTime    = types.ScanTime
	TypesScanBool    = types.ScanBool

	rePosParam = regexp.MustCompile(`\$[0-9]+`)
)

type (
	// types.Reader from go-pg package, so that you don't have to import it.
	TypesReader = types.Reader

	DB struct {
		*pg.DB
	}

	Tx struct {
		*pg.Tx
	}

	Result struct {
		rowsAffected int64
	}

	queryRows struct {
		db  *DB
		tx  *Tx
		ctx context.Context

		query string
		args  []interface{}

		mutex  sync.Mutex
		closed bool

		destChan chan []interface{}
		errChan  chan error
		nextChan chan bool

		rowDest  []interface{}
		rowError error
	}

	queryRow struct {
		db  *DB
		tx  *Tx
		ctx context.Context

		query string
		args  []interface{}
	}
)

// MustOpen is like Open but panics if connect operation fails.
func MustOpen(conn string) db.DB {
	c, err := Open(conn)
	if err != nil {
		panic(err)
	}
	return c
}

// Open creates and establishes one connection to database.
func Open(conn string) (db.DB, error) {
	opt, err := pg.ParseURL(conn)
	if err != nil {
		return nil, err
	}
	db := pg.Connect(opt)
	if err := db.Ping(context.Background()); err != nil {
		return nil, err
	}
	return &DB{db}, nil
}

// Convert positional parameters (like $1 in "WHERE name = $1") to question
// marks ("?") used in go-pg.
func (d *DB) ConvertParameters(query string, args []interface{}) (outQuery string, outArgs []interface{}) {
	outQuery = rePosParam.ReplaceAllStringFunc(query, func(in string) string {
		pos, _ := strconv.Atoi(strings.TrimPrefix(in, "$"))
		outArgs = append(outArgs, args[pos-1])
		return "?"
	})
	return
}

func (d *DB) Exec(query string, args ...interface{}) (db.Result, error) {
	re, err := d.DB.Exec(query, args...)
	if err != nil {
		return nil, err
	}
	return &Result{
		rowsAffected: int64(re.RowsAffected()),
	}, nil
}

func (d *DB) Query(query string, args ...interface{}) (db.Rows, error) {
	return &queryRows{
		db:    d,
		query: query,
		args:  args,
	}, nil
}

func (d *DB) QueryRow(query string, args ...interface{}) db.Row {
	return &queryRow{
		db:    d,
		query: query,
		args:  args,
	}
}

func (d *DB) BeginTx(ctx context.Context, isolationLevel string) (db.Tx, error) {
	tx, err := d.DB.BeginContext(ctx)
	if err != nil {
		return nil, err
	}
	return &Tx{tx}, nil
}

func (d *DB) ErrNoRows() error {
	return pg.ErrNoRows
}

func (d *DB) ErrGetCode(err error) string {
	if e, ok := err.(interface{ Field(byte) string }); ok {
		return e.Field('C')
	}
	return "unknown"
}

func (t *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (db.Result, error) {
	re, err := t.Tx.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &Result{
		rowsAffected: int64(re.RowsAffected()),
	}, nil
}

func (t *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (db.Rows, error) {
	return &queryRows{
		tx:    t,
		ctx:   ctx,
		query: query,
		args:  args,
	}, nil
}

func (t *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) db.Row {
	return &queryRow{
		tx:    t,
		ctx:   ctx,
		query: query,
		args:  args,
	}
}

func (t *Tx) Commit(ctx context.Context) error {
	return t.Tx.CommitContext(ctx)
}

func (t *Tx) Rollback(ctx context.Context) error {
	return t.Tx.RollbackContext(ctx)
}

func (r Result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func (q *queryRows) isClosed() bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return q.closed
}

func (q *queryRows) Close() error {
	q.mutex.Lock()
	q.closed = true
	q.mutex.Unlock()
	if q.destChan != nil {
		close(q.destChan)
	}
	if q.errChan != nil {
		close(q.errChan)
	}
	if q.nextChan != nil {
		close(q.nextChan)
	}
	return nil // step 13
}

func (q *queryRows) Err() error {
	if q.nextChan == nil { // no Next() called
		return nil
	}
	return <-q.errChan // step 12
}

// Since go-pg doesn't use something like Go's database/sql.DB.Query, so we
// need to use channels to make it work with db.DB:
//  rows, _ := db.Query()
//  defer rows.Close()
//  for rows.Next() {
//  	rows.Scan(...)
//  }
//  rows.Err()
// The "queryRows" implements go-pg's orm model functions and acts like a model
// for go-pg. When rows.Next() is called for the first time, go-pg's Query()
// function will be executed in a new goroutine. The rows.Next() function
// returns true every time when BeforeScan() is called (only when Query() has
// rows to return), returns false once go-pg's Query() function finishes. When
// rows.Next() returns true, rows.Scan() will be called, and all destination
// pointers of rows.Scan() will pass to ScanColumn(). When a row finishes
// scanning, AfterScan() is called.
func (q *queryRows) Next() bool {
	if q.nextChan == nil { // step 0
		q.destChan = make(chan []interface{}, 1)
		q.errChan = make(chan error, 1)
		q.nextChan = make(chan bool, 1)
		go func() {
			var err error
			if q.tx != nil {
				_, err = q.tx.Tx.QueryContext(q.ctx, q, q.query, q.args...)
			} else {
				_, err = q.db.DB.Query(q, q.query, q.args...) // step 1
			}
			if !q.isClosed() {
				q.nextChan <- false // step 9
				q.errChan <- err    // step 11
			}
		}()
	}
	select {
	case next := <-q.nextChan: // step 3, 10
		return next
	case <-time.After(1 * time.Second):
		panic("timed out: you must call Scan() after Next()")
	}
}

func (q *queryRows) Scan(dest ...interface{}) (err error) {
	if q.isClosed() {
		return
	}
	q.destChan <- dest // step 4
	err = <-q.errChan  // step 8
	return
}

// orm.HooklessModel interface
func (q *queryRows) Init() error                             { return nil }
func (q *queryRows) NextColumnScanner() orm.ColumnScanner    { return q }
func (q queryRows) AddColumnScanner(orm.ColumnScanner) error { return nil }

// for every row
func (q *queryRows) BeforeScan(c context.Context) error {
	if q.isClosed() {
		return nil
	}
	q.nextChan <- true       // step 2
	q.rowDest = <-q.destChan // step 5
	q.rowError = nil
	return nil
}

// for every column
func (q *queryRows) ScanColumn(col types.ColumnInfo, rd types.Reader, n int) error {
	if q.isClosed() {
		return nil
	}
	err := types.Scan(q.rowDest[col.Index], rd, n) // step 6
	if q.rowError == nil {
		q.rowError = err
	}
	return nil
}

// for every row
func (q *queryRows) AfterScan(c context.Context) error {
	if q.isClosed() {
		return nil
	}
	q.errChan <- q.rowError // step 7
	return q.rowError
}

func (q *queryRow) Scan(dest ...interface{}) (err error) {
	if q.tx != nil {
		_, err = q.tx.Tx.QueryOneContext(q.ctx, pg.Scan(dest...), q.query, q.args...)
	} else {
		_, err = q.db.DB.QueryOne(pg.Scan(dest...), q.query, q.args...)
	}
	return
}
