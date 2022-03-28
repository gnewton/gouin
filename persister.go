package gouin

import (
	"database/sql"
	"errors"
	"log"
)

type Persister struct {
	dialect   Dialect
	db        *sql.DB
	tx        *sql.Tx
	txSize    uint32
	txCounter uint32
}

func NewPersister(dialect Dialect, db *sql.DB, txSize uint32) (*Persister, error) {
	if dialect == nil {
		return nil, errors.New("Dialect is nil")
	}

	if db == nil {
		return nil, errors.New("DB is nil")
	}

	p := Persister{
		dialect: dialect,
		db:      db,
		txSize:  txSize,
	}

	return &p, nil
}

func (p *Persister) CreateTables(tables ...*Table) error {
	if p.tx != nil {
		return errors.New("Must no be inside transaxtion (tx must be nil)")
	}
	var err error
	p.tx, err = p.db.Begin()
	if err != nil {
		log.Println(err)
		return err
	}
	for i, _ := range tables {
		tab := tables[i]
		createSql, err := p.dialect.CreateTableSql(tab.name, tab.fields, tab.pk.name)
		if err != nil {
			return err
		}
		result, err := p.tx.Exec(createSql)
		if err != nil {
			return err
		}
		if result == nil {
			return errors.New("result is nil")
		}

	}
	err = p.tx.Commit()
	if err != nil {
		return err
	}
	p.tx = nil
	return nil
}
func (p *Persister) ExistsByPK(tab *Table, v any) (bool, error) {
	return false, errors.New("TODO")
}

func (p *Persister) SelectOneRecordByPK(tab *Table, v any, rec *Record) error {
	if v == nil {
		return errors.New("PK is nil")
	}
	if tab == nil {
		return errors.New("Table is nil")
	}
	if rec == nil {
		return errors.New("Record is nil")
	}
	if tab.selectOneRecordByPKPreparedStatement == nil {
		return errors.New("Table.selectOneRecordByPKPreparedStatement is nil; table:" + tab.name)
	}

	rows, err := tab.selectOneRecordByPKPreparedStatement.Query(v)
	if err != nil {
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		return errors.New("No records returned" + tab.name)
	}

	if err := rows.Scan(rec.outValues...); err != nil {
		return err
	}
	if rows.Next() {
		return errors.New(">1 records returned" + tab.name)
	}
	return nil

}

func (p *Persister) DeleteByPK(tab *Table, v any) error {
	if tab == nil {
		return errors.New("Table is nil")
	}
	if tab.deleteByPKPreparedStatement == nil {
		return errors.New("Table.deleteByPKPreparedStatement is nil; table:" + tab.name)
	}
	results, err := tab.deleteByPKPreparedStatement.Exec(v)
	if err != nil {
		return err
	}
	if results == nil {
		return err
	}
	rowsAffected, err := results.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected != 1 {
		return err
	}
	return nil
}

// Responsible to:
// 1 - If Right table id is not in cache (not previously saved; key is )
//     - Increment right table id counter, add it to cache; assign to right table rec,
//     - Assign right table rec PK = right table id counter
//     - Save right table rec
// 2 - Assign jt rt id = right table id counter
// 3 - Save jt
func (p *Persister) InsertJoin(jt *Table, leftRec, rightRec *Record) error {
	if jt == nil {
		return errors.New("Join table is nil")
	}
	if jt.joinTableInfo == nil {
		return errors.New("JoinTableInfo is nil")
	}
	if leftRec == nil {
		return errors.New("Left record is nil")
	}
	if rightRec == nil {
		return errors.New("Right record is nil")
	}
	if rightRec.table == nil {
		return errors.New("Right record.table is nil")
	}
	if rightRec.table.pk == nil {
		return errors.New("Right record.table.pkis nil")
	}
	//
	jtInfo := jt.joinTableInfo
	if jtInfo.leftTable != leftRec.table {
		return errors.New("Left tables do not match")
	}
	if jtInfo.rightTable != rightRec.table {
		return errors.New("Right tables do not match")
	}

	key, err := jtInfo.makeKey(rightRec)
	if err != nil {
		return err
	}

	log.Println(key)

	leftId := leftRec.values[leftRec.table.pk.positionInTable]
	rightId := rightRec.values[rightRec.table.pk.positionInTable]

	joinRec, err := jt.Record()
	if err != nil {
		return err
	}

	// left table id value
	joinRec.SetAt(0, leftId)
	// left table id value
	joinRec.SetAt(1, rightId)

	if err := p.Insert(joinRec); err != nil {
		return err
	}

	return errors.New("TODO")
}

func (p *Persister) Insert(rec *Record) error {
	if rec == nil {
		err := errors.New("Record is nil")
		log.Println(err)
		return err
	}

	if rec.table == nil {
		err := errors.New("Record.table is nil")
		log.Println(err)
		return err
	}

	if rec.table.insertPreparedStatement == nil {
		err := errors.New("Prepared statement is nil: table:" + rec.table.name)
		return err
	}
	result, err := rec.table.insertPreparedStatement.Exec(rec.values...)

	if err != nil {
		log.Println(err)
		return err
	}
	if result == nil {
		log.Println(err)
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Println(err)
		return err
	}
	if rowsAffected != 1 {
		log.Println(err)
		return err
	}
	return nil
}

func (p *Persister) JoinTableInsert(joinTable *Table, leftRec, rightRec *Record) error {
	// TODO: leftRec is saved elsewhere; cache rightRec ids; save rightRec; joinTable should contain fields used for cache key
	if leftRec.table != joinTable.joinTableInfo.leftTable {
		return errors.New("Record left table does not match join table left table")
	}

	if rightRec.table != joinTable.joinTableInfo.rightTable {
		return errors.New("Record right table does not match join table right table")
	}

	jrec, err := joinTable.Record()
	if err != nil {
		return err
	}
	// left table id value
	jrec.SetAt(0, leftRec.values[leftRec.table.pk.positionInTable])
	// left table id value
	jrec.SetAt(1, rightRec.values[rightRec.table.pk.positionInTable])

	if err := p.Insert(jrec); err != nil {
		return err
	}

	return nil

}

func (p *Persister) TxCommit(tables ...*Table) error {
	var err error
	for i, _ := range tables {
		tab := tables[i]
		err = closePreparedStatements(tab.insertPreparedStatement, tab.deleteByPKPreparedStatement, tab.selectOneRecordByPKPreparedStatement)

		if err != nil {
			return err
		}
		tab.insertPreparedStatement = nil
		tab.deleteByPKPreparedStatement = nil
		tab.selectOneRecordByPKPreparedStatement = nil
	}

	err = p.tx.Commit()
	if err != nil {
		return err
	}
	p.tx = nil
	return nil
}

//Start a DB transactions
//
// All tables involved in a transaction must be included here, as prepared statements for each table are created here and cached
func (p *Persister) TxBegin(tables ...*Table) error {
	var err error

	p.tx, err = p.db.Begin()
	if err != nil {
		return err
	}

	if len(tables) == 0 {
		return errors.New("No tables in TxBegin")
	}

	for i, _ := range tables {
		tab := tables[i]
		err = makeNewPreparedStatements(p.dialect, tab, p.tx)
		if err != nil {
			return err
		}
	}

	return err
}

func closePreparedStatements(stmts ...*sql.Stmt) error {
	for i, _ := range stmts {
		stmt := stmts[i]
		if stmt != nil {
			if err := stmt.Close(); err != nil {
				return err
			}
		}
	}
	return nil

}

func makeNewPreparedStatements(dialect Dialect, tab *Table, tx *sql.Tx) error {
	var err error

	// INSERT
	if tab.insertPreparedStatementSql == "" {
		tab.insertPreparedStatementSql, err = dialect.InsertPreparedStatementSql(tab.name, tab.fields)
		if err != nil {
			return err
		}
	}
	tab.insertPreparedStatement, err = tx.Prepare(tab.insertPreparedStatementSql)
	if err != nil {
		return err
	}

	// DELETE BY PK
	if tab.deleteByPKPreparedStatementSql == "" {

		tab.deleteByPKPreparedStatementSql, err = dialect.DeleteByPKPreparedStatementSql(tab.name, tab.pk.name, tab.pk.NeedsQuotes())
		if err != nil {
			return err
		}
	}
	tab.deleteByPKPreparedStatement, err = tx.Prepare(tab.deleteByPKPreparedStatementSql)
	if err != nil {
		return err
	}

	// SELECT BY PK
	if tab.selectOneRecordByPKPreparedStatementSql == "" {
		tab.selectOneRecordByPKPreparedStatementSql, err = dialect.SelectOneRecordByPKPreparedStatementSql(tab.name, tab.fields, tab.pk.name, tab.pk.NeedsQuotes())

		if err != nil {
			return err
		}
	}
	tab.selectOneRecordByPKPreparedStatement, err = tx.Prepare(tab.selectOneRecordByPKPreparedStatementSql)
	if err != nil {
		return err
	}

	return nil
}
