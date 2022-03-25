package gouin

import (
	"errors"
	"testing"
)

//////////////////////////////////////////////////////////////////////
// Failing tests
func TestPersist_InsertJoin_JTNil(t *testing.T) {
	//t.Fatal("TODO")
}

func TestPersist_InsertJoin_JTInfoNil(t *testing.T) {
	//	t.Fatal("TODO")
}

func TestPersist_InsertJoin_LeftRecNil(t *testing.T) {
	//	t.Fatal("TODO")
}

func TestPersist_InsertJoin_RightRecNil(t *testing.T) {
	//	t.Fatal("TODO")
}

func TestPersist_InsertJoin_LeftRecNEJTInfoLeftRec(t *testing.T) {
	//	t.Fatal("TODO")
}

func TestPersist_InsertJoin_RightRecNEJTInfoRightRec(t *testing.T) {
	//	t.Fatal("TODO")
}

//////////////////////////////////////////////////////////////////////
// Positive tests
func TestPersist_positive(t *testing.T) {
	//t.Fatal("TODO")
}

func TestPersist_Insert(t *testing.T) {
	dialect, err := NewDialectSqlite3()
	if err != nil {
		t.Fatal(err)
	}
	db, err := dialect.OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewPersister(dialect, db, 0)

	personTable, _, _, _, err := personTableFull(dialect)
	if err != nil {
		t.Fatal(err)
	}

	// Make person table
	err = p.CreateTables(personTable)
	if err != nil {
		t.Fatal(err)
	}
	// Start transaction
	p.tx, err = p.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := p.tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}()
	err = makeNewPreparedStatements(dialect, personTable, p.tx)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := personTable.Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(0, uint64(42)); err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(1, "Bill"); err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(2, true); err != nil {
		t.Fatal(err)
	}

	// rec := Record{
	// 	table:  personTable,
	// 	values: []*any{uint32(42), "Bill", true},
	// }

	if err := rec.initialize(false); err != nil {
		t.Fatal(err)
	}

	err = p.Insert(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err = rec.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(0, uint64(49)); err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(1, "Harry"); err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(2, false); err != nil {
		t.Fatal(err)
	}
	err = p.Insert(rec)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(personTable.selectOneRecordByPKPreparedStatementSql)
	// Added: 42; Select: 32: should fail
	newRec, err := personTable.Record()
	if err != nil {
		t.Fatal(err)
	}
	err = p.SelectOneRecordByPK(personTable, 32, newRec)
	if err == nil {
		t.Fatal(err)
	}

	// Should succeed
	t.Log(newRec)
	newRec, err = personTable.Record()
	if err != nil {
		t.Fatal(err)
	}
	err = p.SelectOneRecordByPK(personTable, 42, newRec)
	if err != nil {
		t.Fatal(err)
	}

	if newRec == nil {
		t.Fatal(errors.New("selected record is nil"))
	}
	t.Log(newRec)
	newRec, err = personTable.Record()
	if err != nil {
		t.Fatal(err)
	}
	err = p.SelectOneRecordByPK(personTable, 49, newRec)
	if err != nil {
		t.Fatal(err)
	}

	if newRec == nil {
		t.Fatal(errors.New("selected record is nil"))
	}
	t.Log(newRec)
}

func TestPersist_InsertWithBeginTx(t *testing.T) {
	dialect, err := NewDialectSqlite3()
	if err != nil {
		t.Fatal(err)
	}
	db, err := dialect.OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	p, err := NewPersister(dialect, db, 0)

	personTable, _, _, _, err := personTableFull(dialect)
	if err != nil {
		t.Fatal(err)
	}

	// Make person table
	err = p.CreateTables(personTable)
	if err != nil {
		t.Fatal(err)
	}
	// Start transaction
	err = p.TxBegin(personTable)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := p.TxCommit(personTable)
		if err != nil {
			t.Fatal(err)
		}
	}()
	rec, err := personTable.Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(0, uint64(42)); err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(1, "Bill"); err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(2, true); err != nil {
		t.Fatal(err)
	}

	// rec := Record{
	// 	table:  personTable,
	// 	values: []*any{uint32(42), "Bill", true},
	// }

	err = p.Insert(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err = rec.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(0, uint64(49)); err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(1, "Harry"); err != nil {
		t.Fatal(err)
	}
	if err := rec.SetAt(2, false); err != nil {
		t.Fatal(err)
	}
	err = p.Insert(rec)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(personTable.selectOneRecordByPKPreparedStatementSql)

	// Added: 42; Select'ing: 32: should fail
	newRec, err := personTable.Record()
	if err != nil {
		t.Fatal(err)
	}
	err = p.SelectOneRecordByPK(personTable, 32, newRec)
	if err == nil {
		t.Fatal(err)
	}

	// Should succeed: selecting 42
	t.Log(newRec)
	newRec, err = personTable.Record()
	if err != nil {
		t.Fatal(err)
	}
	err = p.SelectOneRecordByPK(personTable, 42, newRec)
	if err != nil {
		t.Fatal(err)
	}
	if newRec == nil {
		t.Fatal(errors.New("selected record is nil"))
	}
	t.Log(newRec)

	newRec, err = personTable.Record()
	if err != nil {
		t.Fatal(err)
	}
	err = p.SelectOneRecordByPK(personTable, 49, newRec)
	if err != nil {
		t.Fatal(err)
	}

	if newRec == nil {
		t.Fatal(errors.New("selected record is nil"))
	}
	t.Log(newRec)
}
