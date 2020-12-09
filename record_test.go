package gouin

import (
	"testing"
)

// Failing tests
func TestRecord_AddAt_IndexTooLargeLimit(t *testing.T) {
	tab, _, _, _, err := personTableFull(new(DialectSqlite3))
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tab.Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.AddAt(3, uint32(45)); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_Add_FieldPositionInTableTooLarge(t *testing.T) {
	_, f0, _, _ := personTable(new(DialectSqlite3))
	f0.positionInTable = 999
	rec := new(Record)

	if err := rec.Add(f0, uint32(45)); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_AddAt_IndexTooSmallLimit(t *testing.T) {
	tab, _, _, _, err := personTableFull(new(DialectSqlite3))
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tab.Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.AddAt(-1, uint32(45)); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_Add_FieldPositionInTableTooSmall(t *testing.T) {
	tab, f0, _, _ := personTable(new(DialectSqlite3))
	tab.AddField(f0)
	f0.positionInTable = -1
	rec, err := tab.Record()
	if err != nil {
		t.Fatal(err)
	}

	if err := rec.Add(f0, uint32(45)); err == nil {
		t.Fatal("Should fail")
	}
}
func TestRecord_AddAt_NilRecValues(t *testing.T) {
	rec := new(Record)
	rec.values = nil

	if err := rec.AddAt(0, uint32(45)); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_Add_NilRecValues(t *testing.T) {
	_, f0, _, _ := personTable(new(DialectSqlite3))
	rec := new(Record)
	rec.values = nil

	if err := rec.Add(f0, uint32(45)); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_Add_NonUint32Int(t *testing.T) {
	tab, f0, _, _, err := personTableFull(new(DialectSqlite3))
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tab.Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Add(f0, 45); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_AddAt_NonUint32Int(t *testing.T) {
	tab, _, _, _, err := personTableFull(new(DialectSqlite3))
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tab.Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.AddAt(0, 45); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_Add_NonText(t *testing.T) {
	tab, _, f1, _, err := personTableFull(new(DialectSqlite3))
	if err != nil {
		t.Fatal(err)
	}
	rec, err := tab.Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.Add(f1, 45); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_AddAt_NonText(t *testing.T) {
	tab, _, _, _, err := personTableFull(new(DialectSqlite3))
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tab.Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.AddAt(1, 45); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_AddAt_NegIndex(t *testing.T) {
	tab, _, _, _, err := personTableFull(new(DialectSqlite3))
	if err != nil {
		t.Fatal(err)
	}

	rec, err := tab.Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := rec.AddAt(-1, 45); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_Add_NilTable(t *testing.T) {
	_, f0, _, _ := personTable(new(DialectSqlite3))
	rec := new(Record)

	if err := rec.Add(f0, uint32(45)); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_AddAt_NilTable(t *testing.T) {
	rec := new(Record)

	if err := rec.AddAt(0, uint32(45)); err == nil {
		t.Fatal("Should fail")
	}
}

func TestRecord_Initialize_NilTable(t *testing.T) {
	r := Record{
		table: nil,
	}
	if err := r.initialize(true); err == nil {
		t.Fatal()
	}
}

func TestRecord_Initialize_TableFieldsNil(t *testing.T) {
	r := Record{
		table: new(Table),
	}
	if err := r.initialize(true); err == nil {
		t.Fatal()
	}
}

func TestRecord_Initialize_TableFieldsZeroLen(t *testing.T) {
	r := Record{
		table: new(Table),
	}
	r.table.fields = make([]*Field, 0)
	if err := r.initialize(true); err == nil {
		t.Fatal()
	}
}

func TestRecord_Reset_RecordValuesNil(t *testing.T) {
	r := Record{
		table: new(Table),
	}
	r.values = nil
	if err := r.Reset(); err == nil {
		t.Fatal()
	}
}

//////////////////////////////////////////////////////////////////////
// Positive tests

//////////////////////////////////////////////////////////////////////

func carRecord1(carTable *Table, carId, manufacturer, model, year *Field) (*Record, error) {
	car, err := carTable.Record()
	if err != nil {
		return nil, err
	}
	if err := car.Add(carId, uint64(17)); err != nil {
		return nil, err
	}
	if err := car.Add(manufacturer, "Ford"); err != nil {
		return nil, err
	}

	if err := car.Add(model, "Escort"); err != nil {
		return nil, err
	}
	if err := car.Add(year, uint32(1988)); err != nil {
		return nil, err
	}
	return car, nil

}
