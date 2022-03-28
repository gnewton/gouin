package gouin

import (
	"errors"
	"testing"
)

func TestJoinTableInfo_makeKey(t *testing.T) {
	dialect := new(DialectSqlite3)

	db, err := dialect.OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	p := Persister{
		dialect: dialect,
		db:      db,
	}

	personTable, pid, pname, pHasCar, err := personTableFull(dialect)
	if err != nil {
		t.Fatal(err)
	}
	// Create person table
	err = p.CreateTables(personTable)
	if err != nil {
		t.Fatal(err)
	}

	// Person
	person, err := personTable.Record()
	if err != nil {
		t.Fatal(err)
	}
	if err := person.Set(pid, uint64(42)); err != nil {
		t.Fatal(err)
	}
	if err := person.Set(pname, "Bill"); err != nil {
		t.Fatal(err)
	}
	if err := person.Set(pHasCar, true); err != nil {
		t.Fatal(err)
	}

	// Car
	carTable, carId, manufacturer, model, year, err := carTableFull(dialect)
	if err != nil {
		t.Fatal(err)
	}
	if carTable == nil || carId == nil || manufacturer == nil || model == nil || year == nil {
		t.Fatal("Car is broken")
	}
	// Create car table
	err = p.CreateTables(carTable)
	if err != nil {
		t.Fatal(err)
	}

	car, err := carRecord1(carTable, carId, manufacturer, model, year)

	joinTable, err := NewJoinTable(personTable, carTable, nil, manufacturer, model, year)
	if err != nil {
		t.Fatal(err)
	}
	// Create join table
	err = p.CreateTables(joinTable)
	if err != nil {
		t.Fatal(err)
	}

	cacheKey, err := joinTable.joinTableInfo.makeKey(car)
	if err != nil {
		t.Fatal(err)
	}
	if cacheKey != "0:Ford|1:Escort|2:1988" {
		t.Fatal(errors.New("Bad key string:" + cacheKey))
	}

	t.Log(joinTable.joinTableInfo.rightTableIDCache)

	// cache contains cachekey:rightTableKey
	rightRecordExists, rightTableId, err := joinTable.joinTableInfo.rightTableIDInCache(cacheKey)
	if err != nil {
		t.Fatal(err)
	}

	err = p.TxBegin(personTable, carTable, joinTable)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := p.tx.Commit()
		if err != nil {
			t.Fatal(err)
		}
	}()
	if !rightRecordExists {
		t.Log("key: " + cacheKey + " does not exist")
		// Save right record with rightTableId
		car.values[car.table.pk.positionInTable] = rightTableId
		err = p.Insert(car)
		if err != nil {
			t.Fatal(err)
		}

	}
	t.Logf("ID=%d", rightTableId)
	t.Logf("jt.rightTableIDCounter=%d", joinTable.joinTableInfo.rightTableIDCounter)

	t.Log(joinTable.joinTableInfo.rightTableIDCache)
	// Do it again (key should be in cache)
	rightRecordExists, rightTableId, err = joinTable.joinTableInfo.rightTableIDInCache(cacheKey)
	if err != nil {
		t.Fatal(err)
	}

	if !rightRecordExists {
		rightTableId, err = joinTable.joinTableInfo.newRightTableKey(cacheKey)
		if err != nil {
			t.Fatal(err)
		}
		rightRecordExists = true
	}

	// key should now be in the map
	if rightRecordExists {
		t.Log("key: " + cacheKey + " does exists; we just added it;")
	} else {

		t.Fatal("key: " + cacheKey + " does not exist: should")
	}
	t.Logf("ID=%d", rightTableId)
	t.Logf("jt.rightTableIDCounter=%d", joinTable.joinTableInfo.rightTableIDCounter)

	// Create join record
	joinRecord, err := joinTable.Record()
	if err != nil {
		t.Fatal(err)
	}
	// person ID
	err = joinRecord.SetAt(0, person.values[joinTable.joinTableInfo.leftTable.pk.positionInTable])
	if err != nil {
		t.Fatal(err)
	}
	// carId
	err = joinRecord.SetAt(1, rightTableId)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(cacheKey)
	// Save join record

}
