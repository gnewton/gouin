package gouin

import (
	"database/sql"
	"errors"
	"fmt"
)

type Table struct {
	fields                                  []*Field
	name                                    string
	pk                                      *Field
	fieldMap                                map[string]struct{}
	fieldCounter                            int
	dialect                                 Dialect
	insertPreparedStatement                 *sql.Stmt
	insertPreparedStatementSql              string
	deleteByPKPreparedStatement             *sql.Stmt
	deleteByPKPreparedStatementSql          string
	selectOneRecordByPKPreparedStatement    *sql.Stmt
	selectOneRecordByPKPreparedStatementSql string
	joinTablesMap                           map[string]*Table // if this has JoinTables
	joinTableInfo                           *JoinTableInfo    // if this is a JoinTable

}

func (t *Table) String() string {
	return t.name
}

func (t *Table) SetPrimaryKey(pk *Field) error {
	if pk == nil {
		return fmt.Errorf("Table [%s]: pk is nil", t.name)
	}
	if pk.typ != Uint64 {
		return fmt.Errorf("Table [%s] Field [%s]: Primary key is not uint64; is %s", t, pk.name, pk.typ.String())
	}

	t.pk = pk
	return nil
}

func (t *Table) AddField(f *Field) error {
	if f == nil {
		return errors.New("Field is nil; table is " + t.name)
	}
	if f.name == "" {
		return errors.New("Field is empty; table is " + t.name)
	}
	if t.fieldMap == nil {
		t.fieldMap = make(map[string]struct{})
	}
	if _, ok := t.fieldMap[f.name]; ok {
		return errors.New("Field with that name already exists: " + f.name)
	} else {
		t.fieldMap[f.name] = struct{}{}
	}

	t.fields = append(t.fields, f)
	f.positionInTable = t.fieldCounter
	t.fieldCounter++
	return nil
}

func (t *Table) Record() (*Record, error) {
	rec := Record{
		table: t,
	}
	err := rec.initialize(true)

	return &rec, err
}

// Returns the newly create join record and (if does not already exist) the right record (which is also passed in)
// Whatever records are return are to be saved
//
func (t *Table) JoinRecords(leftR, rightR *Record, joinTable *Table, additionalJTfields *Field) ([]*Record, error) {
	if t.joinTableInfo == nil {
		return nil, fmt.Errorf("Table [%s]: is not a join table (joinTableInfo is nil)", t.name)
	}

	if leftR == nil {
		return nil, fmt.Errorf("Table [%s]: left record is nil)", t.name)
	}

	if rightR == nil {
		return nil, fmt.Errorf("Table [%s]: right record is nil)", t.name)
	}

	if leftR.table != t.joinTableInfo.leftTable {
		return nil, fmt.Errorf("Table [%s]: left record table [%s] does not equal jointTableInfo left table [%s])",
			t.name, leftR.table, t.joinTableInfo.leftTable)
	}

	if rightR.table != t.joinTableInfo.rightTable {
		return nil, fmt.Errorf("Table [%s]: right record table [%s] does not equal jointTableInfo right table [%s])",
			t.name, rightR.table, t.joinTableInfo.rightTable)
	}

	// Make the key from the defined fields in the right record
	cacheKey, err := joinTable.joinTableInfo.makeKey(rightR)
	if err != nil {
		return nil, err
	}

	// The new join record and (if needed) the right join record
	recordsToSave := make([]*Record, 0)

	// Does the join (right) record already exist in the table? Look in the cache
	rightRecordExists, rightTableId, err := t.joinTableInfo.rightTableIDInCache(cacheKey)
	if err != nil {
		return nil, err
	}

	if !rightRecordExists {
		rightTableId, err = t.joinTableInfo.newRightTableKey(cacheKey)
	}

	// Record does not already exist; assign the PK; add it to be saved
	if !rightRecordExists {
		rightR.values[rightR.table.pk.positionInTable] = rightTableId
		recordsToSave = append(recordsToSave, rightR)
	}

	// Create join record
	joinRecord, err := joinTable.Record()
	if err != nil {
		return nil, err
	}

	// left record PK
	err = joinRecord.SetAt(0, leftR.values[joinTable.joinTableInfo.leftTable.pk.positionInTable])
	if err != nil {
		return nil, err
	}
	// right record PK
	err = joinRecord.SetAt(1, rightTableId)
	if err != nil {
		return nil, err
	}

	recordsToSave = append(recordsToSave, joinRecord)

	return recordsToSave, nil
}
