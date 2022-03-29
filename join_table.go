package gouin

import (
	"errors"
	"fmt"
)

// type JoinTable struct {
// 	Table
// 	joinTablesMap map[string]*Table // if this has JoinTables
// 	joinTableInfo *JoinTableInfo    // if this is a JoinTable
// }

// A join table is just a regular table

//func NewJoinTable(leftTable, rightTable *Table, additionalFields []*Field, rightTableIDCacheKeyFields ...*Field) (*Table, error) {
func NewJoinTable(leftTable, rightTable *Table, additionalFields []*Field, rightTableIDCacheKeyFields ...*Field) (*Table, error) {
	if err := errorsNewJoinTable(leftTable, rightTable, additionalFields, rightTableIDCacheKeyFields...); err != nil {
		return nil, err
	}

	// Field for table 1 (left table) primary key
	lf := new(Field)
	lf.typ = leftTable.pk.typ
	lf.name = leftTable.name + "_" + leftTable.pk.name

	// Field for table 2 (right table) primary key
	rf := new(Field)
	rf.typ = rightTable.pk.typ
	rf.name = rightTable.name + "_" + rightTable.pk.name

	// Join table name is :jt_leftTableName_rightTableName
	//
	jt := new(Table)
	jt.name = "jt_" + leftTable.name + "_" + rightTable.name
	jt.AddField(lf)
	jt.pk = lf
	jt.AddField(rf)

	err := addAdditionalFields(jt, additionalFields)
	if err != nil {
		return nil, err
	}

	jtInfo := new(JoinTableInfo)
	jtInfo.leftTable = leftTable
	jtInfo.rightTable = rightTable
	jtInfo.rightTableIDCacheKeyFields = rightTableIDCacheKeyFields
	jtInfo.rightTableIDCache = make(map[string]uint64)

	jt.joinTableInfo = jtInfo

	return jt, nil
}

func addAdditionalFields(jt *Table, additionalFields []*Field) error {
	for i, f := range additionalFields {
		if f == nil {
			return fmt.Errorf("additional field %d is nil", i)
		}
		err := jt.AddField(f)
		if err != nil {
			return err
		}
	}
	return nil
}

func errorsNewJoinTable(leftTable, rightTable *Table, additionalFields []*Field, keyFields ...*Field) error {
	//func errorsNewJoinTable2(leftTable, rightTable *Table, keyFields ...*Field) error {
	if leftTable == nil {
		return errors.New("left table is nil")
	}
	if rightTable == nil {
		return errors.New("right table is nil")
	}
	if leftTable.pk == nil {
		err := fmt.Errorf("left table [%s] pk is nil", leftTable)
		return err
	}
	if rightTable.pk == nil {
		err := fmt.Errorf("right table [%s] pk is nil", rightTable)
		return err
	}
	if leftTable.pk.name == "" {
		return errors.New("left table pk name is empty")
	}
	if rightTable.pk.name == "" {
		return errors.New("right table pk name is empty")

	}

	if additionalFields != nil && len(additionalFields) > 0 {
		for i, _ := range additionalFields {
			if additionalFields[i] == nil {
				err := fmt.Errorf("additional fields [%d] is nil", i)
				return err
			}
		}
	}
	return nil
}
