package gouin

import (
	"errors"
	"fmt"
)

func NewJoinTable2(leftTable, rightTable *Table, additionalFields []*Field, rightTableIDCacheKeyFields ...*Field) (*Table, error) {
	if err := errorsNewJoinTable2(leftTable, rightTable, additionalFields, rightTableIDCacheKeyFields...); err != nil {
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

	// if additionalFields != nil {
	// 	for i, _ := range additionalFields {
	// 		af := additionalFields[i]
	// 		if af == nil {
	// 			return nil, errors.New("Additional field is nil")
	// 		}
	// 		err := jt.AddField(af)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 	}
	// }
	// Join info
	jtInfo := new(JoinTableInfo)
	jt.joinTableInfo = jtInfo
	jtInfo.leftTable = leftTable
	jtInfo.rightTable = rightTable
	jtInfo.rightTableIDCacheKeyFields = rightTableIDCacheKeyFields
	jtInfo.rightTableIDCache = make(map[string]uint64)

	return jt, nil
}

func errorsNewJoinTable2(leftTable, rightTable *Table, additionalFields []*Field, keyFields ...*Field) error {
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
