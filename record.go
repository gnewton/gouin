package gouin

import (
	"errors"
	"strconv"
)

type Record struct {
	table     *Table
	values    []any
	outValues []any
}

func (r *Record) initialize(initializeValues bool) error {
	if r.table == nil {
		return errors.New("Table is nil")
	}

	if r.table.fields == nil {
		return errors.New("Table.fields is nil")
	}

	if len(r.table.fields) == 0 {
		return errors.New("Table.fields is len 0")
	}

	if initializeValues {
		r.values = make([]any, len(r.table.fields))
		r.outValues = make([]any, len(r.table.fields))
		for i, _ := range r.values {
			r.outValues[i] = &r.values[i]
		}
	}
	return nil
}

func (r *Record) Reset() error {
	if r.values == nil {
		return errors.New("values is nil")
	}
	for i := 0; i < len(r.values); i++ {
		r.values[i] = nil
	}
	return nil
}
func (r *Record) GetValue(f *Field) (any, error) {
	if f == nil {
		return nil, errors.New("field is nil")
	}
	positionInTable := f.positionInTable
	if positionInTable < 0 {
		return nil, errors.New("positionInTable index is < 0")
	}
	return r.values[positionInTable], nil

}
func (r *Record) SetAt(i int, v any) error {
	if r.table == nil {
		return errors.New("Table is nil")
	}

	if i < 0 {
		return errors.New("Index < 0")
	}

	if r.values == nil {
		r.initialize(true)
	}

	if i >= len(r.values) {
		return errors.New("Out of bounds")
	}

	if err := r.table.fields[i].CheckValueType(v); err != nil {
		return err
	}

	r.values[i] = v
	return nil
}

func (r *Record) Set(f *Field, v any) error {
	if r.table == nil {
		return errors.New("Table is nil")
	}
	if r.values == nil {
		var err error
		err = r.initialize(true)
		if err != nil {
			return err
		}
	}
	if f.positionInTable > len(r.values) || f.positionInTable < 0 {
		return errors.New("Field positionInTable out of bounds:" + r.table.name + ":" + f.name + ":" + strconv.Itoa(f.positionInTable))
	}
	if err := r.table.fields[f.positionInTable].CheckValueType(v); err != nil {
		return err
	}
	r.values[f.positionInTable] = v
	return nil
}
