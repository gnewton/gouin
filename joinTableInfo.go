package gouin

import (
	"errors"
	"fmt"
	"strconv"
)

type JoinTableInfo struct {
	leftTable, rightTable      *Table
	rightTableIDCache          map[string]uint64
	rightTableIDCacheKeyFields []*Field
	rightTableIDCounter        uint64
}

const SEPARATOR = "|"

func (jt *JoinTableInfo) rightTableIDInCache(key string) (bool, uint64, error) {
	if key == "" {
		return false, 0, fmt.Errorf("key is empty; jf leftTable[%s] rightTable[%s]", jt.leftTable, jt.rightTable)
	}

	if jt.rightTableIDCache == nil {
		return false, 0, fmt.Errorf("rightTableIDCache is nil; jf leftTable[%s] rightTable[%s]", jt.leftTable, jt.rightTable)
	}

	id, exists := jt.rightTableIDCache[key]

	if !exists {
		// New right table record
		id = jt.rightTableIDCounter
		jt.rightTableIDCache[key] = jt.rightTableIDCounter
		jt.rightTableIDCounter++
	}

	return exists, id, nil
}

func (jt *JoinTableInfo) makeKey(rec *Record) (string, error) {
	if jt.rightTableIDCacheKeyFields == nil {
		return "", errors.New("rightTableIDCacheKeyFields is nil")
	}
	if len(jt.rightTableIDCacheKeyFields) == 0 {
		return "", errors.New("rightTableIDCacheKeyFields is len 0")
	}

	var key string
	for i, _ := range jt.rightTableIDCacheKeyFields {
		if jt.rightTableIDCacheKeyFields[i] == nil {
			return "", errors.New("rightTableIDCacheKeyFields field is nil")
		}
		field := jt.rightTableIDCacheKeyFields[i]
		fieldType := field.typ
		fieldValue := rec.values[field.positionInTable]
		if i != 0 {
			key += SEPARATOR
		}
		valueString, err := fieldType.ValueToString(field, fieldValue)
		if err != nil {
			return "", err
		}
		key += (strconv.Itoa(i) + ":" + valueString)
	}
	return key, nil
}
