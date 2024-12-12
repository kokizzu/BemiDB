package main

import (
	"fmt"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type OrderedMap struct {
	valueByKey  map[string]string
	orderedKeys []string
}

func NewOrderedMap(keyVals [][]string) *OrderedMap {
	orderedMap := &OrderedMap{
		valueByKey:  make(map[string]string),
		orderedKeys: make([]string, 0),
	}

	for _, keyVal := range keyVals {
		orderedMap.Set(keyVal[0], keyVal[1])
	}

	return orderedMap
}

func (orderedMap *OrderedMap) Get(key string) string {
	return orderedMap.valueByKey[key]
}

func (orderedMap *OrderedMap) HasKey(key string) bool {
	_, ok := orderedMap.valueByKey[key]
	return ok
}

func (orderedMap *OrderedMap) Set(key string, value string) {
	if _, ok := orderedMap.valueByKey[key]; !ok {
		orderedMap.orderedKeys = append(orderedMap.orderedKeys, key)
	}

	orderedMap.valueByKey[key] = value
}

func (orderedMap *OrderedMap) Keys() []string {
	return orderedMap.orderedKeys
}

func (orderedMap *OrderedMap) Values() []string {
	values := make([]string, 0)
	for _, key := range orderedMap.orderedKeys {
		values = append(values, orderedMap.valueByKey[key])
	}

	return values
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Set struct {
	valueByItem map[string]bool
}

func NewSet(items []string) *Set {
	set := &Set{
		valueByItem: make(map[string]bool),
	}

	for _, item := range items {
		set.Add(item)
	}

	return set
}

func (set *Set) Add(item string) {
	set.valueByItem[item] = true
}

func (set *Set) Contains(item string) bool {
	_, ok := set.valueByItem[item]
	return ok
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type SchemaTable struct {
	Schema                 string
	Table                  string
	ParentPartitionedTable string
	Alias                  string
}

func (schemaTable SchemaTable) String() string {
	return fmt.Sprintf(`"%s"."%s"`, schemaTable.Schema, schemaTable.Table)
}
