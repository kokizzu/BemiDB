package main

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

type SchemaTable struct {
	Schema string
	Table  string
}

func (schemaTable SchemaTable) String() string {
	return schemaTable.Schema + "." + schemaTable.Table
}
