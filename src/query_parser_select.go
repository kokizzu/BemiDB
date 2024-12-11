package main

const (
	PG_FUNCTION_QUOTE_INDENT = "quote_ident"
)

type QueryParserSelect struct {
	config *Config
	utils  *QueryUtils
}

func NewQueryParserSelect(config *Config) *QueryParserSelect {
	return &QueryParserSelect{config: config, utils: NewQueryUtils(config)}
}

// quote_ident()
func (parser *QueryParserSelect) IsQuoteIdentFunction(functionName string) bool {
	return functionName == PG_FUNCTION_QUOTE_INDENT
}
