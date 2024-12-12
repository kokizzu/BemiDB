package main

const (
	PG_FUNCTION_QUOTE_INDENT = "quote_ident"
)

type QueryParserSelect struct {
	config *Config
	utils  *QueryParserUtils
}

func NewQueryParserSelect(config *Config) *QueryParserSelect {
	return &QueryParserSelect{config: config, utils: NewQueryParserUtils(config)}
}

// quote_ident()
func (parser *QueryParserSelect) IsQuoteIdentFunction(functionName string) bool {
	return functionName == PG_FUNCTION_QUOTE_INDENT
}
