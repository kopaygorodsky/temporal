// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sql

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/temporalio/sqlparser"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/persistence/visibility/store/query"
	"go.temporal.io/server/common/searchattribute"
)

type (
	// Custom expression types for Oracle
	jsonExistsExpr struct {
		sqlparser.Expr
		JSONDoc      sqlparser.Expr
		JSONPathExpr sqlparser.Expr
	}

	oracleQueryConverter struct{}
)

var _ sqlparser.Expr = (*jsonExistsExpr)(nil)
var _ pluginQueryConverter = (*oracleQueryConverter)(nil)

// Format implements the sqlparser.Expr interface for jsonExistsExpr
func (node *jsonExistsExpr) Format(buf *sqlparser.TrackedBuffer) {
	buf.Myprintf("JSON_EXISTS(%v, %v)", node.JSONDoc, node.JSONPathExpr)
}

// newOracleQueryConverter creates a new query converter for Oracle database
func newOracleQueryConverter(
	namespaceName namespace.Name,
	namespaceID namespace.ID,
	saTypeMap searchattribute.NameTypeMap,
	saMapper searchattribute.Mapper,
	queryString string,
) *QueryConverter {
	return newQueryConverterInternal(
		&oracleQueryConverter{},
		namespaceName,
		namespaceID,
		saTypeMap,
		saMapper,
		queryString,
	)
}

// getDatetimeFormat returns the datetime format for Oracle
// @todo I just have Oracle: https://stackoverflow.com/questions/8855378/oracle-sql-timestamps-in-where-clause
// so let's output standard golang time format here as this method is used before building actual statement and then we will catch it with regexp.
// it's 4 in the morning, whatever
// the idea of plugins have to go in the future, in our own extension it will be much simpler.
func (c *oracleQueryConverter) getDatetimeFormat() string {
	return time.RFC3339Nano
}

// getCoalesceCloseTimeExpr returns the expression for coalesced close time
func (c *oracleQueryConverter) getCoalesceCloseTimeExpr() sqlparser.Expr {
	return newFuncExpr(
		"get_close_time_or_max",
		closeTimeSaColName,
	)
}

// convertKeywordListComparisonExpr converts a keyword list comparison to Oracle syntax
func (c *oracleQueryConverter) convertKeywordListComparisonExpr(
	expr *sqlparser.ComparisonExpr,
) (sqlparser.Expr, error) {
	if !isSupportedKeywordListOperator(expr.Operator) {
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}

	var negate bool
	var newExpr sqlparser.Expr

	switch expr.Operator {
	case sqlparser.EqualStr, sqlparser.NotEqualStr:
		// For equality, use JSON_EXISTS to check if value is in the array
		// In Oracle: JSON_EXISTS(column, '$[*]?(@==value)')
		rightVal := sqlparser.String(expr.Right)
		// Remove quotes if present for proper JSON path syntax
		if strings.HasPrefix(rightVal, "'") && strings.HasSuffix(rightVal, "'") {
			rightVal = rightVal[1 : len(rightVal)-1]
			newExpr = &jsonExistsExpr{
				JSONDoc:      expr.Left,
				JSONPathExpr: newUnsafeSQLString(fmt.Sprintf("'$[*]?(@ == \"%s\")'", rightVal)),
			}
		} else {
			newExpr = &jsonExistsExpr{
				JSONDoc:      expr.Left,
				JSONPathExpr: newUnsafeSQLString(fmt.Sprintf("'$[*]?(@ == %s)'", rightVal)),
			}
		}
		negate = expr.Operator == sqlparser.NotEqualStr

	case sqlparser.InStr, sqlparser.NotInStr:
		// For IN operator, we need to check if any of the values exist in the array
		valTuple, isValTuple := expr.Right.(sqlparser.ValTuple)
		if !isValTuple {
			return nil, query.NewConverterError(
				"%s: unexpected value type (expected tuple of strings, got %s)",
				query.InvalidExpressionErrMessage,
				sqlparser.String(expr.Right),
			)
		}

		values, err := getUnsafeStringTupleValues(valTuple)
		if err != nil {
			return nil, err
		}

		// Build a condition for each value
		var conditions []sqlparser.Expr
		for _, val := range values {
			condition := &jsonExistsExpr{
				JSONDoc:      expr.Left,
				JSONPathExpr: newUnsafeSQLString(fmt.Sprintf("'$[*]?(@ == \"%s\")'", val)),
			}
			conditions = append(conditions, condition)
		}

		// Combine conditions with OR
		if len(conditions) == 1 {
			newExpr = conditions[0]
		} else {
			newExpr = conditions[0]
			for i := 1; i < len(conditions); i++ {
				newExpr = &sqlparser.OrExpr{
					Left:  newExpr,
					Right: conditions[i],
				}
			}
		}

		negate = expr.Operator == sqlparser.NotInStr

	default:
		// This should never happen since isSupportedKeywordListOperator should already fail
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for KeywordList type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}

	if negate {
		newExpr = &sqlparser.NotExpr{Expr: newExpr}
	}
	return newExpr, nil
}

// convertTextComparisonExpr converts a text comparison to Oracle syntax
func (c *oracleQueryConverter) convertTextComparisonExpr(
	expr *sqlparser.ComparisonExpr,
) (sqlparser.Expr, error) {
	if !isSupportedTextOperator(expr.Operator) {
		return nil, query.NewConverterError(
			"%s: operator '%s' not supported for Text type search attribute in `%s`",
			query.InvalidExpressionErrMessage,
			expr.Operator,
			formatComparisonExprStringForError(*expr),
		)
	}

	// For text search in Oracle, we'll use LIKE with wildcards
	var newExpr sqlparser.Expr

	if expr.Operator == sqlparser.EqualStr {
		// Convert to LIKE with % wildcards
		newExpr = &sqlparser.ComparisonExpr{
			Operator: "like",
			Left:     expr.Left,
			Right: &sqlparser.BinaryExpr{
				Operator: "||",
				Left: &sqlparser.BinaryExpr{
					Operator: "||",
					Left:     newUnsafeSQLString("'%'"),
					Right:    expr.Right,
				},
				Right: newUnsafeSQLString("'%'"),
			},
		}
	} else { // NotEqualStr
		// NOT LIKE with % wildcards
		newExpr = &sqlparser.NotExpr{
			Expr: &sqlparser.ComparisonExpr{
				Operator: "like",
				Left:     expr.Left,
				Right: &sqlparser.BinaryExpr{
					Operator: "||",
					Left: &sqlparser.BinaryExpr{
						Operator: "||",
						Left:     newUnsafeSQLString("'%'"),
						Right:    expr.Right,
					},
					Right: newUnsafeSQLString("'%'"),
				},
			},
		}
	}

	return newExpr, nil
}

// buildSelectStmt builds a SELECT statement for Oracle
func (c *oracleQueryConverter) buildSelectStmt(
	namespaceID namespace.ID,
	queryString string,
	pageSize int,
	token *pageToken,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any
	paramIndex := 1

	whereClauses = append(
		whereClauses,
		fmt.Sprintf("ev.%s = :%d", searchattribute.GetSqlDbColName(searchattribute.NamespaceID), paramIndex),
	)
	paramIndex++
	queryArgs = append(queryArgs, namespaceID.String())

	if len(queryString) > 0 {
		// Process query for Oracle (add table aliases and convert datetime literals)
		processedQuery, err := c.processQueryForOracle(queryString, &queryArgs, &paramIndex)
		if err != nil {
			// @todo buildSelectStmt does not support error handling. Will be fixes later in the main repo when moving to a separate extension
			return "", queryArgs
		}

		whereClauses = append(whereClauses, processedQuery)
	}

	if token != nil {
		whereClauses = append(
			whereClauses,
			fmt.Sprintf(
				"((%s = :%d AND ev.%s = :%d AND ev.%s > :%d) OR (%s = :%d AND ev.%s < :%d) OR %s < :%d)",
				sqlparser.String(c.getCoalesceCloseTimeExpr()),
				paramIndex,
				searchattribute.GetSqlDbColName(searchattribute.StartTime),
				paramIndex+1,
				searchattribute.GetSqlDbColName(searchattribute.RunID),
				paramIndex+2,
				sqlparser.String(c.getCoalesceCloseTimeExpr()),
				paramIndex+3,
				searchattribute.GetSqlDbColName(searchattribute.StartTime),
				paramIndex+4,
				sqlparser.String(c.getCoalesceCloseTimeExpr()),
				paramIndex+5,
			),
		)
		paramIndex += 6
		queryArgs = append(
			queryArgs,
			token.CloseTime,
			token.StartTime,
			token.RunID,
			token.CloseTime,
			token.StartTime,
			token.CloseTime,
		)
	}

	// Replace "_version" with "version_num" in the fields
	fields := make([]string, len(sqlplugin.DbFields))
	for i, field := range sqlplugin.DbFields {
		if field == "_version" {
			fields[i] = "version_num"
		} else {
			fields[i] = field
		}
	}

	// Oracle uses FETCH FIRST n ROWS ONLY instead of LIMIT
	queryArgs = append(queryArgs, pageSize)

	resQuery := fmt.Sprintf(
		`SELECT %s
        FROM executions_visibility ev
        LEFT JOIN custom_search_attributes csa
        ON ev.%s = csa.%s AND ev.%s = csa.%s
        WHERE %s
        ORDER BY get_close_time_or_max(ev.close_time) DESC, ev.%s DESC, ev.%s
        FETCH FIRST :%d ROWS ONLY`,
		strings.Join(addPrefix("ev.", fields), ", "),
		searchattribute.GetSqlDbColName(searchattribute.NamespaceID),
		searchattribute.GetSqlDbColName(searchattribute.NamespaceID),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
		strings.Join(whereClauses, " AND "),
		searchattribute.GetSqlDbColName(searchattribute.StartTime),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
		paramIndex,
	)

	fmt.Println(resQuery)

	return resQuery, queryArgs
}

// buildCountStmt builds a COUNT statement for Oracle
func (c *oracleQueryConverter) buildCountStmt(
	namespaceID namespace.ID,
	queryString string,
	groupBy []string,
) (string, []any) {
	var whereClauses []string
	var queryArgs []any
	paramIndex := 1

	whereClauses = append(
		whereClauses,
		fmt.Sprintf("(ev.%s = :%d)", searchattribute.GetSqlDbColName(searchattribute.NamespaceID), paramIndex),
	)
	paramIndex++
	queryArgs = append(queryArgs, namespaceID.String())

	if len(queryString) > 0 {
		// Process query for Oracle (add table aliases and convert datetime literals)
		processedQuery, err := c.processQueryForOracle(queryString, &queryArgs, &paramIndex)
		if err != nil {
			// @todo buildSelectStmt does not support error handling. Will be fixes later in the main repo when moving to a separate extension
			return "", queryArgs
		}

		whereClauses = append(whereClauses, processedQuery)
	}

	// Ensure all groupBy columns have explicit table aliases
	prefixedGroupBy := make([]string, len(groupBy))
	for i, col := range groupBy {
		if col == "status" {
			// For status column, cast to number to ensure Oracle returns it as a number
			prefixedGroupBy[i] = "TO_NUMBER(ev." + col + ")"
		} else {
			// For other columns, just add the ev. prefix
			prefixedGroupBy[i] = "ev." + col
		}
	}

	groupByClause := ""
	if len(prefixedGroupBy) > 0 {
		groupByClause = fmt.Sprintf("GROUP BY %s", strings.Join(prefixedGroupBy, ", "))
	}

	// Create select fields with proper aliases
	selectItems := make([]string, 0, len(groupBy)+1)
	for _, col := range groupBy {
		if col == "status" {
			selectItems = append(selectItems, "TO_NUMBER(ev."+col+") as "+col)
		} else {
			selectItems = append(selectItems, "ev."+col)
		}
	}
	selectItems = append(selectItems, "COUNT(*)")

	resQuery := fmt.Sprintf(
		`SELECT %s
        FROM executions_visibility ev
        LEFT JOIN custom_search_attributes csa
        ON ev.%s = csa.%s AND ev.%s = csa.%s
        WHERE %s
        %s`,
		strings.Join(selectItems, ", "),
		searchattribute.GetSqlDbColName(searchattribute.NamespaceID),
		searchattribute.GetSqlDbColName(searchattribute.NamespaceID),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
		strings.Join(whereClauses, " AND "),
		groupByClause,
	)

	return resQuery, queryArgs
}

// processColumnRef adds table aliases to column references
func (c *oracleQueryConverter) processColumnRef(exprRef *sqlparser.Expr) error {
	if exprRef == nil || *exprRef == nil {
		return nil
	}

	// Handle column references
	if colName, ok := (*exprRef).(*sqlparser.ColName); ok {
		colNameStr := colName.Name.String()

		// Get the SQL DB column name (for system attributes this will be different)
		sqlDbColName := searchattribute.GetSqlDbColName(colNameStr)

		// Check if this is a custom search attribute
		customAttrs := searchattribute.GetSqlDbIndexSearchAttributes().CustomSearchAttributes
		_, isCustom := customAttrs[colNameStr]

		// Decide which table to use based on the attribute type
		if isCustom {
			// Custom search attributes are in custom_search_attributes table
			*exprRef = &sqlparser.ColName{
				Name:      colName.Name,
				Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("csa")},
			}
		} else {
			// System attributes and predefined attributes are in executions_visibility table
			*exprRef = &sqlparser.ColName{
				Name:      sqlparser.NewColIdent(sqlDbColName),
				Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("ev")},
			}
		}
	}

	return nil
}

// processColumnForOracle adds proper table aliases and handles special cases like status
func (c *oracleQueryConverter) processColumnForOracle(colName string) string {
	// For status column, cast to number
	if colName == "status" || colName == searchattribute.ExecutionStatus {
		return "TO_NUMBER(ev." + searchattribute.GetSqlDbColName(colName) + ")"
	}

	// Get the SQL DB column name
	sqlDbColName := searchattribute.GetSqlDbColName(colName)

	// Check if this is a custom search attribute
	customAttrs := searchattribute.GetSqlDbIndexSearchAttributes().CustomSearchAttributes
	_, isCustom := customAttrs[colName]

	if isCustom {
		return "csa." + sqlDbColName
	} else {
		return "ev." + sqlDbColName
	}
}

func (c *oracleQueryConverter) processQueryForOracle(queryString string, queryArgs *[]any, startParamIndex *int) (string, error) {
	// First pass: Parse the query and add table aliases
	tempSQL := "SELECT * FROM table1 WHERE " + queryString
	stmt, err := sqlparser.Parse(tempSQL)
	if err != nil {
		return "", fmt.Errorf("error parsing query statement: %w", err)
	}

	// Get the expression tree and process it
	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok || selectStmt.Where == nil {
		return "", fmt.Errorf("error parsing query statement: expected a Where clause")
	}

	// Add table aliases
	if err := c.processExprForOracle(&selectStmt.Where.Expr, queryArgs, startParamIndex); err != nil {
		return "", fmt.Errorf("error processExprForOracle: %w", err)
	}

	// Get the processed WHERE clause
	processedQuery := sqlparser.String(selectStmt.Where.Expr)

	// Remove backticks from the query string
	processedQuery = strings.ReplaceAll(processedQuery, "`", "")

	// Now convert the parameter placeholders to Oracle's :n format
	return processedQuery, nil
}

// processExprForOracle recursively processes expressions for Oracle
func (c *oracleQueryConverter) processExprForOracle(exprRef *sqlparser.Expr, queryArgs *[]any, startParamIndex *int) error {
	if exprRef == nil || *exprRef == nil {
		return nil
	}

	expr := *exprRef

	switch e := expr.(type) {
	case *sqlparser.ParenExpr:
		return c.processExprForOracle(&e.Expr, queryArgs, startParamIndex)

	case *sqlparser.AndExpr:
		if err := c.processExprForOracle(&e.Left, queryArgs, startParamIndex); err != nil {
			return err
		}
		return c.processExprForOracle(&e.Right, queryArgs, startParamIndex)

	case *sqlparser.OrExpr:
		if err := c.processExprForOracle(&e.Left, queryArgs, startParamIndex); err != nil {
			return err
		}
		return c.processExprForOracle(&e.Right, queryArgs, startParamIndex)

	case *sqlparser.NotExpr:
		return c.processExprForOracle(&e.Expr, queryArgs, startParamIndex)

	case *sqlparser.ComparisonExpr:
		// Process left side for table aliases
		if err := c.processColumnRef(&e.Left); err != nil {
			return err
		}

		// Process right side for datetime literals
		return c.processValueExpr(&e.Right, queryArgs, startParamIndex)

	case *sqlparser.RangeCond:
		if err := c.processColumnRef(&e.Left); err != nil {
			return err
		}
		if err := c.processValueExpr(&e.From, queryArgs, startParamIndex); err != nil {
			return err
		}
		return c.processValueExpr(&e.To, queryArgs, startParamIndex)

	case *sqlparser.IsExpr:
		return c.processColumnRef(&e.Expr)

	case *sqlparser.FuncExpr:
		if strings.EqualFold(e.Name.String(), "get_close_time_or_max") {
			for i := range e.Exprs {
				switch arg := e.Exprs[i].(type) {
				case *sqlparser.AliasedExpr:
					if colName, ok := arg.Expr.(*sqlparser.ColName); ok {
						if strings.EqualFold(colName.Name.String(), "close_time") {
							arg.Expr = &sqlparser.ColName{
								Name:      colName.Name,
								Qualifier: sqlparser.TableName{Name: sqlparser.NewTableIdent("ev")},
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// processValueExpr converts RFC3339 datetime literals to Oracle timestamps
func (c *oracleQueryConverter) processValueExpr(exprRef *sqlparser.Expr, queryArgs *[]any, startParamIndex *int) error {
	if exprRef == nil || *exprRef == nil {
		return nil
	}

	// Handle string literals that might contain datetime values or ?
	if sqlVal, ok := (*exprRef).(*sqlparser.SQLVal); ok {
		if sqlVal.Type == sqlparser.StrVal {
			// Get the string value
			strVal := string(sqlVal.Val)

			// Check if this is an RFC3339 datetime string
			if isRFC3339DateTimeString(strVal) {
				parsedTime, err := time.Parse(time.RFC3339Nano, strVal)
				if err != nil {
					return fmt.Errorf("failed to parse RFC3339 timestamp: %v", err)
				}

				*queryArgs = append(*queryArgs, parsedTime)

				*exprRef = sqlparser.NewValArg([]byte(fmt.Sprintf(":%d", *startParamIndex)))
				*startParamIndex++
			}

			if strVal == "?" {
				*exprRef = sqlparser.NewValArg([]byte(fmt.Sprintf(":%d", *startParamIndex)))
				*startParamIndex++
			}
		}
	}

	return nil
}

// isRFC3339DateTimeString checks if a string matches RFC3339 datetime format
func isRFC3339DateTimeString(s string) bool {
	// Strip any surrounding quotes that might be present
	s = strings.Trim(s, "'\"")

	// More comprehensive regex for RFC3339 datetime format
	// Handles optional fractional seconds and timezone offset
	rfc3339Pattern := regexp.MustCompile(
		`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})$`,
	)

	return rfc3339Pattern.MatchString(s)
}
