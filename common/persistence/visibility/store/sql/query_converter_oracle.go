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
	"strings"

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
func (c *oracleQueryConverter) getDatetimeFormat() string {
	return "2006-01-02T15:04:05.999999Z07:00" // ISO 8601 format
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
		fmt.Sprintf("%s = :%d", searchattribute.GetSqlDbColName(searchattribute.NamespaceID), paramIndex),
	)
	paramIndex++
	queryArgs = append(queryArgs, namespaceID.String())

	if len(queryString) > 0 {
		// Replace all ? in queryString with :n parameters
		parts := strings.Split(queryString, "?")
		if len(parts) > 1 {
			newQueryString := parts[0]
			for i := 1; i < len(parts); i++ {
				newQueryString += fmt.Sprintf(":%d", paramIndex)
				paramIndex++
				newQueryString += parts[i]
			}
			queryString = newQueryString
		}
		whereClauses = append(whereClauses, queryString)
	}

	if token != nil {
		whereClauses = append(
			whereClauses,
			fmt.Sprintf(
				"((%s = :%d AND %s = :%d AND %s > :%d) OR (%s = :%d AND %s < :%d) OR %s < :%d)",
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

	// Replace "version" with "version_num" in the fields
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
		ON ev.namespace_id = csa.%s AND ev.run_id = csa.%s
		WHERE %s
		ORDER BY %s DESC, %s DESC, %s
		FETCH FIRST :%d ROWS ONLY`,
		strings.Join(addPrefix("ev.", fields), ", "),
		searchattribute.GetSqlDbColName(searchattribute.NamespaceID),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
		strings.Join(whereClauses, " AND "),
		sqlparser.String(c.getCoalesceCloseTimeExpr()),
		searchattribute.GetSqlDbColName(searchattribute.StartTime),
		addSinglePrefix("csa.", searchattribute.GetSqlDbColName(searchattribute.RunID)),
		paramIndex,
	)

	return resQuery, queryArgs
}

func addSinglePrefix(prefix, column string) string {
	return prefix + column
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
		fmt.Sprintf("(%s = :%d)", searchattribute.GetSqlDbColName(searchattribute.NamespaceID), paramIndex),
	)
	paramIndex++
	queryArgs = append(queryArgs, namespaceID.String())

	if len(queryString) > 0 {
		// Replace all ? in queryString with :n parameters
		parts := strings.Split(queryString, "?")
		if len(parts) > 1 {
			newQueryString := parts[0]
			for i := 1; i < len(parts); i++ {
				newQueryString += fmt.Sprintf(":%d", paramIndex)
				paramIndex++
				newQueryString += parts[i]
			}
			queryString = newQueryString
		}
		whereClauses = append(whereClauses, queryString)
	}

	groupByClause := ""
	if len(groupBy) > 0 {
		groupByClause = fmt.Sprintf("GROUP BY %s", strings.Join(groupBy, ", "))
	}

	resQuery := fmt.Sprintf(
		`SELECT %s
		FROM executions_visibility ev
		LEFT JOIN custom_search_attributes csa
		ON ev.namespace_id = csa.%s AND ev.run_id = csa.%s
		WHERE %s
		%s`,
		strings.Join(append(groupBy, "COUNT(*)"), ", "),
		searchattribute.GetSqlDbColName(searchattribute.NamespaceID),
		searchattribute.GetSqlDbColName(searchattribute.RunID),
		strings.Join(whereClauses, " AND "),
		groupByClause,
	)

	return resQuery, queryArgs
}
