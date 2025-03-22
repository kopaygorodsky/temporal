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

package sqlplugin

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func appendPrefix(prefix string, fields []string) []string {
	out := make([]string, len(fields))
	for i, field := range fields {
		out[i] = prefix + field
	}
	return out
}

func BuildNamedPlaceholder(fields ...string) string {
	return strings.Join(appendPrefix(":", fields), ", ")
}

// DebugSignalRows provides comprehensive output for debugging signal rows issues
func DebugSignalRows(title string, before, after []SignalsRequestedSetsRow) {
	fmt.Printf("\n==== %s ====\n", title)

	// Print full arrays
	fmt.Println("\n[BEFORE] Full Array Contents:")
	if len(before) == 0 {
		fmt.Println("  (empty array)")
	}
	for i, row := range before {
		fmt.Printf("  [%d] ShardID: %d, NamespaceID: %s, WorkflowID: %s, RunID: %s, SignalID: %s\n",
			i, row.ShardID, row.NamespaceID, row.WorkflowID, row.RunID, row.SignalID)
		fmt.Printf("      NamespaceID bytes: %v\n", row.NamespaceID.Downcast())
		fmt.Printf("      RunID bytes: %v\n", row.RunID.Downcast())
	}

	fmt.Println("\n[AFTER] Full Array Contents:")
	if len(after) == 0 {
		fmt.Println("  (empty array)")
	}
	for i, row := range after {
		fmt.Printf("  [%d] ShardID: %d, NamespaceID: %s, WorkflowID: %s, RunID: %s, SignalID: %s\n",
			i, row.ShardID, row.NamespaceID, row.WorkflowID, row.RunID, row.SignalID)
		fmt.Printf("      NamespaceID bytes: %v\n", row.NamespaceID.Downcast())
		fmt.Printf("      RunID bytes: %v\n", row.RunID.Downcast())
	}

	// Create maps for quick lookup and tracking operations
	beforeMap := make(map[string]SignalsRequestedSetsRow)
	afterMap := make(map[string]SignalsRequestedSetsRow)

	// Helper function to create a unique key for each row
	createFullKey := func(row SignalsRequestedSetsRow) string {
		return fmt.Sprintf("%d|%s|%s|%s|%s",
			row.ShardID,
			row.NamespaceID.String(),
			row.WorkflowID,
			row.RunID.String(),
			row.SignalID,
		)
	}

	// Helper function to create a key without signal ID (for execution grouping)
	createExecKey := func(row SignalsRequestedSetsRow) string {
		return fmt.Sprintf("%d|%s|%s|%s",
			row.ShardID,
			row.NamespaceID.String(),
			row.WorkflowID,
			row.RunID.String(),
		)
	}

	// Populate maps
	for _, row := range before {
		beforeMap[createFullKey(row)] = row
	}

	for _, row := range after {
		afterMap[createFullKey(row)] = row
	}

	// Track operations by execution
	type OperationType string
	const (
		Added   OperationType = "ADDED"
		Deleted OperationType = "DELETED"
	)

	type Operation struct {
		Type     OperationType
		SignalID string
	}

	operationsByExec := make(map[string][]Operation)

	// Find deleted signals
	for key, row := range beforeMap {
		if _, exists := afterMap[key]; !exists {
			execKey := createExecKey(row)
			operationsByExec[execKey] = append(operationsByExec[execKey], Operation{
				Type:     Deleted,
				SignalID: row.SignalID,
			})
		}
	}

	// Find added signals
	for key, row := range afterMap {
		if _, exists := beforeMap[key]; !exists {
			execKey := createExecKey(row)
			operationsByExec[execKey] = append(operationsByExec[execKey], Operation{
				Type:     Added,
				SignalID: row.SignalID,
			})
		}
	}

	// Print operations by execution
	fmt.Println("\n[OPERATIONS] Changes By Execution:")
	if len(operationsByExec) == 0 {
		fmt.Println("  No changes detected (arrays are identical)")
	}

	for execKey, operations := range operationsByExec {
		// Parse execution key
		parts := strings.Split(execKey, "|")
		if len(parts) != 4 {
			continue
		}

		shardID, _ := strconv.Atoi(parts[0])
		namespaceID := parts[1]
		workflowID := parts[2]
		runID := parts[3]

		fmt.Printf("\nExecution [ShardID: %d, NamespaceID: %s, WorkflowID: %s, RunID: %s]:\n",
			shardID, namespaceID, workflowID, runID)

		for _, op := range operations {
			switch op.Type {
			case Added:
				fmt.Printf("  + ADDED signal_id: %s\n", op.SignalID)
			case Deleted:
				fmt.Printf("  - DELETED signal_id: %s\n", op.SignalID)
			}
		}
	}

	// Signal sets by execution
	fmt.Println("\n[SIGNAL SETS] By Execution:")

	// Track all unique execution keys
	allExecKeys := make(map[string]bool)
	for key := range beforeMap {
		row := beforeMap[key]
		allExecKeys[createExecKey(row)] = true
	}
	for key := range afterMap {
		row := afterMap[key]
		allExecKeys[createExecKey(row)] = true
	}

	// Get signals for each execution
	for execKey := range allExecKeys {
		// Parse execution key
		parts := strings.Split(execKey, "|")
		if len(parts) != 4 {
			continue
		}

		shardID, _ := strconv.Atoi(parts[0])
		namespaceID := parts[1]
		workflowID := parts[2]
		runID := parts[3]

		beforeSignals := make(map[string]bool)
		afterSignals := make(map[string]bool)

		// Collect signals for this execution
		for _, row := range beforeMap {
			if createExecKey(row) == execKey {
				beforeSignals[row.SignalID] = true
			}
		}

		for _, row := range afterMap {
			if createExecKey(row) == execKey {
				afterSignals[row.SignalID] = true
			}
		}

		// Convert to sorted slices for display
		beforeList := make([]string, 0, len(beforeSignals))
		for sig := range beforeSignals {
			beforeList = append(beforeList, sig)
		}
		sort.Strings(beforeList)

		afterList := make([]string, 0, len(afterSignals))
		for sig := range afterSignals {
			afterList = append(afterList, sig)
		}
		sort.Strings(afterList)

		fmt.Printf("\nExecution [ShardID: %d, NamespaceID: %s, WorkflowID: %s, RunID: %s]:\n",
			shardID, namespaceID, workflowID, runID)

		fmt.Println("  BEFORE Signals:", beforeList)
		fmt.Println("  AFTER Signals:", afterList)
	}

	// Final summary
	beforeCount := len(before)
	afterCount := len(after)
	fmt.Printf("\n[SUMMARY]\n")
	fmt.Printf("  Before: %d signals\n", beforeCount)
	fmt.Printf("  After: %d signals\n", afterCount)
	fmt.Printf("  Difference: %+d signals\n", afterCount-beforeCount)

	if len(operationsByExec) == 0 {
		fmt.Println("  Result: IDENTICAL - No changes detected")
	} else {
		fmt.Println("  Result: DIFFERENT - Changes detected (see above)")
	}

	fmt.Printf("\n==== END %s ====\n\n", title)
}
