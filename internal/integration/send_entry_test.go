// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func makeTask(index int, provider string, run func() error) sendTask {
	return sendTask{index: index, provider: provider, run: run}
}

func TestRunSendTasksConcurrent(t *testing.T) {
	const taskCount = 20

	var mu sync.Mutex
	active := 0
	maxConcurrent := 0
	var wg sync.WaitGroup
	wg.Add(taskCount)

	tasks := make([]sendTask, 0, taskCount)
	for i := range taskCount {
		i := i
		tasks = append(tasks, makeTask(i, "Provider"+string(rune('A'+i)), func() error {
			defer wg.Done()

			mu.Lock()
			active++
			if active > maxConcurrent {
				maxConcurrent = active
			}
			mu.Unlock()

			time.Sleep(50 * time.Millisecond)

			mu.Lock()
			active--
			mu.Unlock()

			return nil
		}))
	}

	results := runSendTasks(context.Background(), tasks, 5*time.Second)

	if len(results) != taskCount {
		t.Fatalf("expected %d results, got %d", taskCount, len(results))
	}

	for i, result := range results {
		expected := "Provider" + string(rune('A'+i))
		if result.Provider != expected {
			t.Fatalf("result %d: expected provider %q, got %q", i, expected, result.Provider)
		}
		if !result.Success {
			t.Fatalf("result %d: expected success, got error %v", i, result.Err)
		}
	}

	if maxConcurrent < 2 {
		t.Fatalf("expected tasks to run concurrently, max concurrency was %d", maxConcurrent)
	}
}

func TestRunSendTasksAggregatesSuccessAndFailure(t *testing.T) {
	errBoom := errors.New("boom")

	tasks := []sendTask{
		makeTask(0, "Wallabag", func() error { return nil }),
		makeTask(1, "Pinboard", func() error { return errBoom }),
		makeTask(2, "Notion", func() error { return nil }),
	}

	results := runSendTasks(context.Background(), tasks, time.Second)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	if results[0].Provider != "Wallabag" || !results[0].Success {
		t.Fatalf("unexpected result[0]: %+v", results[0])
	}
	if results[1].Provider != "Pinboard" || results[1].Success || !errors.Is(results[1].Err, errBoom) {
		t.Fatalf("unexpected result[1]: %+v", results[1])
	}
	if results[2].Provider != "Notion" || !results[2].Success {
		t.Fatalf("unexpected result[2]: %+v", results[2])
	}

	if !results.HasFailures() {
		t.Fatal("expected HasFailures to be true")
	}

	if len(results.Successes()) != 2 {
		t.Fatalf("expected 2 successes, got %d", len(results.Successes()))
	}

	failures := results.Failures()
	if len(failures) != 1 || failures[0].Provider != "Pinboard" {
		t.Fatalf("unexpected failures: %+v", failures)
	}
}

func TestRunSendTasksTimeout(t *testing.T) {
	release := make(chan struct{})
	tasks := []sendTask{
		makeTask(0, "SlowService", func() error {
			<-release
			return nil
		}),
	}

	start := time.Now()
	results := runSendTasks(context.Background(), tasks, 50*time.Millisecond)
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Fatalf("timeout took too long: %s", elapsed)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	result := results[0]
	if result.Success {
		t.Fatal("expected slow task to fail with timeout")
	}
	if !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", result.Err)
	}
	if !strings.Contains(result.Err.Error(), "SlowService") {
		t.Fatalf("expected provider name in error, got %v", result.Err)
	}

	close(release)
}

func TestRunSendTasksParentContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	tasks := []sendTask{
		makeTask(0, "Cancelled", func() error {
			time.Sleep(time.Second)
			return nil
		}),
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	results := runSendTasks(ctx, tasks, 5*time.Second)
	if results[0].Success {
		t.Fatal("expected task to fail when parent context is cancelled")
	}
	if !errors.Is(results[0].Err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", results[0].Err)
	}
}

func TestRunSendTasksNoTasks(t *testing.T) {
	results := runSendTasks(context.Background(), nil, time.Second)
	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}
	if results.HasFailures() {
		t.Fatal("expected no failures")
	}
}

func TestSendResultsJSONShape(t *testing.T) {
	results := SendResults{
		{Provider: "Wallabag", Success: true},
		{Provider: "Pinboard", Success: false, Err: errors.New("pinboard: boom")},
	}

	payload, err := json.Marshal(results)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	jsonBody := string(payload)
	expectedSnippets := []string{
		`"provider":"Wallabag"`,
		`"success":true`,
		`"provider":"Pinboard"`,
		`"success":false`,
		`"error":"pinboard: boom"`,
	}

	for _, snippet := range expectedSnippets {
		if !strings.Contains(jsonBody, snippet) {
			t.Fatalf("expected JSON to contain %s, got %s", snippet, jsonBody)
		}
	}
}
