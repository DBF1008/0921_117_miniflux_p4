// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"miniflux.app/v2/internal/model"
)

func TestSendEntryLogsLinkwardenCollectionID(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(handler)
	prev := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(prev)

	entry := &model.Entry{ID: 52, URL: "https://example.org/test.html", Title: "Test"}
	coll := int64(12345)
	userIntegrations := &model.Integration{
		UserID:                 1,
		LinkwardenEnabled:      true,
		LinkwardenCollectionID: &coll,
		LinkwardenURL:          "",
		LinkwardenAPIKey:       "",
	}

	SendEntry(entry, userIntegrations)

	out := buf.String()
	if !strings.Contains(out, `"collection_id":12345`) {
		t.Fatalf("expected collection_id in logs; got: %s", out)
	}
}

func TestSendEntryLogsLinkwardenWithoutCollectionID(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, nil)
	logger := slog.New(handler)
	prev := slog.Default()
	slog.SetDefault(logger)
	defer slog.SetDefault(prev)

	entry := &model.Entry{ID: 52, URL: "https://example.org/test.html", Title: "Test"}
	userIntegrations := &model.Integration{
		UserID:            1,
		LinkwardenEnabled: true,
		LinkwardenURL:     "",
		LinkwardenAPIKey:  "",
	}

	SendEntry(entry, userIntegrations)

	out := buf.String()
	if strings.Contains(out, "collection_id") {
		t.Fatalf("did not expect collection_id in logs; got: %s", out)
	}
}

func TestSendEntryReturnsNoResultWhenNoIntegrationEnabled(t *testing.T) {
	results := SendEntry(&model.Entry{ID: 1, URL: "https://example.org/a"}, &model.Integration{UserID: 1})
	if results != nil {
		t.Fatalf("expected no results, got: %v", results)
	}
}

func TestBuildSendTasksOnlyIncludesEnabledProviders(t *testing.T) {
	entry := &model.Entry{ID: 1, URL: "https://example.org/a"}
	userIntegrations := &model.Integration{
		UserID:          1,
		PinboardEnabled: true,
		WebhookEnabled:  true,
	}

	tasks := buildSendTasks(entry, userIntegrations)

	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d: %v", len(tasks), tasks)
	}

	providers := []string{tasks[0].provider, tasks[1].provider}
	for _, expected := range []string{"Pinboard", "Webhook"} {
		found := false
		for _, provider := range providers {
			if provider == expected {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("expected a task for %q, got: %v", expected, providers)
		}
	}
}

func TestRunSendTasksAggregatesResults(t *testing.T) {
	errExpected := errors.New("boom")
	tasks := []sendTask{
		{provider: "Successful", send: func() error { return nil }},
		{provider: "Failing", send: func() error { return errExpected }},
	}

	results := runSendTasks(tasks)

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d: %v", len(results), results)
	}

	if results[0].Provider != "Successful" || results[0].Err != nil {
		t.Fatalf("expected the first provider to succeed, got: %+v", results[0])
	}

	if results[1].Provider != "Failing" || !errors.Is(results[1].Err, errExpected) {
		t.Fatalf("expected the second provider to fail, got: %+v", results[1])
	}
}

func TestRunSendTasksExecutesConcurrently(t *testing.T) {
	const taskCount = 3
	const taskDuration = 300 * time.Millisecond

	tasks := make([]sendTask, 0, taskCount)
	for range taskCount {
		tasks = append(tasks, sendTask{
			provider: "Slow",
			send:     func() error { time.Sleep(taskDuration); return nil },
		})
	}

	start := time.Now()
	results := runSendTasks(tasks)
	elapsed := time.Since(start)

	if len(results) != taskCount {
		t.Fatalf("expected %d results, got %d", taskCount, len(results))
	}

	if elapsed >= taskCount*taskDuration {
		t.Fatalf("expected tasks to run concurrently, took %s", elapsed)
	}
}

func TestRunSendTaskTimeout(t *testing.T) {
	previousTimeout := sendEntryTimeout
	sendEntryTimeout = 100 * time.Millisecond
	t.Cleanup(func() { sendEntryTimeout = previousTimeout })

	unblock := make(chan struct{})
	t.Cleanup(func() { close(unblock) })

	start := time.Now()
	result := runSendTask(sendTask{
		provider: "Hanging",
		send:     func() error { <-unblock; return nil },
	})
	elapsed := time.Since(start)

	if result.Provider != "Hanging" {
		t.Fatalf("unexpected provider name: %q", result.Provider)
	}

	if result.Err == nil {
		t.Fatal("expected a timeout error, got nil")
	}

	if !strings.Contains(result.Err.Error(), "timed out") {
		t.Fatalf("expected a timeout error, got: %v", result.Err)
	}

	if elapsed >= 5*time.Second {
		t.Fatalf("expected the task to abort after the timeout, took %s", elapsed)
	}
}
