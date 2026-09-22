// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"testing"

	"miniflux.app/v2/internal/model"
)

func TestSendEntryNoEnabledIntegration(t *testing.T) {
	results := SendEntry(context.Background(), &model.Entry{ID: 1}, &model.Integration{UserID: 1})
	if len(results) != 0 {
		t.Fatalf("expected no results, got %d", len(results))
	}
	if results.HasFailures() {
		t.Fatal("expected no failures")
	}
}

func TestSendEntryAggregatesProviderFailure(t *testing.T) {
	// Cubox with an invalid API link fails immediately while building the
	// request endpoint; this exercises a real provider end to end without
	// requiring network access.
	settings := &model.Integration{
		UserID:       1,
		CuboxEnabled: true,
		CuboxAPILink: "://not-a-url",
	}

	results := SendEntry(context.Background(), &model.Entry{ID: 1, URL: "https://example.com/article"}, settings)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d: %+v", len(results), results)
	}
	if results[0].Success {
		t.Fatal("expected Cubox to fail with invalid URL")
	}
	if results[0].Provider != "Cubox" {
		t.Fatalf("unexpected provider: %s", results[0].Provider)
	}
	if results[0].Err == nil {
		t.Fatal("expected error to be aggregated")
	}

	failures := results.Failures()
	if len(failures) != 1 || failures[0].Provider != "Cubox" {
		t.Fatalf("unexpected failures: %+v", failures)
	}
}
