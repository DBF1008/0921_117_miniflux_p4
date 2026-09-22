// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package ui // import "miniflux.app/v2/internal/ui"

import (
	"net/http"

	"miniflux.app/v2/internal/http/request"
	"miniflux.app/v2/internal/http/response"
	"miniflux.app/v2/internal/integration"
)

func (h *handler) saveEntry(w http.ResponseWriter, r *http.Request) {
	entryID := request.RouteInt64Param(r, "entryID")

	entry, err := h.store.NewEntryQueryBuilder(request.UserID(r)).
		WithEntryIDs(entryID).
		GetEntry()
	if err != nil {
		response.JSONServerError(w, r, err)
		return
	}

	if entry == nil {
		response.JSONNotFound(w, r)
		return
	}

	userIntegrations, err := h.store.Integration(request.UserID(r))
	if err != nil {
		response.JSONServerError(w, r, err)
		return
	}

	results := integration.SendEntry(r.Context(), entry, userIntegrations)

	response.JSONCreated(w, r, map[string]any{
		"message":       "saved",
		"total":         len(results),
		"success_count": len(results.Successes()),
		"failure_count": len(results.Failures()),
		"results":       results,
	})
}
