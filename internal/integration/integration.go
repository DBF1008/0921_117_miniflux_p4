// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package integration // import "miniflux.app/v2/internal/integration"

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"miniflux.app/v2/internal/integration/apprise"
	"miniflux.app/v2/internal/integration/archiveorg"
	"miniflux.app/v2/internal/integration/betula"
	"miniflux.app/v2/internal/integration/cubox"
	"miniflux.app/v2/internal/integration/discord"
	"miniflux.app/v2/internal/integration/espial"
	"miniflux.app/v2/internal/integration/instapaper"
	"miniflux.app/v2/internal/integration/karakeep"
	"miniflux.app/v2/internal/integration/linkace"
	"miniflux.app/v2/internal/integration/linkding"
	"miniflux.app/v2/internal/integration/linktaco"
	"miniflux.app/v2/internal/integration/linkwarden"
	"miniflux.app/v2/internal/integration/matrixbot"
	"miniflux.app/v2/internal/integration/notion"
	"miniflux.app/v2/internal/integration/ntfy"
	"miniflux.app/v2/internal/integration/nunuxkeeper"
	"miniflux.app/v2/internal/integration/omnivore"
	"miniflux.app/v2/internal/integration/pinboard"
	"miniflux.app/v2/internal/integration/pushover"
	"miniflux.app/v2/internal/integration/raindrop"
	"miniflux.app/v2/internal/integration/readeck"
	"miniflux.app/v2/internal/integration/readwise"
	"miniflux.app/v2/internal/integration/shaarli"
	"miniflux.app/v2/internal/integration/shiori"
	"miniflux.app/v2/internal/integration/slack"
	"miniflux.app/v2/internal/integration/telegrambot"
	"miniflux.app/v2/internal/integration/wallabag"
	"miniflux.app/v2/internal/integration/webhook"
	"miniflux.app/v2/internal/model"
)

// SendResult describes the outcome of sending an entry to a single integration.
type SendResult struct {
	Provider string
	Success  bool
	Err      error
}

// MarshalJSON exposes the result in a stable JSON shape.
func (r SendResult) MarshalJSON() ([]byte, error) {
	result := struct {
		Provider string `json:"provider"`
		Success  bool   `json:"success"`
		Error    string `json:"error,omitempty"`
	}{
		Provider: r.Provider,
		Success:  r.Success,
	}

	if r.Err != nil {
		result.Error = r.Err.Error()
	}

	return json.Marshal(result)
}

// SendResults is the aggregated outcome of all activated integrations.
type SendResults []SendResult

// Successes returns the list of integrations that accepted the entry.
func (results SendResults) Successes() []SendResult {
	var successes []SendResult
	for _, result := range results {
		if result.Success {
			successes = append(successes, result)
		}
	}
	return successes
}

// Failures returns the list of integrations that rejected or timed out.
func (results SendResults) Failures() []SendResult {
	var failures []SendResult
	for _, result := range results {
		if !result.Success {
			failures = append(failures, result)
		}
	}
	return failures
}

// HasFailures reports whether at least one integration failed.
func (results SendResults) HasFailures() bool {
	for _, result := range results {
		if !result.Success {
			return true
		}
	}
	return false
}

// sendTask represents a single integration invocation.
type sendTask struct {
	index    int
	provider string
	run      func() error
}

// defaultSendEntryTimeout is the maximum time a single integration may take.
// It is a variable so tests can shorten it when exercising timeouts.
var defaultSendEntryTimeout = 15 * time.Second

// SendEntry sends the entry to every enabled third-party provider concurrently.
// Each provider runs in its own goroutine bounded by a context timeout, and the
// aggregated per-provider outcome is returned to the caller.
func SendEntry(ctx context.Context, entry *model.Entry, userIntegrations *model.Integration) SendResults {
	return runSendTasks(ctx, buildSendEntryTasks(entry, userIntegrations), defaultSendEntryTimeout)
}

// runSendTasks executes the tasks concurrently, enforcing per-task timeouts and
// returning the results in the original task order.
func runSendTasks(parentCtx context.Context, tasks []sendTask, timeout time.Duration) SendResults {
	results := make(SendResults, len(tasks))
	if len(tasks) == 0 {
		return results
	}

	type indexedResult struct {
		index  int
		result SendResult
	}

	resultCh := make(chan indexedResult, len(tasks))
	var wg sync.WaitGroup

	for _, task := range tasks {
		wg.Add(1)

		go func(task sendTask) {
			defer wg.Done()

			taskCtx, cancel := context.WithTimeout(parentCtx, timeout)
			defer cancel()

			doneCh := make(chan error, 1)

			go func() {
				doneCh <- task.run()
			}()

			select {
			case <-taskCtx.Done():
				resultCh <- indexedResult{
					index: task.index,
					result: SendResult{
						Provider: task.provider,
						Success:  false,
						Err:      fmt.Errorf("%s: %w", task.provider, taskCtx.Err()),
					},
				}
			case err := <-doneCh:
				resultCh <- indexedResult{
					index: task.index,
					result: SendResult{
						Provider: task.provider,
						Success:  err == nil,
						Err:      err,
					},
				}
			}
		}(task)
	}

	wg.Wait()
	close(resultCh)

	for indexed := range resultCh {
		results[indexed.index] = indexed.result
	}

	return results
}

// buildSendEntryTasks collects one runnable task for each enabled integration.
func buildSendEntryTasks(entry *model.Entry, userIntegrations *model.Integration) []sendTask {
	var tasks []sendTask
	addTask := func(provider string, run func() error) {
		tasks = append(tasks, sendTask{index: len(tasks), provider: provider, run: run})
	}

	baseLogAttrs := func() []any {
		return []any{
			slog.Int64("user_id", userIntegrations.UserID),
			slog.Int64("entry_id", entry.ID),
			slog.String("entry_url", entry.URL),
		}
	}

	if userIntegrations.BetulaEnabled {
		addTask("Betula", func() error {
			slog.Debug("Sending entry to Betula", baseLogAttrs()...)
			client := betula.NewClient(userIntegrations.BetulaURL, userIntegrations.BetulaToken)
			return client.CreateBookmark(entry.URL, entry.Title, entry.Tags)
		})
	}

	if userIntegrations.PinboardEnabled {
		addTask("Pinboard", func() error {
			slog.Debug("Sending entry to Pinboard", baseLogAttrs()...)
			client := pinboard.NewClient(userIntegrations.PinboardToken)
			return client.CreateBookmark(
				entry.URL,
				entry.Title,
				userIntegrations.PinboardTags,
				userIntegrations.PinboardMarkAsUnread,
			)
		})
	}

	if userIntegrations.InstapaperEnabled {
		addTask("Instapaper", func() error {
			slog.Debug("Sending entry to Instapaper", baseLogAttrs()...)
			client := instapaper.NewClient(userIntegrations.InstapaperUsername, userIntegrations.InstapaperPassword)
			return client.AddURL(entry.URL, entry.Title)
		})
	}

	if userIntegrations.WallabagEnabled {
		addTask("Wallabag", func() error {
			attrs := append(baseLogAttrs(), slog.String("user_tags", userIntegrations.WallabagTags))
			slog.Debug("Sending entry to Wallabag", attrs...)
			client := wallabag.NewClient(
				userIntegrations.WallabagURL,
				userIntegrations.WallabagClientID,
				userIntegrations.WallabagClientSecret,
				userIntegrations.WallabagUsername,
				userIntegrations.WallabagPassword,
				userIntegrations.WallabagTags,
				userIntegrations.WallabagOnlyURL,
			)
			return client.CreateEntry(entry.URL, entry.Title, entry.Content)
		})
	}

	if userIntegrations.NotionEnabled {
		addTask("Notion", func() error {
			slog.Debug("Sending entry to Notion", baseLogAttrs()...)
			client := notion.NewClient(
				userIntegrations.NotionToken,
				userIntegrations.NotionPageID,
			)
			return client.UpdateDocument(entry.URL, entry.Title)
		})
	}

	if userIntegrations.NunuxKeeperEnabled {
		addTask("NunuxKeeper", func() error {
			slog.Debug("Sending entry to NunuxKeeper", baseLogAttrs()...)
			client := nunuxkeeper.NewClient(
				userIntegrations.NunuxKeeperURL,
				userIntegrations.NunuxKeeperAPIKey,
			)
			return client.AddEntry(entry.URL, entry.Title, entry.Content)
		})
	}

	if userIntegrations.EspialEnabled {
		addTask("Espial", func() error {
			slog.Debug("Sending entry to Espial", baseLogAttrs()...)
			client := espial.NewClient(
				userIntegrations.EspialURL,
				userIntegrations.EspialAPIKey,
			)
			return client.CreateLink(entry.URL, entry.Title, userIntegrations.EspialTags)
		})
	}

	if userIntegrations.LinkAceEnabled {
		addTask("LinkAce", func() error {
			slog.Debug("Sending entry to LinkAce", baseLogAttrs()...)
			client := linkace.NewClient(
				userIntegrations.LinkAceURL,
				userIntegrations.LinkAceAPIKey,
				userIntegrations.LinkAceTags,
				userIntegrations.LinkAcePrivate,
				userIntegrations.LinkAceCheckDisabled,
			)
			return client.AddURL(entry.URL, entry.Title)
		})
	}

	if userIntegrations.LinkdingEnabled {
		addTask("Linkding", func() error {
			slog.Debug("Sending entry to Linkding", baseLogAttrs()...)
			client := linkding.NewClient(
				userIntegrations.LinkdingURL,
				userIntegrations.LinkdingAPIKey,
				userIntegrations.LinkdingTags,
				userIntegrations.LinkdingMarkAsUnread,
			)
			return client.CreateBookmark(entry.URL, entry.Title)
		})
	}

	if userIntegrations.LinktacoEnabled {
		addTask("LinkTaco", func() error {
			slog.Debug("Sending entry to LinkTaco", baseLogAttrs()...)
			client := linktaco.NewClient(
				userIntegrations.LinktacoAPIToken,
				userIntegrations.LinktacoOrgSlug,
				userIntegrations.LinktacoTags,
				userIntegrations.LinktacoVisibility,
			)
			return client.CreateBookmark(entry.URL, entry.Title, entry.Content)
		})
	}

	if userIntegrations.LinkwardenEnabled {
		addTask("Linkwarden", func() error {
			attrs := baseLogAttrs()
			if userIntegrations.LinkwardenCollectionID != nil {
				attrs = append(attrs, slog.Int64("collection_id", *userIntegrations.LinkwardenCollectionID))
			}
			slog.Debug("Sending entry to linkwarden", attrs...)
			client := linkwarden.NewClient(
				userIntegrations.LinkwardenURL,
				userIntegrations.LinkwardenAPIKey,
				userIntegrations.LinkwardenCollectionID,
			)
			return client.CreateBookmark(entry.URL, entry.Title)
		})
	}

	if userIntegrations.ReadeckEnabled {
		addTask("Readeck", func() error {
			slog.Debug("Sending entry to Readeck", baseLogAttrs()...)
			client := readeck.NewClient(
				userIntegrations.ReadeckURL,
				userIntegrations.ReadeckAPIKey,
				userIntegrations.ReadeckLabels,
				userIntegrations.ReadeckOnlyURL,
			)
			return client.CreateBookmark(entry.URL, entry.Title, entry.Content)
		})
	}

	if userIntegrations.ReadwiseEnabled {
		addTask("Readwise", func() error {
			slog.Debug("Sending entry to Readwise", baseLogAttrs()...)
			client := readwise.NewClient(userIntegrations.ReadwiseAPIKey)
			return client.CreateDocument(entry.URL)
		})
	}

	if userIntegrations.CuboxEnabled {
		addTask("Cubox", func() error {
			slog.Debug("Sending entry to Cubox", baseLogAttrs()...)
			client := cubox.NewClient(userIntegrations.CuboxAPILink)
			return client.SaveLink(entry.URL)
		})
	}

	if userIntegrations.ShioriEnabled {
		addTask("Shiori", func() error {
			slog.Debug("Sending entry to Shiori", baseLogAttrs()...)
			client := shiori.NewClient(
				userIntegrations.ShioriURL,
				userIntegrations.ShioriUsername,
				userIntegrations.ShioriPassword,
			)
			return client.CreateBookmark(entry.URL, entry.Title)
		})
	}

	if userIntegrations.ShaarliEnabled {
		addTask("Shaarli", func() error {
			slog.Debug("Sending entry to Shaarli", baseLogAttrs()...)
			client := shaarli.NewClient(
				userIntegrations.ShaarliURL,
				userIntegrations.ShaarliAPISecret,
			)
			return client.CreateLink(entry.URL, entry.Title)
		})
	}

	if userIntegrations.ArchiveorgEnabled {
		addTask("Archive.org", func() error {
			slog.Debug("Sending entry to archive.org", baseLogAttrs()...)
			return archiveorg.NewClient().SendURL(entry.URL)
		})
	}

	if userIntegrations.WebhookEnabled {
		addTask("Webhook", func() error {
			var webhookURL string
			if entry.Feed != nil && entry.Feed.WebhookURL != "" {
				webhookURL = entry.Feed.WebhookURL
			} else {
				webhookURL = userIntegrations.WebhookURL
			}

			attrs := append(baseLogAttrs(), slog.String("webhook_url", webhookURL))
			slog.Debug("Sending entry to Webhook", attrs...)

			webhookClient := webhook.NewClient(webhookURL, userIntegrations.WebhookSecret)
			return webhookClient.SendSaveEntryWebhookEvent(entry)
		})
	}

	if userIntegrations.OmnivoreEnabled {
		addTask("Omnivore", func() error {
			slog.Debug("Sending entry to Omnivore", baseLogAttrs()...)
			client := omnivore.NewClient(userIntegrations.OmnivoreAPIKey, userIntegrations.OmnivoreURL)
			return client.SaveURL(entry.URL)
		})
	}

	if userIntegrations.KarakeepEnabled {
		addTask("Karakeep", func() error {
			attrs := append(baseLogAttrs(), slog.String("user_tags", userIntegrations.KarakeepTags))
			slog.Debug("Sending entry to Karakeep", attrs...)
			client := karakeep.NewClient(
				userIntegrations.KarakeepAPIKey,
				userIntegrations.KarakeepURL,
				userIntegrations.KarakeepTags,
			)
			return client.SaveURL(entry.URL)
		})
	}

	if userIntegrations.RaindropEnabled {
		addTask("Raindrop", func() error {
			slog.Debug("Sending entry to Raindrop", baseLogAttrs()...)
			client := raindrop.NewClient(userIntegrations.RaindropToken, userIntegrations.RaindropCollectionID, userIntegrations.RaindropTags)
			return client.CreateRaindrop(entry.URL, entry.Title)
		})
	}

	return tasks
}

// PushEntries pushes a list of entries to activated third-party providers during feed refreshes.
func PushEntries(feed *model.Feed, entries model.Entries, userIntegrations *model.Integration) {
	if userIntegrations.MatrixBotEnabled {
		slog.Debug("Sending new entries to Matrix",
			slog.Int64("user_id", userIntegrations.UserID),
			slog.Int("nb_entries", len(entries)),
			slog.Int64("feed_id", feed.ID),
		)

		err := matrixbot.PushEntries(
			feed,
			entries,
			userIntegrations.MatrixBotURL,
			userIntegrations.MatrixBotUser,
			userIntegrations.MatrixBotPassword,
			userIntegrations.MatrixBotChatID,
		)
		if err != nil {
			slog.Error("Unable to send new entries to Matrix",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int("nb_entries", len(entries)),
				slog.Int64("feed_id", feed.ID),
				slog.Any("error", err),
			)
		}
	}
	if userIntegrations.WebhookEnabled {
		var webhookURL string
		if feed.WebhookURL != "" {
			webhookURL = feed.WebhookURL
		} else {
			webhookURL = userIntegrations.WebhookURL
		}

		slog.Debug("Sending new entries to Webhook",
			slog.Int64("user_id", userIntegrations.UserID),
			slog.Int("nb_entries", len(entries)),
			slog.Int64("feed_id", feed.ID),
			slog.String("webhook_url", webhookURL),
		)

		webhookClient := webhook.NewClient(webhookURL, userIntegrations.WebhookSecret)
		if err := webhookClient.SendNewEntriesWebhookEvent(feed, entries); err != nil {
			slog.Warn("Unable to send new entries to Webhook",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int("nb_entries", len(entries)),
				slog.Int64("feed_id", feed.ID),
				slog.String("webhook_url", webhookURL),
				slog.Any("error", err),
			)
		}
	}

	if userIntegrations.NtfyEnabled && feed.NtfyEnabled {
		ntfyTopic := feed.NtfyTopic
		if ntfyTopic == "" {
			ntfyTopic = userIntegrations.NtfyTopic
		}
		slog.Debug("Sending new entries to Ntfy",
			slog.Int64("user_id", userIntegrations.UserID),
			slog.Int("nb_entries", len(entries)),
			slog.Int64("feed_id", feed.ID),
			slog.String("topic", ntfyTopic),
		)

		client := ntfy.NewClient(
			userIntegrations.NtfyURL,
			ntfyTopic,
			userIntegrations.NtfyAPIToken,
			userIntegrations.NtfyUsername,
			userIntegrations.NtfyPassword,
			userIntegrations.NtfyIconURL,
			userIntegrations.NtfyInternalLinks,
			feed.NtfyPriority,
		)

		if err := client.SendMessages(feed, entries); err != nil {
			slog.Warn("Unable to send new entries to Ntfy", slog.Any("error", err))
		}
	}

	if userIntegrations.AppriseEnabled {
		slog.Debug("Sending new entries to Apprise",
			slog.Int64("user_id", userIntegrations.UserID),
			slog.Int("nb_entries", len(entries)),
			slog.Int64("feed_id", feed.ID),
		)

		appriseServiceURLs := userIntegrations.AppriseServicesURL
		if feed.AppriseServiceURLs != "" {
			appriseServiceURLs = feed.AppriseServiceURLs
		}

		client := apprise.NewClient(
			appriseServiceURLs,
			userIntegrations.AppriseURL,
		)

		if err := client.SendNotification(feed, entries); err != nil {
			slog.Warn("Unable to send new entries to Apprise", slog.Any("error", err))
		}
	}

	if userIntegrations.DiscordEnabled {
		slog.Debug("Sending new entries to Discord",
			slog.Int64("user_id", userIntegrations.UserID),
			slog.Int("nb_entries", len(entries)),
			slog.Int64("feed_id", feed.ID),
		)

		client := discord.NewClient(
			userIntegrations.DiscordWebhookLink,
		)

		if err := client.SendDiscordMsg(feed, entries); err != nil {
			slog.Warn("Unable to send new entries to Discord", slog.Any("error", err))
		}
	}

	if userIntegrations.SlackEnabled {
		slog.Debug("Sending new entries to Slack",
			slog.Int64("user_id", userIntegrations.UserID),
			slog.Int("nb_entries", len(entries)),
			slog.Int64("feed_id", feed.ID),
		)

		client := slack.NewClient(
			userIntegrations.SlackWebhookLink,
		)

		if err := client.SendSlackMsg(feed, entries); err != nil {
			slog.Warn("Unable to send new entries to Slack", slog.Any("error", err))
		}
	}

	if userIntegrations.PushoverEnabled && feed.PushoverEnabled {
		slog.Debug("Sending new entries to Pushover",
			slog.Int64("user_id", userIntegrations.UserID),
			slog.Int("nb_entries", len(entries)),
			slog.Int64("feed_id", feed.ID),
		)

		client := pushover.NewClient(
			userIntegrations.PushoverUser,
			userIntegrations.PushoverToken,
			feed.PushoverPriority,
			userIntegrations.PushoverDevice,
			userIntegrations.PushoverPrefix,
		)

		if err := client.SendMessages(feed, entries); err != nil {
			slog.Warn("Unable to send new entries to Pushover", slog.Any("error", err))
		}
	}

	// Integrations that only support sending individual entries
	if userIntegrations.TelegramBotEnabled {
		for _, entry := range entries {
			slog.Debug("Sending a new entry to Telegram",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			if err := telegrambot.PushEntry(
				feed,
				entry,
				userIntegrations.TelegramBotToken,
				userIntegrations.TelegramBotChatID,
				userIntegrations.TelegramBotTopicID,
				userIntegrations.TelegramBotDisableWebPagePreview,
				userIntegrations.TelegramBotDisableNotification,
				userIntegrations.TelegramBotDisableButtons,
			); err != nil {
				slog.Error("Unable to send entry to Telegram",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
			}
		}
	}

	// Push each new entry to Readeck when push is enabled
	if userIntegrations.ReadeckPushEnabled {
		client := readeck.NewClient(
			userIntegrations.ReadeckURL,
			userIntegrations.ReadeckAPIKey,
			userIntegrations.ReadeckLabels,
			userIntegrations.ReadeckOnlyURL,
		)
		for _, entry := range entries {
			slog.Debug("Sending a new entry to Readeck",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			if err := client.CreateBookmark(entry.URL, entry.Title, entry.Content); err != nil {
				slog.Error("Unable to send entry to Readeck",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
			}
		}
	}
}
