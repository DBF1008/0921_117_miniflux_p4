// SPDX-FileCopyrightText: Copyright The Miniflux Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package integration // import "miniflux.app/v2/internal/integration"

import (
	"context"
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

// sendEntryTimeout is the maximum amount of time allowed for a single
// third-party provider to accept an entry before the operation is aborted.
var sendEntryTimeout = 30 * time.Second

// SendResult represents the outcome of sending an entry to a single
// third-party provider.
type SendResult struct {
	Provider string
	Err      error
}

// sendTask is a unit of work that sends an entry to one third-party provider.
type sendTask struct {
	provider string
	send     func() error
}

// SendEntry sends the entry to third-party providers when the user click on "Save".
//
// Each enabled provider is called concurrently in its own goroutine with an
// individual timeout, so a slow or unresponsive provider cannot block the
// others. It returns the aggregated result of every attempted provider.
func SendEntry(entry *model.Entry, userIntegrations *model.Integration) []SendResult {
	return runSendTasks(buildSendTasks(entry, userIntegrations))
}

// runSendTasks executes every task concurrently and returns one result per
// task, in the same order.
func runSendTasks(tasks []sendTask) []SendResult {
	if len(tasks) == 0 {
		return nil
	}

	results := make([]SendResult, len(tasks))
	var waitGroup sync.WaitGroup
	for index, task := range tasks {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			results[index] = runSendTask(task)
		}()
	}
	waitGroup.Wait()

	return results
}

// runSendTask executes a single provider call, aborting the wait when the
// per-provider timeout expires.
func runSendTask(task sendTask) SendResult {
	ctx, cancel := context.WithTimeout(context.Background(), sendEntryTimeout)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- task.send()
	}()

	select {
	case err := <-done:
		return SendResult{Provider: task.provider, Err: err}
	case <-ctx.Done():
		return SendResult{
			Provider: task.provider,
			Err:      fmt.Errorf("integration %q timed out after %s", task.provider, sendEntryTimeout),
		}
	}
}

// buildSendTasks returns the list of provider calls to execute based on the
// integrations enabled by the user.
func buildSendTasks(entry *model.Entry, userIntegrations *model.Integration) []sendTask {
	var tasks []sendTask

	if userIntegrations.BetulaEnabled {
		tasks = append(tasks, sendTask{provider: "Betula", send: func() error {
			slog.Debug("Sending entry to Betula",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := betula.NewClient(userIntegrations.BetulaURL, userIntegrations.BetulaToken)
			err := client.CreateBookmark(
				entry.URL,
				entry.Title,
				entry.Tags,
			)

			if err != nil {
				slog.Error("Unable to send entry to Betula",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
			}
			return err
		}})
	}

	if userIntegrations.PinboardEnabled {
		tasks = append(tasks, sendTask{provider: "Pinboard", send: func() error {
			slog.Debug("Sending entry to Pinboard",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := pinboard.NewClient(userIntegrations.PinboardToken)
			err := client.CreateBookmark(
				entry.URL,
				entry.Title,
				userIntegrations.PinboardTags,
				userIntegrations.PinboardMarkAsUnread,
			)

			if err != nil {
				slog.Error("Unable to send entry to Pinboard",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
			}
			return err
		}})
	}

	if userIntegrations.InstapaperEnabled {
		tasks = append(tasks, sendTask{provider: "Instapaper", send: func() error {
			slog.Debug("Sending entry to Instapaper",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := instapaper.NewClient(userIntegrations.InstapaperUsername, userIntegrations.InstapaperPassword)
			if err := client.AddURL(entry.URL, entry.Title); err != nil {
				slog.Error("Unable to send entry to Instapaper",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.WallabagEnabled {
		tasks = append(tasks, sendTask{provider: "Wallabag", send: func() error {
			slog.Debug("Sending entry to Wallabag",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.String("user_tags", userIntegrations.WallabagTags),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := wallabag.NewClient(
				userIntegrations.WallabagURL,
				userIntegrations.WallabagClientID,
				userIntegrations.WallabagClientSecret,
				userIntegrations.WallabagUsername,
				userIntegrations.WallabagPassword,
				userIntegrations.WallabagTags,
				userIntegrations.WallabagOnlyURL,
			)

			if err := client.CreateEntry(entry.URL, entry.Title, entry.Content); err != nil {
				slog.Error("Unable to send entry to Wallabag",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.String("user_tags", userIntegrations.WallabagTags),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.NotionEnabled {
		tasks = append(tasks, sendTask{provider: "Notion", send: func() error {
			slog.Debug("Sending entry to Notion",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := notion.NewClient(
				userIntegrations.NotionToken,
				userIntegrations.NotionPageID,
			)
			if err := client.UpdateDocument(entry.URL, entry.Title); err != nil {
				slog.Error("Unable to send entry to Notion",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.NunuxKeeperEnabled {
		tasks = append(tasks, sendTask{provider: "NunuxKeeper", send: func() error {
			slog.Debug("Sending entry to NunuxKeeper",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := nunuxkeeper.NewClient(
				userIntegrations.NunuxKeeperURL,
				userIntegrations.NunuxKeeperAPIKey,
			)

			if err := client.AddEntry(entry.URL, entry.Title, entry.Content); err != nil {
				slog.Error("Unable to send entry to NunuxKeeper",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.EspialEnabled {
		tasks = append(tasks, sendTask{provider: "Espial", send: func() error {
			slog.Debug("Sending entry to Espial",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := espial.NewClient(
				userIntegrations.EspialURL,
				userIntegrations.EspialAPIKey,
			)

			if err := client.CreateLink(entry.URL, entry.Title, userIntegrations.EspialTags); err != nil {
				slog.Error("Unable to send entry to Espial",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.LinkAceEnabled {
		tasks = append(tasks, sendTask{provider: "LinkAce", send: func() error {
			slog.Debug("Sending entry to LinkAce",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := linkace.NewClient(
				userIntegrations.LinkAceURL,
				userIntegrations.LinkAceAPIKey,
				userIntegrations.LinkAceTags,
				userIntegrations.LinkAcePrivate,
				userIntegrations.LinkAceCheckDisabled,
			)
			if err := client.AddURL(entry.URL, entry.Title); err != nil {
				slog.Error("Unable to send entry to LinkAce",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.LinkdingEnabled {
		tasks = append(tasks, sendTask{provider: "Linkding", send: func() error {
			slog.Debug("Sending entry to Linkding",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := linkding.NewClient(
				userIntegrations.LinkdingURL,
				userIntegrations.LinkdingAPIKey,
				userIntegrations.LinkdingTags,
				userIntegrations.LinkdingMarkAsUnread,
			)
			if err := client.CreateBookmark(entry.URL, entry.Title); err != nil {
				slog.Error("Unable to send entry to Linkding",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.LinktacoEnabled {
		tasks = append(tasks, sendTask{provider: "LinkTaco", send: func() error {
			slog.Debug("Sending entry to LinkTaco",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := linktaco.NewClient(
				userIntegrations.LinktacoAPIToken,
				userIntegrations.LinktacoOrgSlug,
				userIntegrations.LinktacoTags,
				userIntegrations.LinktacoVisibility,
			)
			if err := client.CreateBookmark(entry.URL, entry.Title, entry.Content); err != nil {
				slog.Error("Unable to send entry to LinkTaco",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.LinkwardenEnabled {
		tasks = append(tasks, sendTask{provider: "Linkwarden", send: func() error {
			attrs := []any{
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			}

			if userIntegrations.LinkwardenCollectionID != nil {
				attrs = append(attrs, slog.Int64("collection_id", *userIntegrations.LinkwardenCollectionID))
			}

			slog.Debug("Sending entry to linkwarden", attrs...)

			client := linkwarden.NewClient(
				userIntegrations.LinkwardenURL,
				userIntegrations.LinkwardenAPIKey,
				userIntegrations.LinkwardenCollectionID,
			)
			if err := client.CreateBookmark(entry.URL, entry.Title); err != nil {
				attrs = append(attrs, slog.Any("error", err))
				slog.Error("Unable to send entry to Linkwarden", attrs...)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.ReadeckEnabled {
		tasks = append(tasks, sendTask{provider: "Readeck", send: func() error {
			slog.Debug("Sending entry to Readeck",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := readeck.NewClient(
				userIntegrations.ReadeckURL,
				userIntegrations.ReadeckAPIKey,
				userIntegrations.ReadeckLabels,
				userIntegrations.ReadeckOnlyURL,
			)
			if err := client.CreateBookmark(entry.URL, entry.Title, entry.Content); err != nil {
				slog.Error("Unable to send entry to Readeck",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.ReadwiseEnabled {
		tasks = append(tasks, sendTask{provider: "Readwise", send: func() error {
			slog.Debug("Sending entry to Readwise",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := readwise.NewClient(
				userIntegrations.ReadwiseAPIKey,
			)

			if err := client.CreateDocument(entry.URL); err != nil {
				slog.Error("Unable to send entry to Readwise",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.CuboxEnabled {
		tasks = append(tasks, sendTask{provider: "Cubox", send: func() error {
			slog.Debug("Sending entry to Cubox",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := cubox.NewClient(userIntegrations.CuboxAPILink)

			if err := client.SaveLink(entry.URL); err != nil {
				slog.Error("Unable to send entry to Cubox",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.ShioriEnabled {
		tasks = append(tasks, sendTask{provider: "Shiori", send: func() error {
			slog.Debug("Sending entry to Shiori",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := shiori.NewClient(
				userIntegrations.ShioriURL,
				userIntegrations.ShioriUsername,
				userIntegrations.ShioriPassword,
			)

			if err := client.CreateBookmark(entry.URL, entry.Title); err != nil {
				slog.Error("Unable to send entry to Shiori",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.ShaarliEnabled {
		tasks = append(tasks, sendTask{provider: "Shaarli", send: func() error {
			slog.Debug("Sending entry to Shaarli",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := shaarli.NewClient(
				userIntegrations.ShaarliURL,
				userIntegrations.ShaarliAPISecret,
			)

			if err := client.CreateLink(entry.URL, entry.Title); err != nil {
				slog.Error("Unable to send entry to Shaarli",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.ArchiveorgEnabled {
		tasks = append(tasks, sendTask{provider: "Archive.org", send: func() error {
			slog.Debug("Sending entry to archive.org",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			if err := archiveorg.NewClient().SendURL(entry.URL); err != nil {
				slog.Error("Unable to send entry to Archive.org",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.WebhookEnabled {
		tasks = append(tasks, sendTask{provider: "Webhook", send: func() error {
			var webhookURL string
			if entry.Feed != nil && entry.Feed.WebhookURL != "" {
				webhookURL = entry.Feed.WebhookURL
			} else {
				webhookURL = userIntegrations.WebhookURL
			}

			slog.Debug("Sending entry to Webhook",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
				slog.String("webhook_url", webhookURL),
			)

			webhookClient := webhook.NewClient(webhookURL, userIntegrations.WebhookSecret)
			if err := webhookClient.SendSaveEntryWebhookEvent(entry); err != nil {
				slog.Error("Unable to send entry to Webhook",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.String("webhook_url", webhookURL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.OmnivoreEnabled {
		tasks = append(tasks, sendTask{provider: "Omnivore", send: func() error {
			slog.Debug("Sending entry to Omnivore",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := omnivore.NewClient(userIntegrations.OmnivoreAPIKey, userIntegrations.OmnivoreURL)
			if err := client.SaveURL(entry.URL); err != nil {
				slog.Error("Unable to send entry to Omnivore",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.KarakeepEnabled {
		tasks = append(tasks, sendTask{provider: "Karakeep", send: func() error {
			slog.Debug("Sending entry to Karakeep",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.String("user_tags", userIntegrations.KarakeepTags),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := karakeep.NewClient(
				userIntegrations.KarakeepAPIKey,
				userIntegrations.KarakeepURL,
				userIntegrations.KarakeepTags,
			)
			if err := client.SaveURL(entry.URL); err != nil {
				slog.Error("Unable to send entry to Karakeep",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.String("user_tags", userIntegrations.KarakeepTags),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
	}

	if userIntegrations.RaindropEnabled {
		tasks = append(tasks, sendTask{provider: "Raindrop", send: func() error {
			slog.Debug("Sending entry to Raindrop",
				slog.Int64("user_id", userIntegrations.UserID),
				slog.Int64("entry_id", entry.ID),
				slog.String("entry_url", entry.URL),
			)

			client := raindrop.NewClient(userIntegrations.RaindropToken, userIntegrations.RaindropCollectionID, userIntegrations.RaindropTags)
			if err := client.CreateRaindrop(entry.URL, entry.Title); err != nil {
				slog.Error("Unable to send entry to Raindrop",
					slog.Int64("user_id", userIntegrations.UserID),
					slog.Int64("entry_id", entry.ID),
					slog.String("entry_url", entry.URL),
					slog.Any("error", err),
				)
				return err
			}
			return nil
		}})
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
