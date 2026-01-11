package targets

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

type plexActivityManager struct {
	targetName string
	mu         sync.RWMutex
	trackers   map[string]*plexActivityTracker
	liveSubs   map[string]func(plexActivityNotification)
	nextID     uint64
}

func newPlexActivityManager(targetName string) *plexActivityManager {
	return &plexActivityManager{
		targetName: targetName,
		trackers:   make(map[string]*plexActivityTracker),
		liveSubs:   make(map[string]func(plexActivityNotification)),
	}
}

func (m *plexActivityManager) registerLiveSubscriber(cb func(plexActivityNotification)) string {
	if cb == nil {
		return ""
	}
	id := m.nextSubscriberID("live")
	m.mu.Lock()
	m.liveSubs[id] = cb
	m.mu.Unlock()
	return id
}

func (m *plexActivityManager) registerTracker(tracker *plexActivityTracker) {
	if tracker == nil {
		return
	}
	if tracker.id == "" {
		tracker.id = m.nextSubscriberID("tracker")
	}
	m.mu.Lock()
	m.trackers[tracker.id] = tracker
	m.mu.Unlock()
}

func (m *plexActivityManager) unregisterTracker(id string) {
	if id == "" {
		return
	}
	m.mu.Lock()
	delete(m.trackers, id)
	m.mu.Unlock()
}

func (m *plexActivityManager) handleActivityNotifications(activities []plexActivityNotification) {
	for _, activity := range activities {
		m.dispatchLive(activity)

		actType := activity.Activity.Type
		if !plexAnalysisActivityTypes[actType] {
			continue
		}

		subtitle := activity.Activity.Subtitle
		uuid := activity.Activity.UUID

		log.Trace().
			Str("target", m.targetName).
			Str("event", activity.Event).
			Str("activity", actType).
			Str("item", subtitle).
			Str("uuid", uuid).
			Msg("Plex analysis activity")

		if subtitle == "" {
			continue
		}

		m.notifyTrackers(subtitle, uuid, actType, activity.Event)
	}
}

func (m *plexActivityManager) dispatchLive(activity plexActivityNotification) {
	m.mu.RLock()
	if len(m.liveSubs) == 0 {
		m.mu.RUnlock()
		return
	}
	subs := make([]func(plexActivityNotification), 0, len(m.liveSubs))
	for _, cb := range m.liveSubs {
		subs = append(subs, cb)
	}
	m.mu.RUnlock()

	for _, cb := range subs {
		cb(activity)
	}
}

func (m *plexActivityManager) notifyTrackers(itemName, uuid, actType, event string) {
	m.mu.RLock()
	if len(m.trackers) == 0 {
		m.mu.RUnlock()
		return
	}
	trackers := make([]*plexActivityTracker, 0, len(m.trackers))
	for _, tracker := range m.trackers {
		trackers = append(trackers, tracker)
	}
	m.mu.RUnlock()

	for _, tracker := range trackers {
		tracker.mu.Lock()
		if tracker.allowTitleProbe {
			tracker.applySubtitleOverride(itemName)
		}
		matched, details := tracker.matchItemWithDetails(itemName)
		if !matched {
			tracker.mu.Unlock()
			continue
		}

		tracker.lastActivity = time.Now()
		tracker.scanStarted = true

		uuidUpdated := false
		switch event {
		case "started":
			tracker.activeUUIDs[uuid] = actType
			uuidUpdated = true
		case "updated":
			if _, ok := tracker.activeUUIDs[uuid]; !ok {
				tracker.activeUUIDs[uuid] = actType
				uuidUpdated = true
			}
		case "ended":
			if _, ok := tracker.activeUUIDs[uuid]; ok {
				delete(tracker.activeUUIDs, uuid)
				uuidUpdated = true
			}
		}

		trackerID := tracker.id
		trackerTitle := tracker.title
		trackerYear := tracker.year
		seasonPatterns := append([]string(nil), tracker.seasonPatterns...)
		activeCount := len(tracker.activeUUIDs)
		tracker.mu.Unlock()

		log.Debug().
			Str("target", m.targetName).
			Str("tracker_id", trackerID).
			Str("event", event).
			Str("activity", actType).
			Str("item", itemName).
			Str("title", trackerTitle).
			Str("year", trackerYear).
			Strs("season_patterns", seasonPatterns).
			Int("active_count", activeCount).
			Bool("idle_reset", true).
			Bool("scan_started", true).
			Msg("Plex activity matched tracker")

		if uuidUpdated {
			log.Debug().
				Str("target", m.targetName).
				Str("tracker_id", trackerID).
				Str("uuid", uuid).
				Str("event", event).
				Int("active_count", activeCount).
				Msg("Plex activity UUID updated")
		}

		log.Trace().
			Str("target", m.targetName).
			Str("tracker_id", trackerID).
			Strs("tracker_candidates", plexCandidateTitles(details.trackerCandidates)).
			Strs("item_candidates", plexCandidateTitles(details.itemCandidates)).
			Msg("Plex title candidates built")

		log.Trace().
			Str("target", m.targetName).
			Str("tracker_id", trackerID).
			Str("tracker_norm", details.titleMatch.trackerNormalized).
			Str("item_norm", details.titleMatch.itemNormalized).
			Bool("year_match", details.titleMatch.yearMatch).
			Str("match_strategy", details.titleMatch.strategy).
			Msg("Plex title compare")

		if len(seasonPatterns) > 0 {
			log.Trace().
				Str("target", m.targetName).
				Str("tracker_id", trackerID).
				Str("item", itemName).
				Strs("patterns", seasonPatterns).
				Str("matched_pattern", details.seasonMatchedPattern).
				Msg("Plex season pattern check")
		}
	}
}

func (m *plexActivityManager) nextSubscriberID(prefix string) string {
	id := atomic.AddUint64(&m.nextID, 1)
	return fmt.Sprintf("%s-%d", prefix, id)
}
