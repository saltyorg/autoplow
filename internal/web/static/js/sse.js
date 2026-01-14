// SSE Client for real-time updates
(function() {
    'use strict';

    let eventSource = null;
    let reconnectAttempts = 0;
    let fallbackReconnectAttempts = 0;
    let fallbackEnabled = false;
    const maxReconnectAttempts = 10;
    const baseReconnectDelay = 1000;
    const fallbackReconnectDelay = 30000;
    const fallbackReconnectMaxDelay = 600000;

    // Map SSE events to HTMX element refreshes
    const eventTargets = {
        // Scan events trigger scan-related element refreshes
        'scan_queued': ['#dashboard-stats', '#recent-scans', '#scan-history-stats', '#scan-history-table'],
        'scan_started': ['#dashboard-stats', '#recent-scans', '#scan-history-stats', '#scan-history-table'],
        'scan_completed': ['#dashboard-stats', '#recent-scans', '#scan-history-stats', '#scan-history-table'],
        'scan_failed': ['#dashboard-stats', '#recent-scans', '#scan-history-stats', '#scan-history-table'],

        // Upload events trigger upload-related element refreshes
        'upload_queued': ['#dashboard-upload-stats', '#recent-uploads', '#upload-queue', '#upload-queue-stats', '#upload-queue-pagination', '#upload-history-stats', '#upload-history-table'],
        'upload_started': ['#dashboard-upload-stats', '#recent-uploads', '#upload-queue', '#upload-queue-stats', '#upload-queue-pagination', '#active-transfers', '#upload-history-stats', '#upload-history-table'],
        'upload_progress': ['#active-transfers'],
        'upload_completed': ['#dashboard-upload-stats', '#recent-uploads', '#upload-queue', '#upload-queue-stats', '#upload-queue-pagination', '#active-transfers', '#upload-history-stats', '#upload-history-table'],
        'upload_failed': ['#dashboard-upload-stats', '#recent-uploads', '#upload-queue', '#upload-queue-stats', '#upload-queue-pagination', '#active-transfers', '#upload-history-stats', '#upload-history-table'],

        // Session events trigger session and throttle refreshes
        'session_started': ['#active-sessions', '#throttle-status-section'],
        'session_ended': ['#active-sessions', '#throttle-status-section'],

        // Throttle changes
        'throttle_changed': ['#throttle-status-section'],

        // Matcharr events trigger matcharr page refreshes
        'matcharr_run_started': ['#matcharr-status', '#matcharr-last-run', '#matcharr-history', '#matcharr-arr-gaps', '#matcharr-target-gaps', '#quick-actions', '#matcharr-tab-counts'],
        'matcharr_run_completed': ['#matcharr-status', '#matcharr-last-run', '#matcharr-history', '#matcharr-mismatches', '#matcharr-arr-gaps', '#matcharr-target-gaps', '#matcharr-file-mismatches', '#matcharr-file-ignores', '#quick-actions', '#matcharr-tab-counts'],
        'matcharr_run_failed': ['#matcharr-status', '#matcharr-last-run', '#matcharr-history', '#matcharr-arr-gaps', '#matcharr-target-gaps', '#matcharr-file-mismatches', '#matcharr-file-ignores', '#quick-actions', '#matcharr-tab-counts'],
        'matcharr_mismatch_updated': ['#matcharr-mismatches', '#matcharr-status', '#quick-actions', '#matcharr-tab-counts'],

        // Plex Auto Languages events
        'plex_auto_languages_track_changed': ['#pal-recent-activity', '#pal-history', '.pal-preferences-target', '#pal-status'],
        'notification_logged': ['#notification-stats', '#notification-logs'],
        'rclone_status_changed': ['#rclone-status-card'],
        'gdrive_snapshot_complete': ['#gdrive-snapshot-content']
    };

    // Debounce refresh requests to avoid hammering the server
    const pendingRefreshes = new Map();
    const debounceMs = 100;

    function scheduleRefresh(selector) {
        if (pendingRefreshes.has(selector)) {
            return; // Already scheduled
        }
        pendingRefreshes.set(selector, setTimeout(function() {
            pendingRefreshes.delete(selector);
            const nodes = document.querySelectorAll(selector);
            if (!nodes.length) {
                return;
            }
            nodes.forEach(function(el) {
                htmx.trigger(el, 'sse-refresh');
            });
        }, debounceMs));
    }

    function handleEvent(event) {
        const targets = eventTargets[event.type];
        if (!targets) {
            return;
        }
        targets.forEach(function(selector) {
            scheduleRefresh(selector);
        });
    }

    function connect() {
        if (eventSource) {
            eventSource.close();
        }

        eventSource = new EventSource('/api/events');

        eventSource.onopen = function() {
            console.log('SSE connected');
            reconnectAttempts = 0;
            fallbackReconnectAttempts = 0;
            if (fallbackEnabled) {
                console.log('SSE reconnected, disabling polling fallback');
                disablePollingFallback();
            }
        };

        eventSource.onerror = function() {
            console.log('SSE connection error');
            eventSource.close();
            eventSource = null;
            scheduleReconnect();
        };

        // Register handlers for all event types
        Object.keys(eventTargets).forEach(function(eventType) {
            eventSource.addEventListener(eventType, function(e) {
                handleEvent({ type: eventType, data: e.data });
            });
        });

        // Heartbeat keeps connection alive
        eventSource.addEventListener('heartbeat', function() {
            // Connection is healthy
        });
    }

    function scheduleReconnect() {
        if (fallbackEnabled) {
            scheduleFallbackReconnect();
            return;
        }

        if (reconnectAttempts >= maxReconnectAttempts) {
            console.log('SSE max reconnect attempts reached, falling back to polling');
            enablePollingFallback();
            scheduleFallbackReconnect();
            return;
        }

        const delay = baseReconnectDelay * Math.pow(2, reconnectAttempts);
        reconnectAttempts++;
        console.log('SSE reconnecting in ' + delay + 'ms (attempt ' + reconnectAttempts + ')');

        setTimeout(function() {
            connect();
        }, delay);
    }

    function scheduleFallbackReconnect() {
        const delay = Math.min(
            fallbackReconnectDelay * Math.pow(2, fallbackReconnectAttempts),
            fallbackReconnectMaxDelay
        );
        fallbackReconnectAttempts++;
        console.log('SSE reconnecting in fallback mode in ' + delay + 'ms');

        setTimeout(function() {
            connect();
        }, delay);
    }

    function enablePollingFallback() {
        if (fallbackEnabled) {
            return;
        }
        fallbackEnabled = true;

        // Re-enable HTMX polling on elements as fallback
        document.querySelectorAll('[data-sse-polling-fallback]').forEach(function(el) {
            if (!el.hasAttribute('data-sse-original-trigger')) {
                el.setAttribute('data-sse-original-trigger', el.getAttribute('hx-trigger') || '');
            }
            const interval = el.getAttribute('data-sse-polling-fallback');
            if (interval) {
                el.setAttribute('hx-trigger', 'load, every ' + interval);
                htmx.process(el);
            }
        });
    }

    function disablePollingFallback() {
        if (!fallbackEnabled) {
            return;
        }
        fallbackEnabled = false;

        document.querySelectorAll('[data-sse-original-trigger]').forEach(function(el) {
            const original = el.getAttribute('data-sse-original-trigger');
            if (original) {
                el.setAttribute('hx-trigger', original);
            } else {
                el.removeAttribute('hx-trigger');
            }
            el.removeAttribute('data-sse-original-trigger');
            htmx.process(el);
        });
    }

    // Initialize on page load
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', connect);
    } else {
        connect();
    }

    // Cleanup on page unload
    window.addEventListener('beforeunload', function() {
        if (eventSource) {
            eventSource.close();
        }
    });

    // Expose for debugging
    window.autoplow = window.autoplow || {};
    window.autoplow.sse = {
        reconnect: connect,
        getStatus: function() {
            return eventSource ? eventSource.readyState : -1;
        }
    };
})();
