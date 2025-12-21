// SSE Client for real-time updates
(function() {
    'use strict';

    let eventSource = null;
    let reconnectAttempts = 0;
    const maxReconnectAttempts = 10;
    const baseReconnectDelay = 1000;

    // Map SSE events to HTMX element refreshes
    const eventTargets = {
        // Scan events trigger scan-related element refreshes
        'scan_queued': ['#dashboard-stats', '#recent-scans'],
        'scan_started': ['#dashboard-stats', '#recent-scans'],
        'scan_completed': ['#dashboard-stats', '#recent-scans'],
        'scan_failed': ['#dashboard-stats', '#recent-scans'],

        // Upload events trigger upload-related element refreshes
        'upload_queued': ['#dashboard-upload-stats', '#recent-uploads', '#upload-queue', '#upload-queue-stats', '#upload-queue-pagination'],
        'upload_started': ['#dashboard-upload-stats', '#recent-uploads', '#upload-queue', '#upload-queue-stats', '#upload-queue-pagination', '#active-transfers'],
        'upload_progress': ['#active-transfers'],
        'upload_completed': ['#dashboard-upload-stats', '#recent-uploads', '#upload-queue', '#upload-queue-stats', '#upload-queue-pagination', '#active-transfers'],
        'upload_failed': ['#dashboard-upload-stats', '#recent-uploads', '#upload-queue', '#upload-queue-stats', '#upload-queue-pagination', '#active-transfers'],

        // Session events trigger session and throttle refreshes
        'session_started': ['#active-sessions', '#throttle-status-section'],
        'session_ended': ['#active-sessions', '#throttle-status-section'],

        // Throttle changes
        'throttle_changed': ['#throttle-status-section']
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
            const el = document.querySelector(selector);
            if (el) {
                htmx.trigger(el, 'sse-refresh');
            }
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
        if (reconnectAttempts >= maxReconnectAttempts) {
            console.log('SSE max reconnect attempts reached, falling back to polling');
            enablePollingFallback();
            return;
        }

        const delay = baseReconnectDelay * Math.pow(2, reconnectAttempts);
        reconnectAttempts++;
        console.log('SSE reconnecting in ' + delay + 'ms (attempt ' + reconnectAttempts + ')');

        setTimeout(function() {
            connect();
        }, delay);
    }

    function enablePollingFallback() {
        // Re-enable HTMX polling on elements as fallback
        document.querySelectorAll('[data-sse-polling-fallback]').forEach(function(el) {
            const interval = el.getAttribute('data-sse-polling-fallback');
            if (interval) {
                el.setAttribute('hx-trigger', 'load, every ' + interval);
                htmx.process(el);
            }
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
