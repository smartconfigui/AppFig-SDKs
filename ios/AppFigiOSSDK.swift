/**
 * AppFig iOS SDK - Client-side feature flags and remote configuration
 *
 * Version: 2.0.0
 * Platform: iOS (Swift)
 * Architecture: Client-side rule evaluation with CDN delivery
 *
 * Features:
 * - Local rule evaluation (zero latency)
 * - Event-based targeting with sequences
 * - User and device property targeting
 * - Automatic caching and background sync
 * - Offline-first architecture
 *
 * Cloud Mode Usage:
 *   // Initialize with auto-refresh enabled
 *   AppFig.initialize(
 *       companyId: "acmegames",
 *       tenantId: "spaceshooter",
 *       env: "prod",
 *       apiKey: "your-api-key",
 *       autoRefresh: true,
 *       pollInterval: 43200000  // 12 hours
 *   )
 *
 *   // Log events
 *   AppFig.logEvent(name: "level_complete", parameters: ["level": "5"])
 *
 *   // Check features
 *   let isEnabled = AppFig.isFeatureEnabled("double_xp")
 *   let value = AppFig.getFeatureValue("max_lives")
 *
 * Local Mode Usage (Development/Testing):
 *   // Initialize without API key
 *   let rulesJson = """{"features": {"my_feature": [{"value": "enabled", "conditions": {...}}]}}"""
 *   AppFig.initializeLocal(rulesJson: rulesJson)
 */

import Foundation
import UIKit

/// Main AppFig SDK class
/// Thread-safe static class for managing feature flags and remote configuration
public class AppFig {

    // MARK: - Constants

    private static let defaultPollIntervalMs: TimeInterval = 43200000 // 12 hours
    private static let defaultSessionTimeoutMs: TimeInterval = 1800000 // 30 minutes
    private static let defaultMaxEvents = 5000
    private static let defaultMaxEventAgeDays = 7
    private static let userDefaultsSuiteName = "com.appfig.sdk"

    // MARK: - State Management

    private static let queue = DispatchQueue(label: "com.appfig.sdk.queue", attributes: .concurrent)
    private static var urlSession: URLSession = {
        let config = URLSessionConfiguration.default
        config.timeoutIntervalForRequest = 30
        config.timeoutIntervalForResource = 30
        return URLSession(configuration: config)
    }()

    // Configuration
    private static var companyId: String = ""
    private static var tenantId: String = ""
    private static var env: String = ""
    private static var apiKey: String = ""
    private static var cdnBaseUrl: String = "https://rules-prod.appfig.com"
    private static var pointerUrl: String = ""
    private static var pollIntervalMs: TimeInterval = 43200000
    private static var autoRefreshEnabled: Bool = true
    private static var sessionTimeoutMs: TimeInterval = 1800000
    private static var debugMode: Bool = false
    private static var maxEvents: Int = 5000
    private static var maxEventAgeDays: Int = 7

    // Data storage
    private static var eventHistory: [EventRecord] = []
    private static var eventCounts: [String: Int] = [:]
    private static var userProperties: [String: String] = [:]
    private static var deviceProperties: [String: String] = [:]
    private static var featureCache: [String: String?] = [:]
    private static var features: [String: String?] = [:] // Active feature values (source of truth)
    private static var rules: [Rule] = []
    private static var currentRulesHash: String = ""
    private static var lastFetchTime: Date = Date(timeIntervalSince1970: 0)
    private static var isInitialized: Bool = false
    private static var isFetchInProgress: Bool = false

    // Session tracking
    private static var sessionActive: Bool = false
    private static var currentScreen: String = ""
    private static var lastActivity: Date = Date()

    // Performance optimization
    private static var eventToFeaturesIndex: [String: Set<String>] = [:]
    private static var userPropertyToFeaturesIndex: [String: Set<String>] = [:]
    private static var devicePropertyToFeaturesIndex: [String: Set<String>] = [:]
    private static var featureToRulesIndex: [String: [Rule]] = [:]

    // Local mode
    private static var useLocalMode: Bool = false
    private static var localRulesJson: String?

    // Background task
    private static var refreshTimer: Timer?

    // Event debouncing
    private static var eventsSavedCount: Int = 0
    private static var eventSaveDebounceTimer: DispatchWorkItem?

    // Schema discovery
    private static var schemaEvents: Set<String> = []
    private static var schemaEventParams: [String: Set<String>] = [:]
    private static var schemaEventParamValues: [String: [String: Set<String>]] = [:]
    private static var schemaUserProperties: [String: Set<String>] = [:]
    private static var schemaDeviceProperties: [String: Set<String>] = [:]
    private static var schemaDiffNewEvents: Set<String> = []
    private static var schemaDiffNewEventParams: [String: (params: Set<String>, paramValues: [String: Set<String>])] = [:]
    private static var schemaDiffNewUserProperties: [String: Set<String>] = [:]
    private static var schemaDiffNewDeviceProperties: [String: Set<String>] = [:]
    private static var schemaUploadCount: Int = 0
    private static var schemaUploadTimer: DispatchWorkItem?
    private static var deviceId: String = ""
    private static var lastSchemaUploadTime: TimeInterval = 0
    private static let SCHEMA_UPLOAD_BATCH_SIZE = 50
    private static let SCHEMA_UPLOAD_INTERVAL_MS: TimeInterval = 600000 // 10 minutes
    private static let SCHEMA_UPLOAD_THROTTLE_MS: TimeInterval = 43200000 // 12 hours

    // Callbacks and listeners
    private static var onReadyCallback: (() -> Void)?
    private static var onRulesUpdatedCallback: (() -> Void)?
    private static var featureListeners: [String: Set<FeatureListener>] = [:]

    private class FeatureListener: Hashable {
        let id = UUID()
        let callback: (String, String?) -> Void

        init(_ callback: @escaping (String, String?) -> Void) {
            self.callback = callback
        }

        static func == (lhs: FeatureListener, rhs: FeatureListener) -> Bool {
            lhs.id == rhs.id
        }

        func hash(into hasher: inout Hasher) {
            hasher.combine(id)
        }
    }

    // MARK: - Logging System

    private enum AppFigLogLevel {
        case info
        case warn
        case error
        case debug
    }

    private static func log(_ level: AppFigLogLevel, _ message: String) {
        // Errors and warnings always show
        if level == .warn || level == .error {
            if level == .error {
                print("[AppFig] ERROR: \(message)")
            } else {
                print("[AppFig] WARNING: \(message)")
            }
            return
        }

        // Info + Debug only show when debugMode=true
        guard debugMode else { return }

        if level == .info {
            print("[AppFig] \(message)")
        } else {
            print("[AppFig:Debug] \(message)")
        }
    }

    // MARK: - Initialization

    /// Initialize AppFig SDK in cloud mode (paid plans)
    ///
    /// - Parameters:
    ///   - companyId: Your Firestore company document ID (e.g., 'acmegames')
    ///   - tenantId: Your Firestore tenant document ID (e.g., 'spaceshooter')
    ///   - env: env: 'dev' or 'prod'
    ///   - apiKey: Your API key from AppFig dashboard
    ///   - autoRefresh: Enable automatic background rule updates (default: true)
    ///   - pollInterval: Auto-refresh interval in milliseconds (default: 12 hours)
    ///   - debugMode: Enable debug logging (default: false)
    ///   - sessionTimeoutMs: Session timeout in milliseconds (default: 30 minutes, range: 1 min - 2 hours)
    ///   - maxEvents: Maximum events to store (100-100000, default: 5000)
    ///   - maxEventAgeDays: Maximum age of events in days (1-365, default: 7)
    public static func initialize(
        companyId: String,
        tenantId: String,
        env: String,
        apiKey: String,
        autoRefresh: Bool = true,
        pollInterval: TimeInterval = 43200000,
        debugMode: Bool = false,
        sessionTimeoutMs: TimeInterval = 1800000,
        maxEvents: Int = 5000,
        maxEventAgeDays: Int = 7,
        onReady: (() -> Void)? = nil,
        onRulesUpdated: (() -> Void)? = nil
    ) {
        // Validate inputs
        guard !apiKey.isEmpty else {
            print("❌ [AppFig] API key is required for remote mode. Use initializeLocal() for local development.")
            return
        }

        guard !companyId.isEmpty else {
            print("❌ [AppFig] Company ID is required. This should be your Firestore company document ID.")
            return
        }

        guard !tenantId.isEmpty else {
            print("❌ [AppFig] Tenant ID is required. This should be your Firestore tenant document ID.")
            return
        }

        if companyId.contains(" ") {
            print("❌ [AppFig] Invalid company ID '\(companyId)' - IDs cannot contain spaces. Use the Firestore document ID.")
            return
        }

        if tenantId.contains(" ") {
            print("❌ [AppFig] Invalid tenant ID '\(tenantId)' - IDs cannot contain spaces. Use the Firestore document ID.")
            return
        }

        if companyId.count > 100 || tenantId.count > 100 {
            print("⚠️ [AppFig] Unusually long ID detected. Are you using the correct Firestore document IDs?")
        }

        // Initialize
        queue.async(flags: .barrier) {
            self.companyId = companyId
            self.tenantId = tenantId
            self.env = env
            self.apiKey = apiKey
            self.autoRefreshEnabled = autoRefresh
            self.pollIntervalMs = pollInterval
            self.debugMode = debugMode
            self.useLocalMode = false
            self.onReadyCallback = onReady
            self.onRulesUpdatedCallback = onRulesUpdated

            // Validate and set session timeout
            self.sessionTimeoutMs = sessionTimeoutMs
            let minSessionTimeout: TimeInterval = 60000 // 1 minute
            let maxSessionTimeout: TimeInterval = 7200000 // 2 hours
            if self.sessionTimeoutMs < minSessionTimeout || self.sessionTimeoutMs > maxSessionTimeout {
                log(.warn, "Session timeout \(self.sessionTimeoutMs)ms out of range. Clamping to \(minSessionTimeout)-\(maxSessionTimeout)ms")
                self.sessionTimeoutMs = max(minSessionTimeout, min(maxSessionTimeout, self.sessionTimeoutMs))
            }
            log(.info, "Initializing AppFig")

            // Validate and set event retention
            self.maxEvents = min(max(maxEvents, 100), 100000)
            self.maxEventAgeDays = min(max(maxEventAgeDays, 1), 365)

            if maxEvents != self.maxEvents {
                print("⚠️ [AppFig] maxEvents \(maxEvents) out of range. Clamped to \(self.maxEvents)")
            }

            if maxEventAgeDays != self.maxEventAgeDays {
                print("⚠️ [AppFig] maxEventAgeDays \(maxEventAgeDays) out of range. Clamped to \(self.maxEventAgeDays)")
            }

            if self.maxEvents > 10000 {
                print("⚠️ [AppFig] Large event limit (\(self.maxEvents)) may impact memory and storage.")
            }


            // Reset session and collect device info
            self.resetSession()
            self.collectDeviceProperties()
            self.logFirstOpenIfNeeded()
            self.startSessionTracking()

            // Load cached events
            self.loadCachedEvents()

            // Initialize schema discovery
            self.initSchemaDiscovery()

            // Set up CDN URLs
            self.pointerUrl = "\(self.cdnBaseUrl)/rules_versions/\(companyId)/\(tenantId)/\(env)/current/latest.json"


            // Load cached rules (DO NOT evaluate yet)
            let _ = self.loadCachedRules()

            // Fetch pointer to check for updates
            self.fetchRulesFromCDN {
                self.isInitialized = true

                // Start auto-refresh after first fetch completes
                if self.autoRefreshEnabled {
                    self.scheduleAutoRefresh()
                }
            }
        }
    }

    /// Initialize AppFig SDK in local mode (free plan)
    /// Rules are loaded from a JSON string instead of CDN
    ///
    /// LOCAL MODE BEHAVIOR:
    /// - Events are persisted to disk (same as cloud mode)
    /// - Event history maintained across app restarts
    /// - No companyId/tenantId required
    /// - Suitable for testing and development
    ///
    /// - Parameters:
    ///   - rulesJson: JSON string containing rules
    public static func initializeLocal(
        rulesJson: String? = nil,
        onReady: (() -> Void)? = nil,
        onRulesUpdated: (() -> Void)? = nil
    ) {
        queue.async(flags: .barrier) {
            self.useLocalMode = true
            self.localRulesJson = rulesJson
            self.onReadyCallback = onReady
            self.onRulesUpdatedCallback = onRulesUpdated

            // Set sentinel values for cache keys in local mode
            self.companyId = "local"
            self.tenantId = "local"
            self.env = "local"

            self.resetSession()
            self.collectDeviceProperties()
            self.detectCountryFromCDN() // Standalone country detection for local mode
            self.logFirstOpenIfNeeded()
            self.startSessionTracking()

            // Load cached events from disk (same as cloud mode)
            self.loadCachedEvents()

            print("🏠 [AppFig] Initialized in LOCAL MODE")

            if let rulesJson = rulesJson {
                self.parseAndApplyRules(rulesJson)
                // Fire onReady callback after rules are applied
                DispatchQueue.main.async {
                    self.onReadyCallback?()
                }
            } else {
                print("⚠️ [AppFig] No rules provided for local mode. Features will return nil.")
            }

            self.isInitialized = true
        }
    }

    // MARK: - Event Logging

    /// Log an event with optional parameters
    /// Events are stored locally and used for rule evaluation
    ///
    /// - Parameters:
    ///   - name: Name of the event
    ///   - parameters: Optional event parameters as key-value pairs
    public static func logEvent(name: String, parameters: [String: String]? = nil) {
        guard isInitialized else {
            print("⚠️ [AppFig] AppFig not initialized. Call initialize() first.")
            return
        }

        queue.async(flags: .barrier) {
            // Track schema
            self.trackEventSchema(eventName: name, parameters: parameters)

            let event = EventRecord(name: name, timestamp: Date(), parameters: parameters ?? [:])

            self.eventHistory.append(event)
            self.eventCounts[name, default: 0] += 1

            print("📝 [AppFig] Event logged: '\(name)' (count: \(self.eventCounts[name] ?? 0), total events: \(self.eventHistory.count))")

            // Trim old events
            self.trimEventHistory()


            // Debounce saving events (batch save every 10 events or 5 seconds)
            self.debounceSaveEvents()

            self.updateActivity()

            // Immediately re-evaluate all features
            self.evaluateAllFeatures()
        }
    }

    /// Log a screen view event
    ///
    /// - Parameters:
    ///   - screenName: Name of the screen
    ///   - previousScreen: Previous screen name (optional)
    public static func logScreenView(screenName: String, previousScreen: String? = nil) {
        var params = ["screen_name": screenName]
        if let previous = previousScreen {
            params["previous_screen"] = previous
        }
        logEvent(name: "screen_view", parameters: params)

        queue.async(flags: .barrier) {
            self.currentScreen = screenName
        }
    }

    // MARK: - Property Management

    /// Set a user property
    /// User properties persist across sessions and can be used in rules
    ///
    /// - Parameters:
    ///   - key: Property key
    ///   - value: Property value
    public static func setUserProperty(key: String, value: String) {
        queue.async(flags: .barrier) {
            self.userProperties[key] = value

            // Track schema
            self.trackUserPropertySchema(props: [key: value])


            print("👤 [AppFig] User property set: \(key) = \(value)")

            // Immediately re-evaluate all features
            // Always re-evaluate, even during init (removed isInitialized check)
            self.evaluateAllFeatures()
        }
    }

    /// Remove a user property
    ///
    /// - Parameter key: Property key to remove
    public static func removeUserProperty(key: String) {
        queue.async(flags: .barrier) {
            self.userProperties.removeValue(forKey: key)

            if let affectedFeatures = self.userPropertyToFeaturesIndex[key] {
            }

            print("👤 [AppFig] User property removed: \(key)")

            // Immediately re-evaluate all features
            // Always re-evaluate, even during init (removed isInitialized check)
            self.evaluateAllFeatures()
        }
    }

    /// Set a device property
    /// Device properties are typically set once during initialization
    ///
    /// - Parameters:
    ///   - key: Property key
    ///   - value: Property value
    public static func setDeviceProperty(key: String, value: String) {
        queue.async(flags: .barrier) {
            self.deviceProperties[key] = value

            // Track schema
            self.trackDevicePropertySchema(props: [key: value])


            print("📱 [AppFig] Device property set: \(key) = \(value)")

            // Immediately re-evaluate all features
            // Always re-evaluate, even during init (removed isInitialized check)
            self.evaluateAllFeatures()
        }
    }

    /// Remove a device property
    ///
    /// - Parameter key: Property key to remove
    public static func removeDeviceProperty(key: String) {
        queue.async(flags: .barrier) {
            self.deviceProperties.removeValue(forKey: key)


            print("📱 [AppFig] Device property removed: \(key)")

            // Immediately re-evaluate all features
            // Always re-evaluate, even during init (removed isInitialized check)
            self.evaluateAllFeatures()
        }
    }

    /// Set app version
    /// Convenience method to set the app version as a device property
    ///
    /// - Parameter version: App version string
    public static func setAppVersion(_ version: String) {
        setDeviceProperty(key: "app_version", value: version)
    }

    /// Detect and set country code using static endpoint
    /// @deprecated Country is now automatically detected from CDN response headers during rules fetch.
    /// This fallback method is kept for backward compatibility.
    /// This method makes a request to detect the user's country and automatically sets it as a device property
    ///
    /// - Parameter completion: Optional completion handler called with country code (or nil on failure)
    @available(*, deprecated, message: "Country is now automatically detected from CDN response headers during rules fetch.")
    public static func detectAndSetCountry(completion: ((String?) -> Void)? = nil) {
        guard let url = URL(string: "https://rules-dev.appfig.com/rules_versions/country/country/dev/current/latest.json") else {
            print("❌ [AppFig] Invalid country detection URL")
            completion?(nil)
            return
        }

        var request = URLRequest(url: url)
        request.timeoutInterval = 5

        let task = urlSession.dataTask(with: request) { data, response, _ in

            if let httpResponse = response as? HTTPURLResponse,
               let countryHeader = httpResponse.allHeaderFields["Country"] as? String {
                AppFig.setDeviceProperty(key: "country", value: countryHeader)
                completion?(countryHeader)
            } else {
                completion?(nil)
            }
        }

        task.resume()
    }

    // MARK: - Feature Flag Checks

    /// Check if a feature is enabled
    /// Returns true if the feature value is "true", "on", "enabled", or "1"
    ///
    /// - Parameter feature: Feature name
    /// - Returns: true if enabled, false otherwise
    public static func isFeatureEnabled(_ feature: String) -> Bool {
        guard let value = getFeatureValue(feature)?.lowercased() else { return false }
        return ["true", "on", "enabled", "1"].contains(value)
    }

    /// Get the value of a feature flag
    /// Returns nil if no matching rule is found
    ///
    /// - Parameter feature: Feature name
    /// - Returns: Feature value or nil
    public static func getFeatureValue(_ feature: String) -> String? {
        guard isInitialized else {
            print("⚠️ [AppFig] AppFig not initialized. Call initialize() first.")
            return nil
        }

        // Check if rules need refreshing (automatic updating)
        checkAndTriggerAutoRefresh()

        // Return cached value if exists
        if let value = features[feature] {
            return value
        }

        // Evaluate on-demand if not cached
        for rule in rules {
            if rule.feature == feature {
                let passed = evaluateConditions(rule.conditions)
                if passed {
                    let value = rule.value
                    features[feature] = value
                    return value
                }
            }
        }

        // No matching rule found
        features[feature] = nil
        return nil
    }

    private static func checkAndTriggerAutoRefresh() {
        // Only check if auto-refresh is enabled and not in local mode
        guard autoRefreshEnabled, !useLocalMode else {
            return
        }

        // Check if poll interval has elapsed
        let timeSinceLastFetch = Date().timeIntervalSince(lastFetchTime) * 1000
        if timeSinceLastFetch > TimeInterval(pollIntervalMs) {
            log(.info, "Fetching latest rules")
            // Trigger background refresh (non-blocking)
            queue.async {
                self.fetchRulesFromCDN()
            }
        }
    }

    /// Check if rules have been loaded
    ///
    /// - Returns: true if rules are loaded, false otherwise
    public static func hasRulesLoaded() -> Bool {
        return queue.sync { !rules.isEmpty }
    }

    /// Reset a specific feature's cached value and force re-evaluation
    /// Useful for implementing recurring triggers (e.g., "show popup every 3 events")
    ///
    /// Example usage:
    /// ```swift
    /// // Check if feature is enabled
    /// if AppFig.isFeatureEnabled("level_complete_popup") {
    ///     showPopup()
    ///     // Reset the feature so it can trigger again after next 3 events
    ///     AppFig.resetFeature("level_complete_popup")
    /// }
    /// ```
    ///
    /// - Parameter featureName: Name of the feature to reset
    public static func resetFeature(_ featureName: String) {
        queue.async(flags: .barrier) {
            guard self.isInitialized else {
                self.log(.warn, "Cannot reset feature: AppFig not initialized")
                return
            }

            guard !self.rules.isEmpty else {
                self.log(.warn, "Cannot reset feature: No rules loaded")
                return
            }

            self.log(.debug, "Resetting feature: \(featureName)")

            // Store old value for comparison
            let oldValue = self.features[featureName] ?? nil

            // Clear cached value
            self.features.removeValue(forKey: featureName)
            self.featureCache.removeValue(forKey: featureName)

            // Re-evaluate the feature immediately
            let newValue = self.evaluateFeature(featureName)

            // Update features map with new evaluation
            self.features[featureName] = newValue

            // Notify listeners if value changed
            if oldValue != newValue {
                self.log(.debug, "Feature value changed after reset: \(featureName) = \(newValue ?? "nil")")
                self.notifyListeners(changedFeatures: [featureName])
            } else {
                self.log(.debug, "Feature value unchanged after reset: \(featureName) = \(newValue ?? "nil")")
            }
        }
    }

    /// Reset all features and force complete re-evaluation
    /// This clears all cached feature values and re-evaluates all rules
    ///
    /// Use this sparingly - typically you want to reset specific features using resetFeature()
    public static func resetAllFeatures() {
        queue.async(flags: .barrier) {
            guard self.isInitialized else {
                self.log(.warn, "Cannot reset features: AppFig not initialized")
                return
            }

            guard !self.rules.isEmpty else {
                self.log(.warn, "Cannot reset features: No rules loaded")
                return
            }

            self.log(.debug, "Resetting all features")

            // Clear all caches
            self.features.removeAll()
            self.featureCache.removeAll()

            // Re-evaluate all features
            self.evaluateAllFeatures()

            self.log(.debug, "All features reset and re-evaluated")
        }
    }

    // MARK: - Feature Listeners

    public static func addListener(featureName: String, listener: @escaping (String, String?) -> Void) {
        queue.async(flags: .barrier) {
            let wrapper = FeatureListener(listener)
            if self.featureListeners[featureName] == nil {
                self.featureListeners[featureName] = []
            }
            self.featureListeners[featureName]?.insert(wrapper)
        }
    }

    public static func removeAllListeners(featureName: String) {
        queue.async(flags: .barrier) {
            self.featureListeners.removeValue(forKey: featureName)
        }
    }

    public static func clearAllListeners() {
        queue.async(flags: .barrier) {
            self.featureListeners.removeAll()
        }
    }

    // MARK: - Session Management

    /// Reset the session
    /// Clears session-related data but preserves events and properties
    public static func resetSession() {
        queue.async(flags: .barrier) {
            self.sessionActive = false
            self.currentScreen = ""
            self.lastActivity = Date()
        }
    }

    /// Update activity timestamp
    /// Call this when user interacts with the app to prevent session timeout
    public static func updateActivity() {
        queue.async(flags: .barrier) {
            let now = Date()
            let msSinceActivity = now.timeIntervalSince(self.lastActivity) * 1000

            if msSinceActivity > self.sessionTimeoutMs {
                // Session expired, log session_end
                if self.sessionActive {
                    self.logEvent(name: "session_end")
                    self.sessionActive = false
                }

                // Start new session
                self.logEvent(name: "session_start")
                self.sessionActive = true
            }

            self.lastActivity = now
        }
    }

    // MARK: - Rules Refresh

    /// Enable or disable auto-refresh
    /// When enabled, rules are automatically fetched at the configured poll interval
    ///
    /// - Warning: Deprecated. Use the `autoRefresh` parameter in `initialize()` instead.
    /// - Parameter enabled: true to enable auto-refresh, false to disable
    @available(*, deprecated, message: "Use the autoRefresh parameter in initialize() instead")
    public static func setAutoRefresh(enabled: Bool) {
        queue.async(flags: .barrier) {
            self.autoRefreshEnabled = enabled
            if enabled && self.isInitialized && !self.useLocalMode {
                self.scheduleAutoRefresh()
            } else {
                self.cancelAutoRefresh()
            }
        }
    }

    /// Set the polling interval for auto-refresh
    ///
    /// - Warning: Deprecated. Use the `pollInterval` parameter in `initialize()` instead.
    /// - Parameter intervalMs: Interval in milliseconds (min: 60000, default: 43200000)
    @available(*, deprecated, message: "Use the pollInterval parameter in initialize() instead")
    public static func setPollInterval(_ intervalMs: TimeInterval) {
        queue.async(flags: .barrier) {
            self.pollIntervalMs = max(intervalMs, 60000)
        }
    }

    /// Manually refresh rules from CDN
    /// Useful for implementing pull-to-refresh or manual update buttons
    public static func refreshRules() {
        guard !useLocalMode else {
            print("⚠️ [AppFig] Cannot refresh in local mode")
            return
        }

        guard isInitialized else {
            print("⚠️ [AppFig] Cannot refresh: AppFig not initialized")
            return
        }

        fetchRulesFromCDN()
    }

    // MARK: - Cache Management

    /// Clear all cached data for a specific company/tenant/env
    ///
    /// - Parameters:
    ///   - companyId: Company ID
    ///   - tenantId: Tenant ID
    ///   - env: env
    public static func clearCache(companyId: String, tenantId: String, env: String) {
        let defaults = UserDefaults.standard
        defaults.removeObject(forKey: getCacheKey(companyId, tenantId, env, "Rules"))
        defaults.removeObject(forKey: getCacheKey(companyId, tenantId, env, "Hash"))
        defaults.removeObject(forKey: getCacheKey(companyId, tenantId, env, "Timestamp"))
        print("🗑️ [AppFig] Cache cleared for \(companyId)/\(tenantId)/\(env)")
    }

    /// Clear event history
    /// Removes all logged events from memory and persistent storage
    public static func clearEventHistory() {
        queue.async(flags: .barrier) {
            self.eventHistory.removeAll()
            self.eventCounts.removeAll()
            self.saveCachedEvents()

            // Mark all features that depend on events as dirty
            for (_, affectedFeatures) in self.eventToFeaturesIndex {
            }

            print("🗑️ [AppFig] Event history cleared, re-evaluating all features")

            // Immediately re-evaluate all features
            self.evaluateAllFeatures()
        }
    }

    /// Get event history statistics
    ///
    /// - Returns: Dictionary with count, oldestEvent, and newestEvent
    public static func getEventHistoryStats() -> [String: Any] {
        return queue.sync {
            var stats: [String: Any] = ["count": eventHistory.count]

            if !eventHistory.isEmpty {
                if let oldest = eventHistory.min(by: { $0.timestamp < $1.timestamp }) {
                    stats["oldestEvent"] = "\(oldest.name) at \(oldest.timestamp)"
                }

                if let newest = eventHistory.max(by: { $0.timestamp < $1.timestamp }) {
                    stats["newestEvent"] = "\(newest.name) at \(newest.timestamp)"
                }
            } else {
                stats["oldestEvent"] = "N/A"
                stats["newestEvent"] = "N/A"
            }

            return stats
        }
    }

    // MARK: - Private Helpers - Device Properties

    private static func collectDeviceProperties() {
        setDeviceProperty(key: "platform", value: "iOS")
        setDeviceProperty(key: "os_version", value: UIDevice.current.systemVersion)
        setDeviceProperty(key: "device_model", value: UIDevice.current.model)
        setDeviceProperty(key: "sdk_version", value: "2.0.0")

        // Get app version
        if let version = Bundle.main.infoDictionary?["CFBundleShortVersionString"] as? String {
            setDeviceProperty(key: "app_version", value: version)
        }

        // Country will be auto-detected from CDN response headers during rules fetch
        // For local mode, detectCountryFromCDN() must be called separately
    }

    private static func detectCountryFromCDN() {
        guard let url = URL(string: "https://rules-dev.appfig.com/rules_versions/country/country/dev/current/latest.json") else {
            print("❌ [AppFig] Invalid standalone country detection URL")
            return
        }

        var request = URLRequest(url: url)
        request.timeoutInterval = 5

        let task = urlSession.dataTask(with: request) { _, response, _ in

            guard let httpResponse = response as? HTTPURLResponse else {
                print("⚠️ [AppFig] Invalid response from country detection endpoint")
                return
            }

            if httpResponse.statusCode == 200 {
                if let countryHeader = extractCountryHeader(from: httpResponse) {
                    AppFig.queue.async(flags: .barrier) {
                        AppFig.deviceProperties["country"] = countryHeader
                    }
                } else {
                }
            } else {
                print("⚠️ [AppFig] Country detection endpoint returned HTTP \(httpResponse.statusCode)")
            }
        }

        task.resume()
    }

    private static func logFirstOpenIfNeeded() {
        let defaults = UserDefaults.standard
        let hasLoggedFirstOpen = defaults.bool(forKey: "appfig_first_open_logged")
        if !hasLoggedFirstOpen {
            logEvent(name: "first_open")
            defaults.set(true, forKey: "appfig_first_open_logged")
        }
    }

    private static func startSessionTracking() {
        logEvent(name: "session_start")
        queue.async(flags: .barrier) {
            self.sessionActive = true
        }
    }

    // MARK: - Private Helpers - Rules Fetching

    /// Extract country header from HTTP response with case-insensitive lookup
    /// iOS allHeaderFields dictionary keys can vary in capitalization
    private static func extractCountryHeader(from response: HTTPURLResponse) -> String? {
        // Try common capitalizations
        let headerVariants = ["Country", "country", "COUNTRY"]

        for variant in headerVariants {
            if let value = response.allHeaderFields[variant] as? String {
                return value
            }
        }

        // Fallback: case-insensitive search through all headers
        for (key, value) in response.allHeaderFields {
            if let keyStr = key as? String, keyStr.lowercased() == "country" {
                return value as? String
            }
        }

        return nil
    }

    private static func fetchRulesFromCDN(completion: (() -> Void)? = nil) {
        queue.async(flags: .barrier) {
            guard !self.isFetchInProgress else { return }
            self.isFetchInProgress = true
            self.lastFetchTime = Date()
        }

        guard let url = URL(string: pointerUrl) else {
            print("❌ [AppFig] Invalid pointer URL")
            queue.async(flags: .barrier) { self.isFetchInProgress = false }
            completion?()
            return
        }

        var request = URLRequest(url: url)
        request.cachePolicy = .reloadIgnoringLocalCacheData
        request.setValue(apiKey, forHTTPHeaderField: "x-api-key")
        request.setValue("no-cache", forHTTPHeaderField: "Cache-Control")

        let task = urlSession.dataTask(with: request) { data, response, error in
            if let error = error {
                print("❌ [AppFig] Failed to fetch pointer: \(error.localizedDescription)")
                queue.async(flags: .barrier) { self.isFetchInProgress = false }
                completion?()
                return
            }

            guard let httpResponse = response as? HTTPURLResponse else {
                print("❌ [AppFig] Invalid response from pointer URL")
                queue.async(flags: .barrier) { self.isFetchInProgress = false }
                completion?()
                return
            }

            guard httpResponse.statusCode == 200 else {
                print("❌ [AppFig] Failed to fetch pointer: HTTP \(httpResponse.statusCode)")
                print("❌ [AppFig] Verify your companyId ('\(AppFig.companyId)') and tenantId ('\(AppFig.tenantId)') are correct Firestore document IDs")
                queue.async(flags: .barrier) { self.isFetchInProgress = false }
                completion?()
                return
            }

            guard let data = data else {
                print("❌ [AppFig] Empty response from pointer URL")
                queue.async(flags: .barrier) { self.isFetchInProgress = false }
                completion?()
                return
            }

            // Extract Country header from CDN response
            if let countryHeader = extractCountryHeader(from: httpResponse) {
                AppFig.queue.async(flags: .barrier) {
                    AppFig.deviceProperties["country"] = countryHeader
                }
            }

            do {
                let pointer = try JSONDecoder().decode(PointerData.self, from: data)

                // Enforce minimum poll interval from server
                if let minPollIntervalSecs = pointer.minPollIntervalSecs, minPollIntervalSecs > 0 {
                    let minPollIntervalMs = TimeInterval(minPollIntervalSecs * 1000)
                    if AppFig.pollIntervalMs < minPollIntervalMs {
                        AppFig.queue.async(flags: .barrier) {
                            AppFig.pollIntervalMs = minPollIntervalMs
                        }
                    }
                }

                AppFig.queue.sync {
                    let cachedHash = AppFig.getCachedHash()
                    if let cachedHash = cachedHash, cachedHash == pointer.version {
                        // Hash matches - use cached rules
                        AppFig.queue.async(flags: .barrier) {
                            AppFig.saveCacheTimestamp()

                            // Build index and evaluate once
                            AppFig.buildIndexes()
                            for rule in AppFig.rules {
                            }
                            AppFig.evaluateAllFeatures()

                            // Fire onReady callback
                            DispatchQueue.main.async {
                                AppFig.onReadyCallback?()
                            }

                            AppFig.isFetchInProgress = false
                        }
                        completion?()
                        return
                    }

                    // Hash doesn't match - fetch immutable rules
                    let immutableUrl = "\(AppFig.cdnBaseUrl)/rules_versions/\(AppFig.companyId)/\(AppFig.tenantId)/\(AppFig.env)/current/\(pointer.version).json"
                    AppFig.fetchImmutableRules(immutableUrl: immutableUrl, hash: pointer.version, completion: completion)
                }

            } catch {
                print("❌ [AppFig] Failed to parse pointer JSON: \(error)")
                queue.async(flags: .barrier) { self.isFetchInProgress = false }
                completion?()
            }
        }

        task.resume()
    }

    private static func fetchImmutableRules(immutableUrl: String, hash: String, completion: (() -> Void)? = nil) {
        guard let url = URL(string: immutableUrl) else {
            print("❌ [AppFig] Invalid immutable URL")
            completion?()
            return
        }

        var request = URLRequest(url: url)
        request.setValue(apiKey, forHTTPHeaderField: "x-api-key")

        let task = urlSession.dataTask(with: request) { data, response, error in
            if let error = error {
                print("❌ [AppFig] Failed to fetch immutable rules: \(error.localizedDescription)")
                completion?()
                return
            }

            guard let httpResponse = response as? HTTPURLResponse else {
                print("❌ [AppFig] Invalid response from immutable URL")
                completion?()
                return
            }

            guard httpResponse.statusCode == 200 else {
                print("❌ [AppFig] Failed to fetch immutable rules: HTTP \(httpResponse.statusCode)")
                completion?()
                return
            }

            guard let data = data, let rulesJson = String(data: data, encoding: .utf8) else {
                print("❌ [AppFig] Empty response from immutable URL")
                completion?()
                return
            }


            // Parse and apply rules
            AppFig.parseAndApplyRules(rulesJson, fireCallback: true)

            // Save to cache
            AppFig.queue.async(flags: .barrier) {
                AppFig.saveCachedRules(rulesJson, hash: hash)
                AppFig.isFetchInProgress = false
            }

            DispatchQueue.main.async {
                completion?()
            }
        }

        task.resume()
    }

    private static func parseAndApplyRules(_ rulesJson: String, fireCallback: Bool = false) {
        guard let data = rulesJson.data(using: .utf8) else {
            print("❌ [AppFig] Failed to convert rules JSON to data")
            return
        }

        do {
            let featureWrapper = try JSONDecoder().decode(FeatureWrapper.self, from: data)
            var newRules: [Rule] = []

            for (featureName, ruleSets) in featureWrapper.features {
                for ruleSet in ruleSets {
                    newRules.append(Rule(feature: featureName, value: ruleSet.value, conditions: ruleSet.conditions))
                }
            }

            queue.async(flags: .barrier) {
                self.rules = newRules
                self.featureCache.removeAll()
                self.features.removeAll()

                // Build index and evaluate once
                self.buildIndexes()
                for rule in self.rules {
                }
                self.evaluateAllFeatures()

                // Fire onRulesUpdated callback if requested
                if fireCallback {
                    DispatchQueue.main.async {
                        self.onRulesUpdatedCallback?()
                    }
                }
            }
        } catch {
            // If v2 format fails, try legacy format (direct features object without wrapper)

            do {
                // Wrap the JSON in a features key and retry
                let wrappedJson = "{\"features\": \(rulesJson)}"
                guard let wrappedData = wrappedJson.data(using: .utf8) else {
                    print("❌ [AppFig] Failed to create wrapped JSON")
                    return
                }

                let featureWrapper = try JSONDecoder().decode(FeatureWrapper.self, from: wrappedData)
                var newRules: [Rule] = []

                for (featureName, ruleSets) in featureWrapper.features {
                    for ruleSet in ruleSets {
                        newRules.append(Rule(feature: featureName, value: ruleSet.value, conditions: ruleSet.conditions))
                    }
                }

                queue.async(flags: .barrier) {
                    self.rules = newRules
                    self.featureCache.removeAll()
                    self.features.removeAll()
                    self.buildIndexes()

                    // Trigger initial evaluation of all features
                    self.evaluateAllFeatures()

                    // Notify that rules were updated
                    DispatchQueue.main.async {
                        self.onRulesUpdatedCallback?()
                    }
                }


            } catch {
                print("❌ [AppFig] Failed to parse rules JSON in both formats: \(error)")
                print("❌ [AppFig] Expected format: {\"features\": {\"feature_name\": [...rules]}}")
            }
        }
    }

    private static func buildIndexes() {
        eventToFeaturesIndex.removeAll()
        userPropertyToFeaturesIndex.removeAll()
        devicePropertyToFeaturesIndex.removeAll()
        featureToRulesIndex.removeAll()

        var eventIndexCount = 0
        var userPropIndexCount = 0
        var devicePropIndexCount = 0

        for rule in rules {
            let featureName = rule.feature

            // Build feature-to-rules index for O(1) rule lookup during evaluation
            featureToRulesIndex[featureName, default: []].append(rule)

            // Index events from both simple and sequence modes
            let eventsConfig = rule.conditions.events

            // Index events from events array regardless of mode
            if let events = eventsConfig.events {
                for eventCond in events {
                    eventToFeaturesIndex[eventCond.key, default: []].insert(featureName)
                    eventIndexCount += 1
                }
            }

            // Index user properties
            if let userProps = rule.conditions.userProperties {
                for prop in userProps {
                    userPropertyToFeaturesIndex[prop.key, default: []].insert(featureName)
                    userPropIndexCount += 1
                }
            }

            // Index device properties
            if let deviceProps = rule.conditions.device {
                for prop in deviceProps {
                    devicePropertyToFeaturesIndex[prop.key, default: []].insert(featureName)
                    devicePropIndexCount += 1
                }
            }
        }


        currentRulesHash = computeRulesHash()
        saveIndexToUserDefaults()
    }

    /**
     * Evaluate ALL features completely (Unity SDK approach)
     * No dirty feature tracking - always evaluate everything
     * Simpler, more reliable, and matches Unity SDK behavior
     */
    private static func evaluateAllFeatures() {

        guard !rules.isEmpty else {
            return
        }


        var changedFeatures = Set<String>()

        // Evaluate each feature using the index for O(1) rule lookup
        for (featureName, featureRules) in featureToRulesIndex {
            let oldValue = features[featureName] ?? nil
            var newValue: String? = nil

            // Find first matching rule for this feature (no linear search needed!)
            for rule in featureRules {
                if evaluateConditions(rule.conditions) {
                    newValue = rule.value
                    break
                }
            }

            // Set feature value (nil if no match)
            features[featureName] = newValue

            if oldValue != newValue {
                log(.debug, "Feature updated: \(featureName) = \(newValue ?? "nil")")
                changedFeatures.insert(featureName)
            }
        }

        // Remove orphaned features not in current rules
        let toRemove = features.keys.filter { !featureToRulesIndex.keys.contains($0) }
        for key in toRemove {
            features.removeValue(forKey: key)
            changedFeatures.insert(key)
        }

        // Notify listeners of changed features
        if !changedFeatures.isEmpty {
            notifyListeners(changedFeatures: changedFeatures)
        }
    }

    private static func notifyListeners(changedFeatures: Set<String>) {
        DispatchQueue.main.async {
            for featureName in changedFeatures {
                if let listeners = featureListeners[featureName] {
                    let value = features[featureName] ?? nil
                    for listener in listeners {
                        listener.callback(featureName, value)
                    }
                }
            }
        }
    }

    private static func computeRulesHash() -> String {
        let encoder = JSONEncoder()
        encoder.outputFormatting = .sortedKeys
        if let data = try? encoder.encode(rules),
           let jsonString = String(data: data, encoding: .utf8) {
            return String(jsonString.hashValue)
        }
        return ""
    }

    private static func saveIndexToUserDefaults() {
        guard let defaults = UserDefaults(suiteName: userDefaultsSuiteName) else { return }

        defaults.set(currentRulesHash, forKey: "AppFig_IndexHash")
        defaults.set(serializeIndex(eventToFeaturesIndex), forKey: "AppFig_EventIndex")
        defaults.set(serializeIndex(userPropertyToFeaturesIndex), forKey: "AppFig_UserPropIndex")
        defaults.set(serializeIndex(devicePropertyToFeaturesIndex), forKey: "AppFig_DeviceIndex")

    }

    private static func loadIndexFromUserDefaults() {
        guard let defaults = UserDefaults(suiteName: userDefaultsSuiteName) else { return }

        let savedHash = defaults.string(forKey: "AppFig_IndexHash")
        guard let savedHash = savedHash, savedHash == currentRulesHash else {
            return
        }

        let eventIndexJson = defaults.string(forKey: "AppFig_EventIndex") ?? ""
        let userPropIndexJson = defaults.string(forKey: "AppFig_UserPropIndex") ?? ""
        let deviceIndexJson = defaults.string(forKey: "AppFig_DeviceIndex") ?? ""

        eventToFeaturesIndex = deserializeIndex(eventIndexJson)
        userPropertyToFeaturesIndex = deserializeIndex(userPropIndexJson)
        devicePropertyToFeaturesIndex = deserializeIndex(deviceIndexJson)

    }

    private static func serializeIndex(_ index: [String: Set<String>]) -> String {
        return index.map { key, features in
            "\(key):\(features.joined(separator: ","))"
        }.joined(separator: "|")
    }

    private static func deserializeIndex(_ serialized: String) -> [String: Set<String>] {
        var result: [String: Set<String>] = [:]
        guard !serialized.isEmpty else { return result }

        for pair in serialized.split(separator: "|") {
            let parts = pair.split(separator: ":")
            if parts.count == 2 {
                let key = String(parts[0])
                let features = Set(parts[1].split(separator: ",").map(String.init))
                result[key] = features
            }
        }
        return result
    }

    private static func scheduleAutoRefresh() {
        DispatchQueue.main.async {
            self.refreshTimer?.invalidate()
            self.refreshTimer = Timer.scheduledTimer(withTimeInterval: self.pollIntervalMs / 1000.0, repeats: true) { _ in
                guard AppFig.autoRefreshEnabled, !AppFig.useLocalMode else { return }
                AppFig.refreshRules()
            }
        }
    }

    private static func cancelAutoRefresh() {
        DispatchQueue.main.async {
            self.refreshTimer?.invalidate()
            self.refreshTimer = nil
        }
    }

    // MARK: - Private Helpers - Rule Evaluation

    private static func evaluateFeature(_ featureName: String) -> String? {
        let matchingRules = rules.filter { $0.feature == featureName }
        guard !matchingRules.isEmpty else { return nil }

        for rule in matchingRules {
            if evaluateConditions(rule.conditions) {
                return rule.value
            }
        }

        return nil
    }

    private static func evaluateConditions(_ conditions: RuleConditions) -> Bool {
        let eventsPassed = evaluateEvents(conditions.events)
        let userPropsPassed = evaluateProperties(
            conditions.userProperties ?? [],
            operator: conditions.userPropertiesOperator ?? "AND",
            propertyMap: userProperties
        )
        let devicePassed = evaluateProperties(
            conditions.device ?? [],
            operator: conditions.deviceOperator ?? "AND",
            propertyMap: deviceProperties
        )

        return eventsPassed && userPropsPassed && devicePassed
    }

    private static func evaluateEvents(_ eventsConfig: EventsConfig) -> Bool {
        guard let events = eventsConfig.events, !events.isEmpty else { return true }

        let mode = eventsConfig.mode ?? "simple"

        switch mode {
        case "simple":
            let op = eventsConfig.operator ?? "AND"
            return evaluateSimpleEvents(events, operator: op)
        case "sequence":
            let ordering = eventsConfig.ordering ?? "direct"
            return evaluateSequenceEvents(events, ordering: ordering)
        default:
            return false
        }
    }

    private static func evaluateSimpleEvents(_ conditions: [EventCondition], operator op: String) -> Bool {

        switch op {
        case "OR":
            for condition in conditions {
                let result = evaluateEventCondition(condition)
                let finalResult = condition.not ? !result : result
                if finalResult {
                    return true
                }
            }
            return false
        case "AND":
            for condition in conditions {
                let result = evaluateEventCondition(condition)
                let finalResult = condition.not ? !result : result
                if !finalResult {
                    return false
                }
            }
            return true
        default:
            return false
        }
    }

    private static func evaluateEventCondition(_ condition: EventCondition) -> Bool {
        guard !condition.key.isEmpty else {
            return false
        }

        // Use operator for event name matching (default to "==" if not specified)
        let eventOperator = condition.operator ?? "=="

        var matchingEvents = eventHistory.filter { compareValue(actual: $0.name, operator: eventOperator, expected: condition.key) }


        // Apply time window filter if specified
        if let days = condition.withinLastDays, days > 0 {
            // Validate days to prevent overflow (max 365 as per config)
            let safeDays = min(days, 365)
            // Calculate seconds safely: 86400 seconds per day
            let secondsToSubtract = TimeInterval(safeDays) * 86400.0
            let cutoffTime = Date().addingTimeInterval(-secondsToSubtract)
            matchingEvents = matchingEvents.filter { $0.timestamp >= cutoffTime }
        }

        // Check count operator if specified
        if let countOp = condition.count {
            let actualCount = matchingEvents.count
            let countPassed = compareCount(actual: actualCount, operator: countOp.operator, expected: countOp.value)
            if !countPassed {
                return false
            }
        }

        // Check parameter conditions if specified
        if let paramConditions = condition.param, !paramConditions.isEmpty {
            let hasMatchingEvent = matchingEvents.contains { event in
                paramConditions.allSatisfy { (paramKey, opValue) in
                    guard let eventValue = event.parameters[paramKey] else { return false }
                    return compareValue(actual: eventValue, operator: opValue.operator, expected: opValue.value)
                }
            }
            if !hasMatchingEvent {
                return false
            }
        }

        // If we have a count condition, it must have passed
        // If we have param conditions, they must have passed
        // Otherwise, we just need at least one matching event
        return !matchingEvents.isEmpty
    }

    private static func evaluateSequenceEvents(_ steps: [EventCondition], ordering: String) -> Bool {

        if ordering == "direct" {
            return evaluateDirectSequence(steps)
        } else {
            return evaluateIndirectSequence(steps)
        }
    }

    private static func evaluateDirectSequence(_ steps: [EventCondition]) -> Bool {

        // Calculate minimum events needed with overflow protection
        var minEventsNeeded = 0
        for step in steps {
            var stepMinCount = 1
            if let count = step.count {
                // Validate count value to prevent overflow
                let safeCountValue = min(max(count.value, 0), 100000)
                switch count.operator {
                case "==": stepMinCount = safeCountValue
                case ">=": stepMinCount = safeCountValue
                case ">":
                    // Prevent overflow on addition
                    if safeCountValue < Int.max {
                        stepMinCount = safeCountValue + 1
                    } else {
                        stepMinCount = Int.max
                    }
                case "<=", "<": stepMinCount = 1
                default: stepMinCount = 1
                }
            }
            // Check for overflow before adding
            if minEventsNeeded > Int.max - stepMinCount {
                minEventsNeeded = Int.max
                break
            }
            minEventsNeeded += stepMinCount
        }

        // Try to find a consecutive run of events matching all steps
        let maxStartIdx = eventHistory.count - minEventsNeeded
        for startIdx in 0...max(0, maxStartIdx) {
            var sequenceMatched = true
            var eventIdx = startIdx


            for stepIdx in 0..<steps.count {
                let step = steps[stepIdx]
                var matchedCount = 0

                // Determine consumption strategy based on count operator
                let countOperator = step.count?.operator ?? ">="
                let countValue = step.count?.value ?? 1
                let maxEventsToConsume: Int
                switch countOperator {
                case "==": maxEventsToConsume = countValue
                case "<=": maxEventsToConsume = countValue
                case "<": maxEventsToConsume = countValue - 1
                default: maxEventsToConsume = Int.max // >=, > operators are greedy
                }


                // Try to match consecutive events for this step
                while eventIdx < eventHistory.count {
                    let evt = eventHistory[eventIdx]
                    let stepOperator = step.operator ?? "=="

                    if compareValue(actual: evt.name, operator: stepOperator, expected: step.key) {
                        // Check time window first
                        var withinTimeWindow = true
                        if let days = step.withinLastDays, days > 0 {
                            let safeDays = min(days, 365)
                            let secondsToSubtract = TimeInterval(safeDays) * 86400.0
                            let cutoffTime = Date().addingTimeInterval(-secondsToSubtract)
                            if evt.timestamp < cutoffTime {
                                withinTimeWindow = false
                            }
                        }

                        // Check parameters
                        let stepMatch = checkStepMatch(event: evt, step: step)

                        if withinTimeWindow && stepMatch {
                            matchedCount += 1
                            eventIdx += 1

                            // Check if we've consumed enough events
                            if matchedCount >= maxEventsToConsume {
                                break
                            }
                        } else {
                            // Event name matches but fails other checks
                            break
                        }
                    } else {
                        // Different event type stops this step's matching
                        break
                    }
                }


                // Check if count condition is satisfied
                let countSatisfied = compareCount(actual: matchedCount, operator: countOperator, expected: countValue)
                if !countSatisfied {
                    sequenceMatched = false
                    break
                }

            }

            if sequenceMatched {
                return true
            } else {
                print("[AppFig] ❌ Direct sequence failed starting at index \(startIdx), trying next position")
            }
        }

        return false
    }

    private static func evaluateIndirectSequence(_ steps: [EventCondition]) -> Bool {

        var lastMatchedIndex = -1

        for step in steps {
            // Check if this step has a count condition
            if let count = step.count {
                let op = count.operator
                let threshold = count.value

                var matchedCount = 0
                var latestMatchIndex = lastMatchedIndex

                // Prevent overflow when incrementing index
                let startIndex = lastMatchedIndex < Int.max ? lastMatchedIndex + 1 : Int.max
                for i in startIndex..<eventHistory.count {
                    let evt = eventHistory[i]
                    let stepOperator = step.operator ?? "=="

                    if compareValue(actual: evt.name, operator: stepOperator, expected: step.key) && checkStepMatch(event: evt, step: step) {
                        // Check time window
                        if let days = step.withinLastDays, days > 0 {
                            let safeDays = min(days, 365)
                            let secondsToSubtract = TimeInterval(safeDays) * 86400.0
                            let cutoffTime = Date().addingTimeInterval(-secondsToSubtract)
                            if evt.timestamp < cutoffTime {
                                continue
                            }
                        }
                        matchedCount += 1
                        latestMatchIndex = i
                    }
                }

                let countMatched = compareCount(actual: matchedCount, operator: op, expected: threshold)

                if !countMatched {
                    return false
                }

                lastMatchedIndex = latestMatchIndex
            } else {
                // No count condition, just check if at least one matching event exists
                var stepMatched = false

                // Prevent overflow when incrementing index
                let startIndex = lastMatchedIndex < Int.max ? lastMatchedIndex + 1 : Int.max
                for i in startIndex..<eventHistory.count {
                    let evt = eventHistory[i]
                    let stepOperator = step.operator ?? "=="

                    if compareValue(actual: evt.name, operator: stepOperator, expected: step.key) && checkStepMatch(event: evt, step: step) {
                        // Check time window
                        if let days = step.withinLastDays, days > 0 {
                            let safeDays = min(days, 365)
                            let secondsToSubtract = TimeInterval(safeDays) * 86400.0
                            let cutoffTime = Date().addingTimeInterval(-secondsToSubtract)
                            if evt.timestamp < cutoffTime {
                                continue
                            }
                        }

                        stepMatched = true
                        lastMatchedIndex = i
                        break
                    }
                }

                if !stepMatched {
                    return false
                }
            }
        }

        return true
    }

    private static func checkStepMatch(event: EventRecord, step: EventCondition) -> Bool {
        guard let paramConditions = step.param, !paramConditions.isEmpty else {
            return true
        }

        return paramConditions.allSatisfy { (paramKey, opValue) in
            guard let actualValue = event.parameters[paramKey] else { return false }
            return compareValue(actual: actualValue, operator: opValue.operator, expected: opValue.value)
        }
    }

    private static func compareCount(actual: Int, operator op: String, expected: Int) -> Bool {
        switch op {
        case "==": return actual == expected
        case "!=": return actual != expected
        case ">": return actual > expected
        case "<": return actual < expected
        case ">=": return actual >= expected
        case "<=": return actual <= expected
        default: return false
        }
    }

    private static func evaluateProperties(
        _ properties: [UserOrDeviceCondition],
        operator op: String,
        propertyMap: [String: String]
    ) -> Bool {
        guard !properties.isEmpty else { return true }

        switch op {
        case "AND":
            return properties.allSatisfy { evaluateProperty($0, propertyMap: propertyMap) }
        case "OR":
            return properties.contains { evaluateProperty($0, propertyMap: propertyMap) }
        default:
            return false
        }
    }

    private static func evaluateProperty(_ condition: UserOrDeviceCondition, propertyMap: [String: String]) -> Bool {
        guard let actualValue = propertyMap[condition.key] else { return false }

        let result = compareValue(actual: actualValue, operator: condition.value.operator, expected: condition.value.value)
        return condition.not ? !result : result
    }

    private static func compareValue(actual: Any, operator op: String, expected: Any) -> Bool {
        let actualStr = String(describing: actual)
        let expectedStr = String(describing: expected)

        switch op {
        case "==":
            return actualStr == expectedStr
        case "!=":
            return actualStr != expectedStr
        case "==_ci":
            return actualStr.lowercased() == expectedStr.lowercased()
        case "!=_ci":
            return actualStr.lowercased() != expectedStr.lowercased()
        case ">":
            if let a = Double(actualStr), let e = Double(expectedStr) {
                return a > e
            } else {
                return actualStr.compare(expectedStr) == .orderedDescending
            }
        case "<":
            if let a = Double(actualStr), let e = Double(expectedStr) {
                return a < e
            } else {
                return actualStr.compare(expectedStr) == .orderedAscending
            }
        case ">=":
            if let a = Double(actualStr), let e = Double(expectedStr) {
                return a >= e
            } else {
                let result = actualStr.compare(expectedStr)
                return result == .orderedDescending || result == .orderedSame
            }
        case "<=":
            if let a = Double(actualStr), let e = Double(expectedStr) {
                return a <= e
            } else {
                let result = actualStr.compare(expectedStr)
                return result == .orderedAscending || result == .orderedSame
            }
        case "in":
            let expectedList: [String]
            if let arr = expected as? [Any] {
                expectedList = arr.map { String(describing: $0).lowercased() }
            } else {
                expectedList = expectedStr.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces).lowercased() }
            }
            return expectedList.contains(actualStr.lowercased())
        case "not_in":
            let expectedList: [String]
            if let arr = expected as? [Any] {
                expectedList = arr.map { String(describing: $0).lowercased() }
            } else {
                expectedList = expectedStr.split(separator: ",").map { $0.trimmingCharacters(in: .whitespaces).lowercased() }
            }
            return !expectedList.contains(actualStr.lowercased())
        case "contains", "contains_ci":
            return actualStr.localizedCaseInsensitiveContains(expectedStr)
        case "starts_with", "starts_with_ci":
            return actualStr.lowercased().hasPrefix(expectedStr.lowercased())
        case "ends_with", "ends_with_ci":
            return actualStr.lowercased().hasSuffix(expectedStr.lowercased())
        case "regex":
            do {
                let regex = try NSRegularExpression(pattern: expectedStr, options: [])
                let range = NSRange(actualStr.startIndex..., in: actualStr)
                return regex.firstMatch(in: actualStr, options: [], range: range) != nil
            } catch {
                print("[AppFig] ⚠️ Invalid regex pattern: \(expectedStr)")
                return false
            }
        default:
            print("[AppFig] ⚠️ Unknown operator: \(op)")
            return false
        }
    }

    // MARK: - Private Helpers - Persistence

    private static func getCacheKey(_ company: String, _ tenant: String, _ env: String, _ suffix: String) -> String {
        return "AppFig_Cache_\(company)_\(tenant)_\(env)_\(suffix)"
    }

    private static func loadCachedRules() -> Bool {
        let defaults = UserDefaults.standard

        let rulesKey = getCacheKey(companyId, tenantId, env, "Rules")
        let hashKey = getCacheKey(companyId, tenantId, env, "Hash")
        let timestampKey = getCacheKey(companyId, tenantId, env, "Timestamp")

        guard let rulesJson = defaults.string(forKey: rulesKey),
              let hash = defaults.string(forKey: hashKey),
              let timestamp = defaults.object(forKey: timestampKey) as? Date else {
            return false
        }

        currentRulesHash = hash
        lastFetchTime = timestamp

        parseAndApplyRules(rulesJson)

        return true
    }

    private static func saveCachedRules(_ rulesJson: String, hash: String) {
        let defaults = UserDefaults.standard

        let rulesKey = getCacheKey(companyId, tenantId, env, "Rules")
        let hashKey = getCacheKey(companyId, tenantId, env, "Hash")
        let timestampKey = getCacheKey(companyId, tenantId, env, "Timestamp")

        defaults.set(rulesJson, forKey: rulesKey)
        defaults.set(hash, forKey: hashKey)
        defaults.set(Date(), forKey: timestampKey)

        currentRulesHash = hash
    }

    private static func saveCacheTimestamp() {
        let defaults = UserDefaults.standard
        let timestampKey = getCacheKey(companyId, tenantId, env, "Timestamp")
        defaults.set(Date(), forKey: timestampKey)
    }

    private static func getCachedHash() -> String? {
        let defaults = UserDefaults.standard
        let hashKey = getCacheKey(companyId, tenantId, env, "Hash")
        return defaults.string(forKey: hashKey)
    }

    private static func loadCachedEvents() {
        let defaults = UserDefaults.standard
        let eventsKey = getCacheKey(companyId, tenantId, env, "Events")

        guard let data = defaults.data(forKey: eventsKey) else { return }

        do {
            let events = try JSONDecoder().decode([EventRecord].self, from: data)
            eventHistory = events

            // Rebuild event counts
            eventCounts.removeAll()
            for event in events {
                eventCounts[event.name, default: 0] += 1
            }

        } catch {
            print("⚠️ [AppFig] Failed to load cached events: \(error)")
        }
    }

    private static func debounceSaveEvents() {
        // Cancel existing timer
        eventSaveDebounceTimer?.cancel()

        eventsSavedCount += 1

        // Save immediately if 10 events have accumulated
        if eventsSavedCount >= 10 {
            eventsSavedCount = 0
            saveCachedEvents()
            return
        }

        // Otherwise, debounce for 5 seconds
        let workItem = DispatchWorkItem {
            queue.async(flags: .barrier) {
                self.eventsSavedCount = 0
                self.saveCachedEvents()
            }
        }
        eventSaveDebounceTimer = workItem
        queue.asyncAfter(deadline: .now() + 5.0, execute: workItem)
    }

    private static func saveCachedEvents() {
        // Validate we have company/tenant/env info (required for cache keys)
        if companyId.isEmpty || tenantId.isEmpty || env.isEmpty {
            print("⚠️ [AppFig] Cannot save events: missing companyId/tenantId/env info")
            return
        }

        let defaults = UserDefaults.standard
        let eventsKey = getCacheKey(companyId, tenantId, env, "Events")

        do {
            let data = try JSONEncoder().encode(eventHistory)
            defaults.set(data, forKey: eventsKey)
        } catch {
            print("⚠️ [AppFig] Failed to save cached events: \(error)")
        }
    }

    private static func trimEventHistory() {
        // Remove events older than maxEventAgeDays
        let safeDays = min(maxEventAgeDays, 365)
        let secondsToSubtract = TimeInterval(safeDays) * 86400.0
        let cutoffTime = Date().addingTimeInterval(-secondsToSubtract)
        eventHistory.removeAll { $0.timestamp < cutoffTime }

        // Limit to maxEvents
        if eventHistory.count > maxEvents {
            let toRemove = eventHistory.count - Int(Double(maxEvents) * 0.8) // Trim to 80%
            let sorted = eventHistory.sorted { $0.timestamp < $1.timestamp }
            let removed = Array(sorted.prefix(toRemove))
            eventHistory.removeAll { event in removed.contains { $0.name == event.name && $0.timestamp == event.timestamp } }

            // Rebuild counts
            eventCounts.removeAll()
            for event in eventHistory {
                eventCounts[event.name, default: 0] += 1
            }
        }
    }
}

// MARK: - Data Models

struct EventRecord: Codable {
    let name: String
    let timestamp: Date
    let parameters: [String: String]
}

struct Rule: Codable {
    let feature: String
    let value: String
    let conditions: RuleConditions
}

struct RuleConditions: Codable {
    let events: EventsConfig
    let userProperties: [UserOrDeviceCondition]?
    let userPropertiesOperator: String?
    let device: [UserOrDeviceCondition]?
    let deviceOperator: String?

    enum CodingKeys: String, CodingKey {
        case events
        case userProperties = "user_properties"
        case userPropertiesOperator = "user_properties_operator"
        case device
        case deviceOperator = "device_operator"
    }
}

struct EventsConfig: Codable {
    let mode: String?
    let events: [EventCondition]?
    let `operator`: String?
    let ordering: String?

    enum CodingKeys: String, CodingKey {
        case mode
        case events
        case `operator` = "operator"
        case ordering = "ordering"
    }
}

struct EventCondition: Codable {
    let key: String
    let `operator`: String?
    let count: CountOperator?
    let withinLastDays: Int?
    let param: [String: OperatorValue]?
    let not: Bool

    enum CodingKeys: String, CodingKey {
        case key
        case `operator` = "operator"
        case count
        case withinLastDays = "within_last_days"
        case param
        case not
    }
}

struct CountOperator: Codable {
    let `operator`: String
    let value: Int
}

struct OperatorValue: Codable {
    let `operator`: String
    let value: AnyCodable
}

struct UserOrDeviceCondition: Codable {
    let key: String
    let value: OperatorValue
    let not: Bool
}

struct PointerData: Codable {
    let schemaVersion: String?
    let version: String
    let path: String?
    let updatedAt: String?
    let featureCount: Int?
    let ttlSecs: Int?
    let minPollIntervalSecs: Int?

    enum CodingKeys: String, CodingKey {
        case schemaVersion = "schema_version"
        case version
        case path
        case updatedAt = "updated_at"
        case featureCount = "feature_count"
        case ttlSecs = "ttl_secs"
        case minPollIntervalSecs = "min_poll_interval_secs"
    }
}

struct FeatureWrapper: Codable {
    let features: [String: [RuleSet]]
}

struct RuleSet: Codable {
    let value: String
    let conditions: RuleConditions
}

// Helper for decoding Any types
struct AnyCodable: Codable {
    let value: Any

    init(_ value: Any) {
        self.value = value
    }

    init(from decoder: Decoder) throws {
        let container = try decoder.singleValueContainer()

        if let int = try? container.decode(Int.self) {
            value = int
        } else if let double = try? container.decode(Double.self) {
            value = double
        } else if let string = try? container.decode(String.self) {
            value = string
        } else if let bool = try? container.decode(Bool.self) {
            value = bool
        } else if let array = try? container.decode([AnyCodable].self) {
            value = array.map { $0.value }
        } else if let dict = try? container.decode([String: AnyCodable].self) {
            value = dict.mapValues { $0.value }
        } else {
            throw DecodingError.dataCorruptedError(in: container, debugDescription: "Cannot decode value")
        }
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()

        switch value {
        case let int as Int:
            try container.encode(int)
        case let double as Double:
            try container.encode(double)
        case let string as String:
            try container.encode(string)
        case let bool as Bool:
            try container.encode(bool)
        case let array as [Any]:
            try container.encode(array.map { AnyCodable($0) })
        case let dict as [String: Any]:
            try container.encode(dict.mapValues { AnyCodable($0) })
        default:
            throw EncodingError.invalidValue(value, EncodingError.Context(codingPath: [], debugDescription: "Cannot encode value"))
        }
    }
}

extension AnyCodable: CustomStringConvertible {
    var description: String {
        return String(describing: value)
    }
}

// MARK: - Schema Discovery Extension
extension AppFig {

    private static func initSchemaDiscovery() {
        queue.async(flags: .barrier) {
            // Generate stable device ID
            let defaults = UserDefaults.standard
            if let storedDeviceId = defaults.string(forKey: "AppFig_DeviceId") {
                self.deviceId = storedDeviceId
            } else {
                self.deviceId = "\(Date().timeIntervalSince1970)-\(UUID().uuidString)"
                defaults.set(self.deviceId, forKey: "AppFig_DeviceId")
            }

            // Load cached schema and last upload time
            self.loadCachedSchema()
            self.lastSchemaUploadTime = defaults.double(forKey: "AppFig_LastSchemaUpload")

            // 1% deterministic sampling based on device ID hash
            let shouldSample = self.isInSample()
            if !shouldSample {
                self.log(.debug, "Schema discovery: device not in 1% sample")
            }
        }
    }

    private static func isInSample() -> Bool {
        // Simple hash of device ID for 1% sampling
        // Using overflow-safe arithmetic to prevent crashes
        var hash: Int = 5381 // DJB2 hash initial value
        for char in deviceId {
            let charValue = Int(char.unicodeScalars.first?.value ?? 0)
            // Use wrapping arithmetic to prevent overflow
            hash = ((hash &<< 5) &+ hash) &+ charValue // hash * 33 + c
        }
        // Use abs with overflow protection
        let absHash = hash == Int.min ? Int.max : abs(hash)
        return absHash % 100 < 1 // 1% sample
    }

    private static func maskPII(_ value: Any) -> String {
        let str = String(describing: value)

        // Mask emails
        if str.contains("@") && str.contains(".") {
            return "[email]"
        }

        // Truncate long strings (likely IDs or sensitive data)
        if str.count > 50 {
            return String(str.prefix(50)) + "..."
        }

        return str
    }

    private static func trackEventSchema(eventName: String, parameters: [String: String]?) {
        guard isInSample() else { return }

        queue.async(flags: .barrier) {
            // Track event name
            if !self.schemaEvents.contains(eventName) {
                self.schemaDiffNewEvents.insert(eventName)
                self.schemaEvents.insert(eventName)
            }

            // Track event parameters
            if let parameters = parameters, !parameters.isEmpty {
                if self.schemaEventParams[eventName] == nil {
                    self.schemaEventParams[eventName] = []
                }
                if self.schemaEventParamValues[eventName] == nil {
                    self.schemaEventParamValues[eventName] = [:]
                }

                var cachedParams = self.schemaEventParams[eventName]!
                var cachedParamValues = self.schemaEventParamValues[eventName]!

                if self.schemaDiffNewEventParams[eventName] == nil {
                    self.schemaDiffNewEventParams[eventName] = (params: [], paramValues: [:])
                }

                var bufferEntry = self.schemaDiffNewEventParams[eventName]!

                for (paramKey, paramValue) in parameters {
                    // Track parameter name
                    if !cachedParams.contains(paramKey) {
                        bufferEntry.params.insert(paramKey)
                        cachedParams.insert(paramKey)
                    }

                    // Track parameter value (limit to 20 unique values)
                    let maskedValue = self.maskPII(paramValue)
                    if cachedParamValues[paramKey] == nil {
                        cachedParamValues[paramKey] = []
                    }
                    var valueSet = cachedParamValues[paramKey]!

                    if !valueSet.contains(maskedValue) && valueSet.count < 20 {
                        if bufferEntry.paramValues[paramKey] == nil {
                            bufferEntry.paramValues[paramKey] = []
                        }
                        bufferEntry.paramValues[paramKey]?.insert(maskedValue)
                        valueSet.insert(maskedValue)
                    }

                    cachedParamValues[paramKey] = valueSet
                }

                self.schemaEventParams[eventName] = cachedParams
                self.schemaEventParamValues[eventName] = cachedParamValues
                self.schemaDiffNewEventParams[eventName] = bufferEntry
            }

            self.debounceSchemaUpload()
        }
    }

    private static func trackUserPropertySchema(props: [String: String]) {
        guard isInSample() else { return }

        queue.async(flags: .barrier) {
            for (key, value) in props {
                let maskedValue = self.maskPII(value)

                if self.schemaUserProperties[key] == nil {
                    self.schemaUserProperties[key] = []
                }
                var valueSet = self.schemaUserProperties[key]!

                if !valueSet.contains(maskedValue) && valueSet.count < 20 {
                    if self.schemaDiffNewUserProperties[key] == nil {
                        self.schemaDiffNewUserProperties[key] = []
                    }
                    self.schemaDiffNewUserProperties[key]?.insert(maskedValue)
                    valueSet.insert(maskedValue)
                }

                self.schemaUserProperties[key] = valueSet
            }

            self.debounceSchemaUpload()
        }
    }

    private static func trackDevicePropertySchema(props: [String: String]) {
        guard isInSample() else { return }

        queue.async(flags: .barrier) {
            for (key, value) in props {
                let maskedValue = self.maskPII(value)

                if self.schemaDeviceProperties[key] == nil {
                    self.schemaDeviceProperties[key] = []
                }
                var valueSet = self.schemaDeviceProperties[key]!

                if !valueSet.contains(maskedValue) && valueSet.count < 20 {
                    if self.schemaDiffNewDeviceProperties[key] == nil {
                        self.schemaDiffNewDeviceProperties[key] = []
                    }
                    self.schemaDiffNewDeviceProperties[key]?.insert(maskedValue)
                    valueSet.insert(maskedValue)
                }

                self.schemaDeviceProperties[key] = valueSet
            }

            self.debounceSchemaUpload()
        }
    }

    private static func debounceSchemaUpload() {
        // Skip if not in 1% sample
        guard isInSample() else { return }

        // Throttle: skip if uploaded within last 12 hours
        let now = Date().timeIntervalSince1970 * 1000
        if now - lastSchemaUploadTime < SCHEMA_UPLOAD_THROTTLE_MS {
            log(.debug, "Schema upload throttled (12-hour limit)")
            return
        }

        // Cancel existing timer
        schemaUploadTimer?.cancel()

        schemaUploadCount += 1

        // Upload immediately if batch size reached
        if schemaUploadCount >= SCHEMA_UPLOAD_BATCH_SIZE {
            schemaUploadCount = 0
            uploadSchemaDiff()
            return
        }

        // Otherwise, debounce for interval
        let workItem = DispatchWorkItem {
            queue.async(flags: .barrier) {
                self.schemaUploadCount = 0
                self.uploadSchemaDiff()
            }
        }
        schemaUploadTimer = workItem
        queue.asyncAfter(deadline: .now() + (SCHEMA_UPLOAD_INTERVAL_MS / 1000.0), execute: workItem)
    }

    private static func uploadSchemaDiff() {
        // Skip if buffer is empty
        if schemaDiffNewEvents.isEmpty &&
           schemaDiffNewEventParams.isEmpty &&
           schemaDiffNewUserProperties.isEmpty &&
           schemaDiffNewDeviceProperties.isEmpty {
            return
        }

        // Build payload
        var payload: [String: Any] = [
            "tenant_id": tenantId,
            "env": env
        ]

        if !schemaDiffNewEvents.isEmpty {
            payload["new_events"] = Array(schemaDiffNewEvents)
        }

        if !schemaDiffNewEventParams.isEmpty {
            var eventParams: [String: Any] = [:]
            for (eventName, data) in schemaDiffNewEventParams {
                eventParams[eventName] = [
                    "params": Array(data.params),
                    "param_values": data.paramValues.mapValues { Array($0) }
                ]
            }
            payload["new_event_params"] = eventParams
        }

        if !schemaDiffNewUserProperties.isEmpty {
            payload["new_user_properties"] = schemaDiffNewUserProperties.mapValues { Array($0) }
        }

        if !schemaDiffNewDeviceProperties.isEmpty {
            payload["new_device_properties"] = schemaDiffNewDeviceProperties.mapValues { Array($0) }
        }

        // Send to collectMeta endpoint
        let functionUrl = "https://us-central1-appfig-dev.cloudfunctions.net/collectMeta"
        guard let url = URL(string: functionUrl) else { return }

        var request = URLRequest(url: url)
        request.httpMethod = "POST"
        request.setValue("application/json", forHTTPHeaderField: "Content-Type")
        request.setValue(apiKey, forHTTPHeaderField: "X-API-Key")
        request.timeoutInterval = 30

        do {
            request.httpBody = try JSONSerialization.data(withJSONObject: payload)
        } catch {
            return
        }

        let task = urlSession.dataTask(with: request) { data, response, error in
            if let httpResponse = response as? HTTPURLResponse, httpResponse.statusCode == 200 {
                queue.async(flags: .barrier) {
                    self.log(.debug, "Schema uploaded successfully")

                    // Update last upload time
                    self.lastSchemaUploadTime = Date().timeIntervalSince1970 * 1000
                    let defaults = UserDefaults.standard
                    defaults.set(self.lastSchemaUploadTime, forKey: "AppFig_LastSchemaUpload")

                    // Clear buffer
                    self.schemaDiffNewEvents.removeAll()
                    self.schemaDiffNewEventParams.removeAll()
                    self.schemaDiffNewUserProperties.removeAll()
                    self.schemaDiffNewDeviceProperties.removeAll()

                    // Save updated schema cache
                    self.saveCachedSchema()
                }
            } else if let httpResponse = response as? HTTPURLResponse {
                self.log(.warn, "Schema upload failed: \(httpResponse.statusCode)")
            }
        }
        task.resume()
    }

    private static func loadCachedSchema() {
        let schemaKey = "\(companyId)_\(tenantId)_\(env)_Schema"
        let defaults = UserDefaults.standard

        if let cached = defaults.string(forKey: schemaKey),
           let data = cached.data(using: .utf8) {
            do {
                let json = try JSONSerialization.jsonObject(with: data) as? [String: Any]

                if let events = json?["events"] as? [String] {
                    schemaEvents = Set(events)
                }

                if let eventParams = json?["eventParams"] as? [[String: Any]] {
                    for ep in eventParams {
                        if let eventName = ep["eventName"] as? String,
                           let parameters = ep["parameters"] as? [String] {
                            schemaEventParams[eventName] = Set(parameters)
                        }
                    }
                }
            } catch {
            }
        }
    }

    private static func saveCachedSchema() {
        let schemaKey = "\(companyId)_\(tenantId)_\(env)_Schema"

        var data: [String: Any] = [:]
        data["events"] = Array(schemaEvents)

        var eventParamsArray: [[String: Any]] = []
        for (eventName, params) in schemaEventParams {
            eventParamsArray.append([
                "eventName": eventName,
                "parameters": Array(params)
            ])
        }
        data["eventParams"] = eventParamsArray

        do {
            let jsonData = try JSONSerialization.data(withJSONObject: data)
            if let jsonString = String(data: jsonData, encoding: .utf8) {
                UserDefaults.standard.set(jsonString, forKey: schemaKey)
            }
        } catch {
        }
    }
}
