/**
 * AppFig Android SDK - Client-side feature flags and remote configuration
 *
 * Version: 2.0.0
 * Platform: Android (Kotlin)
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
 *   AppFig.init(
 *       context = this,
 *       companyId = "acmegames",
 *       tenantId = "spaceshooter",
 *       env = "prod",
 *       apiKey = "your-api-key",
 *       autoRefresh = true,
 *       pollInterval = 43200000L  // 12 hours
 *   )
 *
 *   // Log events (persisted to disk)
 *   AppFig.logEvent("level_complete", mapOf("level" to "5"))
 *
 *   // Check features
 *   val isEnabled = AppFig.isFeatureEnabled("double_xp")
 *   val value = AppFig.getFeatureValue("max_lives")
 *
 * Local Mode Usage (Development/Testing):
 *   // Initialize without API key
 *   val rulesJson = """{"features": {"my_feature": [{"value": "enabled", "conditions": {...}}]}}"""
 *   AppFig.initLocal(context, rulesJson)
 *
 *   // Log events (persisted to disk, same as cloud mode)
 *   AppFig.logEvent("test_event")
 *
 *   // Update rules at runtime
 *   AppFig.updateLocalRules(newRulesJson)
 *
 * IMPORTANT - Local Mode Behavior:
 * - Events are persisted to disk (same as cloud mode)
 * - Event history maintained across app restarts
 * - No network calls or CDN access
 * - No companyId/tenantId required
 * - Suitable for testing and development only
 */

package com.appfig.sdk

import android.content.Context
import android.content.SharedPreferences
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.util.Log
import com.google.gson.Gson
import com.google.gson.JsonSyntaxException
import com.google.gson.annotations.SerializedName
import com.google.gson.reflect.TypeToken
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody // ByteArray.toRequestBody extension
import java.io.IOException
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.math.abs

/**
 * Main AppFig SDK class
 * Thread-safe singleton for managing feature flags and remote configuration
 *
 * MEMORY SAFETY NOTE:
 * This object stores the Application context (not Activity context) which is safe
 * in static fields. The Application context is a singleton that lives for the entire
 * app process, so there is no memory leak. The Android Lint warning about static
 * context fields is a false positive when storing Application context.
 *
 * We explicitly call context.applicationContext in init() to ensure we never
 * hold a reference to an Activity or other short-lived context.
 */
@Suppress("StaticFieldLeak")
object AppFig {
    private const val TAG = "AppFig"
    private const val PREFS_NAME = "AppFigCache"
    private const val PREFS_KEY_FIRST_OPEN = "AppFig_FirstOpen"
    private const val DEFAULT_POLL_INTERVAL_MS = 43200000L // 12 hours
    private const val DEFAULT_SESSION_TIMEOUT_MS = 1800000L // 30 minutes
    private const val DEFAULT_MAX_EVENTS = 5000
    private const val DEFAULT_MAX_EVENT_AGE_DAYS = 7

    // State management
    // NOTE: Storing Application context is safe - see class documentation above
    private var context: Context? = null
    private var prefs: SharedPreferences? = null
    private val gson = Gson()
    private val executor = Executors.newSingleThreadExecutor()
    private val mainHandler = Handler(Looper.getMainLooper())
    private val httpClient = OkHttpClient.Builder()
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .build()

    // Configuration
    private var companyId: String = ""
    private var tenantId: String = ""
    private var environment: String = ""
    private var apiKey: String = ""
    private var cdnBaseUrl: String = "https://rules-prod.appfig.com"
    private var pointerUrl: String = ""
    private var pollIntervalMs: Long = DEFAULT_POLL_INTERVAL_MS
    private var autoRefreshEnabled: Boolean = true
    private var sessionTimeoutMs: Long = DEFAULT_SESSION_TIMEOUT_MS
    private var debugMode: Boolean = false
    private var maxEvents: Int = DEFAULT_MAX_EVENTS
    private var maxEventAgeDays: Int = DEFAULT_MAX_EVENT_AGE_DAYS

    // Data storage
    private val eventHistory = mutableListOf<EventRecord>()
    private val eventCounts = mutableMapOf<String, Int>()
    private val userProperties = mutableMapOf<String, String>()
    private val deviceProperties = mutableMapOf<String, String>()
    private val featureCache = mutableMapOf<String, String?>()
    private val features = mutableMapOf<String, String?>() // Active feature values (source of truth)
    private var rules = listOf<Rule>()
    private var currentRulesHash: String = ""
    private var lastFetchTime: Long = 0
    private var isInitialized: Boolean = false
    @Volatile private var isFetchInProgress: Boolean = false

    // Event debouncing
    private var eventsSavedCount: Int = 0
    private var eventSaveDebounceRunnable: Runnable? = null

    // Session tracking
    private var sessionActive: Boolean = false
    private var currentScreen: String = ""
    private var lastActivity: Long = System.currentTimeMillis()

    // Performance optimization
    private val eventToFeaturesIndex = mutableMapOf<String, MutableSet<String>>()
    private val userPropertyToFeaturesIndex = mutableMapOf<String, MutableSet<String>>()
    private val devicePropertyToFeaturesIndex = mutableMapOf<String, MutableSet<String>>()
    private val featureToRulesIndex = mutableMapOf<String, MutableList<Rule>>()

    // Local mode
    private var useLocalMode: Boolean = false
    private var localRulesJson: String? = null

    // Schema discovery
    private val schemaEvents = mutableSetOf<String>()
    private val schemaEventParams = mutableMapOf<String, MutableSet<String>>()
    private val schemaEventParamValues = mutableMapOf<String, MutableMap<String, MutableSet<String>>>()
    private val schemaUserProperties = mutableMapOf<String, MutableSet<String>>()
    private val schemaDeviceProperties = mutableMapOf<String, MutableSet<String>>()
    private val schemaDiffNewEvents = mutableSetOf<String>()
    private val schemaDiffNewEventParams = mutableMapOf<String, Pair<MutableSet<String>, MutableMap<String, MutableSet<String>>>>()
    private val schemaDiffNewUserProperties = mutableMapOf<String, MutableSet<String>>()
    private val schemaDiffNewDeviceProperties = mutableMapOf<String, MutableSet<String>>()
    private var schemaUploadCount: Int = 0
    private var schemaUploadRunnable: Runnable? = null
    private var deviceId: String = ""
    private var lastSchemaUploadTime: Long = 0
    private const val SCHEMA_UPLOAD_BATCH_SIZE = 50

    // Callbacks and listeners
    private var onReadyCallback: (() -> Unit)? = null
    private var onRulesUpdatedCallback: (() -> Unit)? = null
    private val featureListeners = mutableMapOf<String, MutableSet<(String, String?) -> Unit>>()
    private const val SCHEMA_UPLOAD_INTERVAL_MS = 600000L // 10 minutes
    private const val SCHEMA_UPLOAD_THROTTLE_MS = 43200000L // 12 hours

    // ==================================================================================
    // LOGGING SYSTEM
    // ==================================================================================

    private enum class AppFigLogLevel {
        INFO,
        WARN,
        ERROR,
        DEBUG
    }

    private fun log(level: AppFigLogLevel, message: String) {
        // Errors and warnings always show
        if (level == AppFigLogLevel.WARN || level == AppFigLogLevel.ERROR) {
            if (level == AppFigLogLevel.ERROR)
                Log.e(TAG, message)
            else
                Log.w(TAG, message)
            return
        }

        // Info + Debug only show when debugMode=true
        if (!debugMode) return

        if (level == AppFigLogLevel.INFO)
            Log.i(TAG, message)
        else
            Log.d(TAG, message)
    }

    // ==================================================================================
    // INITIALIZATION
    // ==================================================================================

    /**
     * Initialize AppFig SDK in cloud mode (paid plans)
     *
     * @param context Application context
     * @param companyId Your Firestore company document ID (e.g., 'acmegames')
     * @param tenantId Your Firestore tenant document ID (e.g., 'spaceshooter')
     * @param env Environment: 'dev' or 'prod'
     * @param apiKey Your API key from AppFig dashboard
     * @param autoRefresh Enable automatic background rule updates (default: true)
     * @param pollInterval Auto-refresh interval in milliseconds (default: 12 hours)
     * @param debugMode Enable debug logging (default: false)
     * @param sessionTimeoutMs Session timeout in milliseconds (default: 30 minutes, range: 1 min - 2 hours)
     * @param maxEventsParam Maximum events to store (100-100000, default: 5000)
     * @param maxEventAgeDaysParam Maximum age of events in days (1-365, default: 7)
     */
    @JvmStatic
    @JvmOverloads
    fun init(
        context: Context,
        companyId: String,
        tenantId: String,
        env: String,
        apiKey: String,
        autoRefresh: Boolean = true,
        pollInterval: Long = DEFAULT_POLL_INTERVAL_MS,
        debugMode: Boolean = false,
        sessionTimeoutMs: Long = DEFAULT_SESSION_TIMEOUT_MS,
        maxEventsParam: Int = DEFAULT_MAX_EVENTS,
        maxEventAgeDaysParam: Int = DEFAULT_MAX_EVENT_AGE_DAYS,
        onReady: (() -> Unit)? = null,
        onRulesUpdated: (() -> Unit)? = null
    ) {
        val initStartTime = System.currentTimeMillis()
        log(AppFigLogLevel.INFO, "Initializing AppFig")

        // Validate inputs
        if (apiKey.isEmpty()) {
            log(AppFigLogLevel.ERROR, "API key is required for remote mode. Use initLocal() for local development.")
            return
        }

        if (companyId.isEmpty()) {
            log(AppFigLogLevel.ERROR, "Company ID is required. This should be your Firestore company document ID.")
            return
        }

        if (tenantId.isEmpty()) {
            log(AppFigLogLevel.ERROR, "Tenant ID is required. This should be your Firestore tenant document ID.")
            return
        }

        if (companyId.contains(" ")) {
            log(AppFigLogLevel.ERROR, "Invalid company ID '$companyId' - IDs cannot contain spaces. Use the Firestore document ID.")
            return
        }

        if (tenantId.contains(" ")) {
            log(AppFigLogLevel.ERROR, "Invalid tenant ID '$tenantId' - IDs cannot contain spaces. Use the Firestore document ID.")
            return
        }

        if (companyId.length > 100 || tenantId.length > 100) {
            log(AppFigLogLevel.WARN, "Unusually long ID detected. Are you using the correct Firestore document IDs?")
        }

        // Store Application context (safe in static fields - see class documentation)
        val appContext = context.applicationContext

        // Warn if someone passed non-Application context (potential misuse)
        if (context::class.java.name.contains("Activity", ignoreCase = true)) {
            log(AppFigLogLevel.WARN, "Activity context detected. Always pass Application context to avoid memory leaks.")
        }

        this.context = appContext
        this.prefs = appContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
        this.companyId = companyId
        this.tenantId = tenantId
        this.environment = env
        this.apiKey = apiKey
        this.autoRefreshEnabled = autoRefresh
        this.pollIntervalMs = pollInterval
        this.debugMode = debugMode
        this.useLocalMode = false
        this.onReadyCallback = onReady
        this.onRulesUpdatedCallback = onRulesUpdated

        // ============================================================
        // NOTE: We do NOT set isInitialized = true here anymore.
        // It will be set AFTER country detection and final evaluation
        // to prevent the app from querying features before they're ready.
        // ============================================================

        // Validate and set session timeout
        this.sessionTimeoutMs = sessionTimeoutMs
        val MIN_SESSION_TIMEOUT = 60000L // 1 minute
        val MAX_SESSION_TIMEOUT = 7200000L // 2 hours
        if (this.sessionTimeoutMs < MIN_SESSION_TIMEOUT || this.sessionTimeoutMs > MAX_SESSION_TIMEOUT) {
            log(AppFigLogLevel.WARN, "Session timeout ${this.sessionTimeoutMs}ms out of range. Clamping to $MIN_SESSION_TIMEOUT-$MAX_SESSION_TIMEOUT ms")
            this.sessionTimeoutMs = this.sessionTimeoutMs.coerceIn(MIN_SESSION_TIMEOUT, MAX_SESSION_TIMEOUT)
        }

        // Validate and set event retention
        this.maxEvents = maxEventsParam.coerceIn(100, 100000)
        this.maxEventAgeDays = maxEventAgeDaysParam.coerceIn(1, 365)

        if (maxEventsParam != this.maxEvents) {
            log(AppFigLogLevel.WARN, "maxEvents $maxEventsParam out of range. Clamped to ${this.maxEvents}")
        }

        if (maxEventAgeDaysParam != this.maxEventAgeDays) {
            log(AppFigLogLevel.WARN, "maxEventAgeDays $maxEventAgeDaysParam out of range. Clamped to ${this.maxEventAgeDays}")
        }

        if (this.maxEvents > 10000) {
            log(AppFigLogLevel.WARN, "Large event limit (${this.maxEvents}) may impact memory and storage.")
        }

        resetSession()

        // ------------------------------------------------------------
        // 1. Load ALL state BEFORE rules
        // ------------------------------------------------------------
        loadCachedEvents()

        collectDeviceProperties()

        initSchemaDiscovery()

        // Build CDN pointer URL
        pointerUrl = "$cdnBaseUrl/rules_versions/$companyId/$tenantId/$env/current/latest.json"

        // Load cached rules (DO NOT evaluate yet)
        log(AppFigLogLevel.INFO, "Rules loaded from cache")
        loadCachedRules()

        // Fetch pointer to check for updates
        log(AppFigLogLevel.INFO, "Fetching latest rules")
        fetchRulesFromCDN {
            isInitialized = true

            val totalInitTime = System.currentTimeMillis() - initStartTime
            log(AppFigLogLevel.INFO, "SDK ready")

            // Start auto-refresh after first fetch completes
            if (autoRefreshEnabled && !useLocalMode) {
                scheduleAutoRefresh()
            }
        }
    }

    /**
     * Initialize AppFig SDK in local mode (free plan)
     * Rules are loaded from a JSON string instead of CDN
     *
     * LOCAL MODE BEHAVIOR:
     * - Events are persisted to disk (same as cloud mode)
     * - Event history maintained across app restarts
     * - No companyId/tenantId required
     * - Rules loaded from provided JSON string
     * - Suitable for testing and development
     *
     * @param context Application context
     * @param rulesJson JSON string containing rules (optional)
     */
    @JvmStatic
    @JvmOverloads
    fun initLocal(
        context: Context,
        rulesJson: String? = null,
        onReady: (() -> Unit)? = null,
        onRulesUpdated: (() -> Unit)? = null
    ) {
        // Store Application context (safe in static fields - see class documentation)
        val appContext = context.applicationContext

        // Warn if someone passed non-Application context (potential misuse)
        if (context::class.java.name.contains("Activity", ignoreCase = true)) {
            log(AppFigLogLevel.WARN, "Activity context detected. Always pass Application context to avoid memory leaks.")
        }

        this.context = appContext
        this.prefs = appContext.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)

        // Set local mode configuration
        this.useLocalMode = true
        this.localRulesJson = rulesJson
        this.onReadyCallback = onReady
        this.onRulesUpdatedCallback = onRulesUpdated

        // Set sentinel values for IDs (not used in local mode)
        this.companyId = "local"
        this.tenantId = "local"
        this.environment = "local"
        this.apiKey = ""

        // Validate and set event retention
        this.maxEvents = DEFAULT_MAX_EVENTS
        this.maxEventAgeDays = DEFAULT_MAX_EVENT_AGE_DAYS

        // Initialize session and device properties
        resetSession()
        collectDeviceProperties()
        detectCountryFromCDN() // Standalone country detection for local mode
        logFirstOpenIfNeeded()
        startSessionTracking()

        // Load cached events from disk (same as cloud mode)
        loadCachedEvents()


        // Parse and apply rules if provided
        if (rulesJson != null) {
            parseAndApplyRules(rulesJson)

            // Rebuild index using current state (events, device props, user props, country)
            buildIndexes()

            // Evaluate all features with complete state
            evaluateAllFeatures()

            // Fire onReady callback after rules are applied
            mainHandler.post {
                onReadyCallback?.invoke()
            }
        } else {
            log(AppFigLogLevel.WARN, "No rules provided for local mode. Features will return null.")
        }

        isInitialized = true
    }

    // ==================================================================================
    // EVENT LOGGING
    // ==================================================================================

    /**
     * Log an event with optional parameters
     * Events are stored locally and used for rule evaluation
     *
     * @param eventName Name of the event
     * @param parameters Optional event parameters as key-value pairs
     */
    @JvmStatic
    @JvmOverloads
    fun logEvent(eventName: String, parameters: Map<String, String>? = null) {
        if (!isInitialized) {
            log(AppFigLogLevel.WARN, "AppFig not initialized. Call init() or initLocal() first.")
            return
        }

        // Track schema
        trackEventSchema(eventName, parameters)

        val event = EventRecord(eventName, System.currentTimeMillis(), parameters?.toMutableMap() ?: mutableMapOf())

        synchronized(eventHistory) {
            eventHistory.add(event)
            eventCounts[eventName] = (eventCounts[eventName] ?: 0) + 1


            // Trim old events
            trimEventHistory()
        }

        // Debounce saving events (batch save every 10 events or 5 seconds)
        debounceSaveEvents()

        // Immediately re-evaluate all features (synchronous to match iOS/Unity behavior)
        // Only evaluate if rules have been loaded (prevents premature evaluation during init)
        if (rules.isNotEmpty()) {
            evaluateAllFeatures()
        }

        updateActivity()
    }

    /**
     * Log a screen view event
     *
     * @param screenName Name of the screen
     * @param previousScreen Previous screen name (optional)
     */
    @JvmStatic
    @JvmOverloads
    fun logScreenView(screenName: String, previousScreen: String? = null) {
        val params = mutableMapOf("screen_name" to screenName)
        previousScreen?.let { params["previous_screen"] = it }
        logEvent("screen_view", params)
        currentScreen = screenName
    }

    // ==================================================================================
    // PROPERTY MANAGEMENT
    // ==================================================================================

    /**
     * Set a user property
     * User properties persist across sessions and can be used in rules
     *
     * @param key Property key
     * @param value Property value
     */
    @JvmStatic
    fun setUserProperty(key: String, value: String) {
        synchronized(userProperties) {
            userProperties[key] = value
        }

        // Track schema
        trackUserPropertySchema(mapOf(key to value))

        // Immediately re-evaluate all features (synchronous to match iOS/Unity behavior)
        // Only evaluate if rules have been loaded (prevents premature evaluation during init)
        if (rules.isNotEmpty()) {
            evaluateAllFeatures()
        }
    }

    /**
     * Remove a user property
     *
     * @param key Property key to remove
     */
    @JvmStatic
    fun removeUserProperty(key: String) {
        synchronized(userProperties) {
            userProperties.remove(key)
        }


        // Immediately re-evaluate all features (synchronous to match iOS/Unity behavior)
        // Only evaluate if rules have been loaded (prevents premature evaluation during init)
        if (rules.isNotEmpty()) {
            evaluateAllFeatures()
        }
    }

    /**
     * Set a device property
     * Device properties are typically set once during initialization
     *
     * @param key Property key
     * @param value Property value
     */
    @JvmStatic
    fun setDeviceProperty(key: String, value: String) {
        synchronized(deviceProperties) {
            deviceProperties[key] = value
        }

        // Track schema
        trackDevicePropertySchema(mapOf(key to value))

        // Immediately re-evaluate all features (synchronous to match iOS/Unity behavior)
        // Only evaluate if rules have been loaded (prevents premature evaluation during init)
        if (rules.isNotEmpty()) {
            evaluateAllFeatures()
        }
    }

    /**
     * Remove a device property
     *
     * @param key Property key to remove
     */
    @JvmStatic
    fun removeDeviceProperty(key: String) {
        synchronized(deviceProperties) {
            deviceProperties.remove(key)
        }


        // Immediately re-evaluate all features (synchronous to match iOS/Unity behavior)
        // Only evaluate if rules have been loaded (prevents premature evaluation during init)
        if (rules.isNotEmpty()) {
            evaluateAllFeatures()
        }
    }

    /**
     * Set app version
     * Convenience method to set the app version as a device property
     *
     * @param version App version string
     */
    @JvmStatic
    fun setAppVersion(version: String) {
        setDeviceProperty("app_version", version)
    }

    /**
     * Detect and set country code using static endpoint
     * @deprecated Country is now automatically detected from CDN response headers during rules fetch.
     * This fallback method is kept for backward compatibility.
     * This method makes a request to detect the user's country and automatically sets it as a device property
     *
     * @param callback Optional callback with country code (or null on failure)
     */
    @Deprecated("Country is now automatically detected from CDN response headers during rules fetch.")
    @JvmStatic
    @JvmOverloads
    fun detectAndSetCountry(callback: ((String?) -> Unit)? = null) {
        executor.execute {
            try {
                val request = Request.Builder()
                    .url("https://rules-dev.appfig.com/rules_versions/country/country/dev/current/latest.json")
                    .build()

                val client = OkHttpClient.Builder()
                    .connectTimeout(5, TimeUnit.SECONDS)
                    .readTimeout(5, TimeUnit.SECONDS)
                    .build()

                val response = client.newCall(request).execute()

                if (!response.isSuccessful) {
                    mainHandler.post { callback?.invoke(null) }
                    return@execute
                }

                val countryHeader = response.header("Country")
                if (countryHeader != null) {
                    setDeviceProperty("country", countryHeader)
                    mainHandler.post { callback?.invoke(countryHeader) }
                } else {
                    mainHandler.post { callback?.invoke(null) }
                }
            } catch (e: Exception) {
                mainHandler.post { callback?.invoke(null) }
            }
        }
    }

    // ==================================================================================
    // FEATURE FLAG CHECKS
    // ==================================================================================

    /**
     * Check if a feature is enabled
     * Returns true if the feature value is "true", "on", "enabled", or "1"
     *
     * @param feature Feature name
     * @return true if enabled, false otherwise
     */
    @JvmStatic
    fun isFeatureEnabled(feature: String): Boolean {
        val value = getFeatureValue(feature)?.lowercase() ?: return false
        return value in setOf("true", "on", "enabled", "1")
    }

    /**
     * Get the value of a feature flag
     * Returns null if no matching rule is found
     *
     * @param feature Feature name
     * @return Feature value or null
     */
    @JvmStatic
    fun getFeatureValue(feature: String): String? {
        // ==========================================
        // FIX:
        // Do NOT warn during init if rules are not yet loaded.
        // Instead, return null silently until first CDN fetch.
        // ==========================================
        if (!isInitialized) return null

        // If rules haven't loaded yet, return null silently.
        if (rules.isEmpty()) return null

        // Check if rules need refreshing (automatic updating)
        checkAndTriggerAutoRefresh()

        // Check features map first (source of truth)
        synchronized(features) {
            if (features.containsKey(feature)) {
                val value = features[feature]
                return value
            }
        }

        // Evaluate on-demand if not cached
        rules.forEach { rule ->
            if (rule.feature == feature) {
                val passed = evaluateConditions(rule.conditions)
                if (passed) {
                    val value = rule.value ?: "on"
                    synchronized(features) {
                        features[feature] = value
                    }
                    return value
                }
            }
        }

        // No matching rule found
        synchronized(features) {
            features[feature] = null
        }

        return null
    }

    private fun checkAndTriggerAutoRefresh() {
        // Only check if auto-refresh is enabled and not in local mode
        if (!autoRefreshEnabled || useLocalMode) {
            return
        }

        // Check if poll interval has elapsed
        val timeSinceLastFetch = System.currentTimeMillis() - lastFetchTime
        if (timeSinceLastFetch > pollIntervalMs) {
            log(AppFigLogLevel.INFO, "Fetching latest rules")
            // Trigger background refresh (non-blocking)
            fetchRulesFromCDN()
        }
    }

    /**
     * Check if rules have been loaded
     *
     * @return true if rules are loaded, false otherwise
     */
    @JvmStatic
    fun hasRulesLoaded(): Boolean = rules.isNotEmpty()

    /**
     * Reset a specific feature's cached value and force re-evaluation
     * Useful for implementing recurring triggers (e.g., "show popup every 3 events")
     *
     * Example usage:
     * ```
     * // Check if feature is enabled
     * if (AppFig.isFeatureEnabled("level_complete_popup")) {
     *     showPopup()
     *     // Reset the feature so it can trigger again after next 3 events
     *     AppFig.resetFeature("level_complete_popup")
     * }
     * ```
     *
     * @param featureName Name of the feature to reset
     */
    @JvmStatic
    fun resetFeature(featureName: String) {
        if (!isInitialized) {
            log(AppFigLogLevel.WARN, "Cannot reset feature: AppFig not initialized")
            return
        }

        if (rules.isEmpty()) {
            log(AppFigLogLevel.WARN, "Cannot reset feature: No rules loaded")
            return
        }

        log(AppFigLogLevel.DEBUG, "Resetting feature: $featureName")

        // Store old value for comparison
        val oldValue = synchronized(features) { features[featureName] }

        // Clear cached value
        synchronized(features) {
            features.remove(featureName)
        }

        // Clear from legacy cache if present
        synchronized(featureCache) {
            featureCache.remove(featureName)
        }

        // Re-evaluate the feature immediately
        val newValue = evaluateFeature(featureName)

        // Update features map with new evaluation
        synchronized(features) {
            features[featureName] = newValue
        }

        // Notify listeners if value changed
        if (oldValue != newValue) {
            log(AppFigLogLevel.DEBUG, "Feature value changed after reset: $featureName = $newValue")
            notifyListeners(setOf(featureName))
        } else {
            log(AppFigLogLevel.DEBUG, "Feature value unchanged after reset: $featureName = $newValue")
        }
    }

    /**
     * Reset all features and force complete re-evaluation
     * This clears all cached feature values and re-evaluates all rules
     *
     * Use this sparingly - typically you want to reset specific features using resetFeature()
     */
    @JvmStatic
    fun resetAllFeatures() {
        if (!isInitialized) {
            log(AppFigLogLevel.WARN, "Cannot reset features: AppFig not initialized")
            return
        }

        if (rules.isEmpty()) {
            log(AppFigLogLevel.WARN, "Cannot reset features: No rules loaded")
            return
        }

        log(AppFigLogLevel.DEBUG, "Resetting all features")

        // Clear all caches
        synchronized(features) {
            features.clear()
        }

        synchronized(featureCache) {
            featureCache.clear()
        }

        // Re-evaluate all features
        evaluateAllFeatures()

        log(AppFigLogLevel.DEBUG, "All features reset and re-evaluated")
    }

    // ==================================================================================
    // FEATURE LISTENERS
    // ==================================================================================

    /**
     * Add a listener for feature value changes
     * Useful for reactive frameworks (Jetpack Compose, etc.)
     *
     * @param featureName Name of the feature to listen to
     * @param listener Callback invoked when feature value changes (featureName, newValue)
     */
    @JvmStatic
    fun addListener(featureName: String, listener: (String, String?) -> Unit) {
        synchronized(featureListeners) {
            val listeners = featureListeners.getOrPut(featureName) { mutableSetOf() }
            listeners.add(listener)
        }
    }

    /**
     * Remove a feature listener
     *
     * @param featureName Name of the feature
     * @param listener Listener to remove
     */
    @JvmStatic
    fun removeListener(featureName: String, listener: (String, String?) -> Unit) {
        synchronized(featureListeners) {
            featureListeners[featureName]?.remove(listener)
            if (featureListeners[featureName]?.isEmpty() == true) {
                featureListeners.remove(featureName)
            }
        }
    }

    /**
     * Remove all listeners for a feature
     *
     * @param featureName Name of the feature
     */
    @JvmStatic
    fun removeAllListeners(featureName: String) {
        synchronized(featureListeners) {
            featureListeners.remove(featureName)
        }
    }

    /**
     * Remove all listeners for all features
     */
    @JvmStatic
    fun clearAllListeners() {
        synchronized(featureListeners) {
            featureListeners.clear()
        }
    }

    // ==================================================================================
    // SESSION MANAGEMENT
    // ==================================================================================

    /**
     * Reset the session
     * Clears session-related data but preserves events and properties
     */
    @JvmStatic
    fun resetSession() {
        sessionActive = false
        currentScreen = ""
        lastActivity = System.currentTimeMillis()
    }

    /**
     * Update activity timestamp
     * Call this when user interacts with the app to prevent session timeout
     */
    @JvmStatic
    fun updateActivity() {
        val now = System.currentTimeMillis()
        val msSinceActivity = now - lastActivity

        if (msSinceActivity > sessionTimeoutMs) {
            // Session expired, log session_end
            if (sessionActive) {
                logEvent("session_end")
                sessionActive = false
            }

            // Start new session
            logEvent("session_start")
            sessionActive = true
        }

        lastActivity = now
    }

    // ==================================================================================
    // RULES REFRESH
    // ==================================================================================

    /**
     * Enable or disable auto-refresh
     * When enabled, rules are automatically fetched at the configured poll interval
     *
     * @deprecated Use the autoRefresh parameter in init() instead
     * @param enabled true to enable auto-refresh, false to disable
     */
    @Deprecated("Use the autoRefresh parameter in init() instead", ReplaceWith("init(context, companyId, tenantId, env, apiKey, autoRefresh = enabled)"))
    @JvmStatic
    fun setAutoRefresh(enabled: Boolean) {
        autoRefreshEnabled = enabled
        if (enabled && isInitialized && !useLocalMode) {
            scheduleAutoRefresh()
        }
    }

    /**
     * Set the polling interval for auto-refresh
     *
     * @deprecated Use the pollInterval parameter in init() instead
     * @param intervalMs Interval in milliseconds (min: 60000, default: 43200000)
     */
    @Deprecated("Use the pollInterval parameter in init() instead", ReplaceWith("init(context, companyId, tenantId, env, apiKey, pollInterval = intervalMs)"))
    @JvmStatic
    fun setPollInterval(intervalMs: Long) {
        pollIntervalMs = intervalMs.coerceAtLeast(60000)
    }

    /**
     * Manually refresh rules from CDN
     * Useful for implementing pull-to-refresh or manual update buttons
     * Note: Only works in cloud mode. Use updateLocalRules() for local mode.
     */
    @JvmStatic
    fun refreshRules() {

        if (useLocalMode) {
            log(AppFigLogLevel.WARN, "Cannot refresh from CDN in local mode. Use updateLocalRules() instead.")
            return
        }

        if (!isInitialized) {
            log(AppFigLogLevel.WARN, "Cannot refresh: AppFig not initialized. Call init() first.")
            return
        }

        fetchRulesFromCDN()
    }

    /**
     * Update rules in local mode
     * Allows you to update rules at runtime without re-initializing
     *
     * @param rulesJson JSON string containing rules
     */
    @JvmStatic
    fun updateLocalRules(rulesJson: String) {
        if (!useLocalMode) {
            log(AppFigLogLevel.WARN, "updateLocalRules() is only for local mode. Use refreshRules() for cloud mode.")
            return
        }

        if (!isInitialized) {
            log(AppFigLogLevel.WARN, "Cannot update rules: AppFig not initialized. Call initLocal() first.")
            return
        }

        parseAndApplyRules(rulesJson)

        // Rebuild index and evaluate with current state
        buildIndexes()
        evaluateAllFeatures()

        localRulesJson = rulesJson
    }

    // ==================================================================================
    // CACHE MANAGEMENT
    // ==================================================================================

    /**
     * Clear all cached data for a specific company/tenant/environment
     *
     * @param companyId Company ID
     * @param tenantId Tenant ID
     * @param env Environment
     */
    @JvmStatic
    fun clearCache(companyId: String, tenantId: String, env: String) {
        prefs?.edit()?.apply {
            remove(getCacheKey(companyId, tenantId, env, "Rules"))
            remove(getCacheKey(companyId, tenantId, env, "Hash"))
            remove(getCacheKey(companyId, tenantId, env, "Timestamp"))
            apply()
        }
    }

    /**
     * Clear event history
     * Removes all logged events from memory and persistent storage
     */
    @JvmStatic
    fun clearEventHistory() {
        synchronized(eventHistory) {
            eventHistory.clear()
            eventCounts.clear()
        }
        saveCachedEvents()

        // Mark all features that depend on events as dirty
        // Immediately re-evaluate all features (synchronous to match iOS/Unity behavior)
        evaluateAllFeatures()
    }

    /**
     * Get event history statistics
     *
     * @return Map with count, oldestEvent, and newestEvent
     */
    @JvmStatic
    fun getEventHistoryStats(): Map<String, Any> {
        synchronized(eventHistory) {
            val stats = mutableMapOf<String, Any>()
            stats["count"] = eventHistory.size

            if (eventHistory.isNotEmpty()) {
                val oldest = eventHistory.minByOrNull { it.timestamp }
                val newest = eventHistory.maxByOrNull { it.timestamp }

                stats["oldestEvent"] = oldest?.let {
                    "${it.name} at ${Date(it.timestamp)}"
                } ?: "N/A"

                stats["newestEvent"] = newest?.let {
                    "${it.name} at ${Date(it.timestamp)}"
                } ?: "N/A"
            } else {
                stats["oldestEvent"] = "N/A"
                stats["newestEvent"] = "N/A"
            }

            return stats
        }
    }

    /**
     * Get count of a specific event in history
     * Useful for debugging event-based rules
     *
     * @param eventName Name of the event to count
     * @return Count of events with this name
     */
    @JvmStatic
    fun getEventCount(eventName: String): Int {
        synchronized(eventHistory) {
            return eventHistory.count { it.name == eventName }
        }
    }

    /**
     * Get all events in history
     * Useful for debugging
     *
     * @return List of all event records
     */
    @JvmStatic
    fun getAllEvents(): List<EventRecord> {
        synchronized(eventHistory) {
            return eventHistory.toList()
        }
    }

    /**
     * Print detailed event history to logcat
     * Useful for debugging event-based rules
     */
    @JvmStatic
    fun dumpEventHistory() {
        synchronized(eventHistory) {
            Log.d(TAG, "EVENT HISTORY DUMP (${eventHistory.size} events)")

            if (eventHistory.isEmpty()) {
                Log.d(TAG, "  (empty)")
            } else {
                val grouped = eventHistory.groupBy { it.name }
                grouped.forEach { (name, events) ->
                    Log.d(TAG, "  '$name': ${events.size} occurrence(s)")
                    events.forEachIndexed { index, event ->
                        val timestamp = Date(event.timestamp)
                        val params = if (event.parameters.isEmpty()) {
                            ""
                        } else {
                            " | params: ${event.parameters}"
                        }
                        Log.d(TAG, "    ${index + 1}. $timestamp$params")
                    }
                }
            }

        }
    }

    // ==================================================================================
    // PRIVATE HELPERS - DEVICE PROPERTIES
    // ==================================================================================

    private fun collectDeviceProperties() {
        context?.let { ctx ->
            setDeviceProperty("platform", "Android")
            setDeviceProperty("os_version", Build.VERSION.RELEASE)
            setDeviceProperty("device_model", Build.MODEL)
            setDeviceProperty("device_brand", Build.BRAND)
            setDeviceProperty("sdk_version", "2.0.0")

            // Get app version
            try {
                val packageInfo = ctx.packageManager.getPackageInfo(ctx.packageName, 0)
                setDeviceProperty("app_version", packageInfo.versionName ?: "unknown")
            } catch (e: Exception) {
                Log.w(TAG, "Could not get app version: ${e.message}")
            }

            // Country will be auto-detected from CDN response headers during rules fetch
            // For local mode, detectCountryFromCDN() must be called separately
        }
    }

    private fun detectCountryFromCDN() {
        executor.execute {
            try {
                val request = Request.Builder()
                    .url("https://rules-dev.appfig.com/rules_versions/country/country/dev/current/latest.json")
                    .build()

                val client = OkHttpClient.Builder()
                    .connectTimeout(5, TimeUnit.SECONDS)
                    .readTimeout(5, TimeUnit.SECONDS)
                    .build()

                val response = client.newCall(request).execute()

                if (response.isSuccessful) {
                    val countryHeader = response.header("Country")
                    if (countryHeader != null) {
                        setDeviceProperty("country", countryHeader)
                    }
                } else {
                    Log.w(TAG, "⚠️ Country detection endpoint returned HTTP ${response.code}")
                }
            } catch (e: Exception) {
                Log.w(TAG, "⚠️ Failed to fetch country from CDN: ${e.message}")
            }
        }
    }

    private fun logFirstOpenIfNeeded() {
        // Use global key (not namespaced by company/tenant) to track first open across all modes
        // This matches Unity SDK behavior
        val hasLoggedFirstOpen = prefs?.getBoolean(PREFS_KEY_FIRST_OPEN, false) ?: false
        if (!hasLoggedFirstOpen) {
            logEvent("first_open")
            prefs?.edit()?.putBoolean(PREFS_KEY_FIRST_OPEN, true)?.apply()
        }
    }

    private fun startSessionTracking() {
        logEvent("session_start")
        sessionActive = true
    }

    // ==================================================================================
    // PRIVATE HELPERS - RULES FETCHING
    // ==================================================================================

    private fun fetchRulesFromCDN(onComplete: (() -> Unit)? = null) {
        synchronized(this) {
            if (isFetchInProgress) return
            isFetchInProgress = true
            lastFetchTime = System.currentTimeMillis()
        }

        executor.execute {
            try {
                val request = Request.Builder()
                    .url(pointerUrl)
                    .header("x-api-key", apiKey)
                    .cacheControl(CacheControl.FORCE_NETWORK)
                    .build()

                val response = httpClient.newCall(request).execute()

                if (!response.isSuccessful) {
                    log(AppFigLogLevel.ERROR, "Unable to load rules")
                    isFetchInProgress = false
                    return@execute
                }

                val pointerJson = response.body?.string() ?: run {
                    log(AppFigLogLevel.ERROR, "Unable to load rules")
                    isFetchInProgress = false
                    return@execute
                }

                // Extract Country header
                val countryHeader = response.header("Country")
                if (countryHeader != null) {
                    setDeviceProperty("country", countryHeader)
                }

                val pointer = gson.fromJson(pointerJson, PointerData::class.java)

                // Enforce minimum poll interval
                pointer.minPollIntervalSecs?.let { minPollIntervalSecs ->
                    if (minPollIntervalSecs > 0) {
                        val minPollIntervalMs = minPollIntervalSecs * 1000L
                        if (pollIntervalMs < minPollIntervalMs) {
                            synchronized(this) {
                                pollIntervalMs = minPollIntervalMs
                            }
                        }
                    }
                }

                val cachedHash = getCachedHash()

                if (cachedHash == pointer.version) {
                    // Hash matches - use cached rules
                    saveCacheTimestamp()

                    // Build index and evaluate once
                    buildIndexes()
                    evaluateAllFeatures()

                    // Fire onReady callback
                    mainHandler.post {
                        onReadyCallback?.invoke()
                        onComplete?.invoke()
                    }
                    isFetchInProgress = false
                    return@execute
                }

                // Hash doesn't match - fetch immutable rules
                val immutableUrl = "$cdnBaseUrl/rules_versions/$companyId/$tenantId/$environment/current/${pointer.version}.json"
                fetchImmutableRules(immutableUrl, pointer.version, onComplete)

            } catch (e: Exception) {
                log(AppFigLogLevel.ERROR, "Unable to load rules")
                isFetchInProgress = false
                mainHandler.post { onComplete?.invoke() }
            }
        }
    }

    private fun fetchImmutableRules(immutableUrl: String, hash: String, onComplete: (() -> Unit)? = null) {
        try {
            Log.d(TAG, "📥 [IMMUTABLE] Fetching immutable rules...")
            Log.d(TAG, "   URL: $immutableUrl")

            val request = Request.Builder()
                .url(immutableUrl)
                .header("x-api-key", apiKey)
                .build()

            val response = httpClient.newCall(request).execute()

            if (!response.isSuccessful) {
                log(AppFigLogLevel.WARN, "Failed to fetch remote rules – using cached version")
                return
            }

            val rulesJson = response.body?.string() ?: run {
                log(AppFigLogLevel.WARN, "Failed to fetch remote rules – using cached version")
                return
            }

            Log.d(TAG, "✅ [IMMUTABLE] Immutable rules fetched successfully")

            // Parse rules
            parseAndApplyRules(rulesJson)

            // Build index and evaluate once
            buildIndexes()
            evaluateAllFeatures()

            // Save to cache
            saveCachedRules(rulesJson, hash)

            // Fire onRulesUpdated callback
            mainHandler.post {
                onRulesUpdatedCallback?.invoke()
                onComplete?.invoke()
            }
            isFetchInProgress = false

        } catch (e: Exception) {
            log(AppFigLogLevel.WARN, "Failed to fetch remote rules – using cached version")
            mainHandler.post { onComplete?.invoke() }
        }
    }

    private fun parseAndApplyRules(rulesJson: String) {

        try {
            // First, try to parse as v2 format with "features" wrapper
            val featureWrapper = gson.fromJson(rulesJson, FeatureWrapper::class.java)
            val newRules = mutableListOf<Rule>()

            featureWrapper.features.forEach { (featureName, ruleSets) ->
                ruleSets.forEach { ruleSet ->
                    newRules.add(Rule(featureName, ruleSet.value, ruleSet.conditions))
                }
            }

            synchronized(this) {
                rules = newRules
                featureCache.clear()
                features.clear()
                // Note: buildIndexes() will be called after all state is loaded in init()
                // or immediately for non-init calls (refreshRules, etc)
            }

        } catch (e: JsonSyntaxException) {
            // If v2 format fails, try legacy format (direct features object without wrapper)
            Log.w(TAG, "⚠️ Failed to parse as v2 format, trying legacy format...")

            try {
                // Wrap the JSON in a features key and retry
                val wrappedJson = "{\"features\": $rulesJson}"
                val featureWrapper = gson.fromJson(wrappedJson, FeatureWrapper::class.java)
                val newRules = mutableListOf<Rule>()

                featureWrapper.features.forEach { (featureName, ruleSets) ->
                    ruleSets.forEach { ruleSet ->
                        newRules.add(Rule(featureName, ruleSet.value, ruleSet.conditions))
                    }
                }

                synchronized(this) {
                    rules = newRules
                    featureCache.clear()
                    features.clear()
                    // Note: buildIndexes() will be called after all state is loaded in init()
                    // or immediately for non-init calls (refreshRules, etc)
                }

                // Notify that rules were updated
                mainHandler.post {
                    onRulesUpdatedCallback?.invoke()
                }

            } catch (e2: JsonSyntaxException) {
                log(AppFigLogLevel.ERROR, "JSON parse error")
            }
        }
    }

    private fun buildIndexes() {
        eventToFeaturesIndex.clear()
        userPropertyToFeaturesIndex.clear()
        devicePropertyToFeaturesIndex.clear()
        featureToRulesIndex.clear()

        var eventIndexCount = 0
        var userPropIndexCount = 0
        var devicePropIndexCount = 0

        rules.forEach { rule ->
            val featureName = rule.feature

            // Build feature-to-rules index for O(1) rule lookup during evaluation
            featureToRulesIndex.getOrPut(featureName) { mutableListOf() }.add(rule)

            // Index events from events array regardless of mode
            val eventsConfig = rule.conditions.events

            eventsConfig.events?.forEach { eventCond ->
                eventToFeaturesIndex.getOrPut(eventCond.key) { mutableSetOf() }.add(featureName)
                eventIndexCount++
            }

            // Index user properties
            rule.conditions.userProperties?.forEach { prop ->
                userPropertyToFeaturesIndex.getOrPut(prop.key) { mutableSetOf() }.add(featureName)
                userPropIndexCount++
            }

            // Index device properties
            rule.conditions.device?.forEach { prop ->
                devicePropertyToFeaturesIndex.getOrPut(prop.key) { mutableSetOf() }.add(featureName)
                devicePropIndexCount++
            }
        }


        currentRulesHash = computeRulesHash()
        saveIndexToPrefs()
    }

    private fun scheduleAutoRefresh() {
        mainHandler.postDelayed({
            if (autoRefreshEnabled && !useLocalMode) {
                refreshRules()
                scheduleAutoRefresh() // Reschedule
            }
        }, pollIntervalMs)
    }

    /**
     * Evaluate ALL features completely (Unity SDK approach)
     * No dirty feature tracking - always evaluate everything
     * Simpler, more reliable, and matches Unity SDK behavior
     */
    private fun evaluateAllFeatures() {

        if (rules.isEmpty()) {
            return
        }


        val changedFeatures = mutableSetOf<String>()

        // Evaluate each feature using the index for O(1) rule lookup
        featureToRulesIndex.forEach { (featureName, featureRules) ->
            val oldValue = synchronized(features) { features[featureName] }
            var newValue: String? = null

            // Find first matching rule for this feature (no linear search needed!)
            for (rule in featureRules) {
                if (evaluateConditions(rule.conditions)) {
                    newValue = rule.value ?: "on"
                    break
                }
            }

            // Set feature value (null if no match)
            synchronized(features) {
                features[featureName] = newValue
            }

            if (oldValue != newValue) {
                log(AppFigLogLevel.DEBUG, "Feature updated: $featureName = $newValue")
                changedFeatures.add(featureName)
            }
        }

        // Remove orphaned features not in current rules
        synchronized(features) {
            val toRemove = features.keys.filter { !featureToRulesIndex.containsKey(it) }
            toRemove.forEach { key ->
                features.remove(key)
                changedFeatures.add(key)
            }
        }

        // Notify listeners of changed features
        if (changedFeatures.isNotEmpty()) {
            notifyListeners(changedFeatures)
        }
    }

    private fun notifyListeners(changedFeatures: Set<String>) {
        // Notify on main thread
        mainHandler.post {
            synchronized(featureListeners) {
                changedFeatures.forEach { featureName ->
                    featureListeners[featureName]?.forEach { listener ->
                        try {
                            val value = synchronized(features) { features[featureName] }
                            listener(featureName, value)
                        } catch (e: Exception) {
                            log(AppFigLogLevel.ERROR, "Conditions could not be evaluated")
                        }
                    }
                }
            }
        }
    }

    private fun computeRulesHash(): String {
        val rulesJson = gson.toJson(rules)
        return rulesJson.hashCode().toString()
    }

    private fun saveIndexToPrefs() {
        try {
            prefs?.edit()?.apply {
                putString("AppFig_IndexHash", currentRulesHash)
                putString("AppFig_EventIndex", serializeIndex(eventToFeaturesIndex))
                putString("AppFig_UserPropIndex", serializeIndex(userPropertyToFeaturesIndex))
                putString("AppFig_DeviceIndex", serializeIndex(devicePropertyToFeaturesIndex))
                apply()
            }
        } catch (e: Exception) {
            Log.w(TAG, "Failed to save index to SharedPreferences: ${e.message}")
        }
    }

    private fun loadIndexFromPrefs() {
        try {
            val savedHash = prefs?.getString("AppFig_IndexHash", null)
            if (savedHash == null || savedHash != currentRulesHash) {
                return
            }

            val eventIndexJson = prefs?.getString("AppFig_EventIndex", null) ?: ""
            val userPropIndexJson = prefs?.getString("AppFig_UserPropIndex", null) ?: ""
            val deviceIndexJson = prefs?.getString("AppFig_DeviceIndex", null) ?: ""

            eventToFeaturesIndex.clear()
            eventToFeaturesIndex.putAll(deserializeIndex(eventIndexJson))

            userPropertyToFeaturesIndex.clear()
            userPropertyToFeaturesIndex.putAll(deserializeIndex(userPropIndexJson))

            devicePropertyToFeaturesIndex.clear()
            devicePropertyToFeaturesIndex.putAll(deserializeIndex(deviceIndexJson))

        } catch (e: Exception) {
            Log.w(TAG, "Failed to load index from SharedPreferences: ${e.message}")
            eventToFeaturesIndex.clear()
            userPropertyToFeaturesIndex.clear()
            devicePropertyToFeaturesIndex.clear()
        }
    }

    private fun serializeIndex(index: Map<String, MutableSet<String>>): String {
        return index.entries.joinToString("|") { (key, features) ->
            "$key:${features.joinToString(",")}"
        }
    }

    private fun deserializeIndex(serialized: String): MutableMap<String, MutableSet<String>> {
        val result = mutableMapOf<String, MutableSet<String>>()
        if (serialized.isEmpty()) return result

        serialized.split("|").forEach { pair ->
            val parts = pair.split(":")
            if (parts.size == 2) {
                val key = parts[0]
                val features = parts[1].split(",").toMutableSet()
                result[key] = features
            }
        }
        return result
    }

    // ==================================================================================
    // PRIVATE HELPERS - RULE EVALUATION
    // ==================================================================================

    private fun evaluateFeature(featureName: String): String? {
        val matchingRules = rules.filter { it.feature == featureName }

        if (matchingRules.isEmpty()) {
            return null
        }

        for ((index, rule) in matchingRules.withIndex()) {
            if (evaluateConditions(rule.conditions)) {
                return rule.value
            }
        }

        return null
    }

    private fun evaluateConditions(conditions: RuleConditions): Boolean {

        val eventsPassed = evaluateEvents(conditions.events)
        Log.d(TAG, "      📅 Events check: ${if (eventsPassed) "✅ PASS" else "❌ FAIL"}")

        val userPropsPassed = evaluateProperties(
            conditions.userProperties ?: emptyList(),
            conditions.userPropertiesOperator ?: "AND",
            userProperties
        )
        Log.d(TAG, "      👤 User props check: ${if (userPropsPassed) "✅ PASS" else "❌ FAIL"}")

        val devicePassed = evaluateProperties(
            conditions.device ?: emptyList(),
            conditions.deviceOperator ?: "AND",
            deviceProperties
        )
        Log.d(TAG, "      📱 Device props check: ${if (devicePassed) "✅ PASS" else "❌ FAIL"}")
        if (!devicePassed && conditions.device?.isNotEmpty() == true) {
            Log.d(TAG, "         Device conditions: ${conditions.device}")
            Log.d(TAG, "         Available device props: ${deviceProperties}")
        }

        val result = eventsPassed && userPropsPassed && devicePassed

        return result
    }

    private fun evaluateEvents(eventsConfig: EventsConfig): Boolean {
        val events = eventsConfig.events ?: return true
        if (events.isEmpty()) return true

        val mode = eventsConfig.mode ?: "simple"

        return when (mode) {
            "simple" -> {
                val operator = eventsConfig.operator ?: "AND"
                evaluateSimpleEvents(events, operator)
            }
            "sequence" -> {
                val ordering = eventsConfig.ordering ?: "direct"
                evaluateSequenceEvents(events, ordering)
            }
            else -> {
                false
            }
        }
    }

    private fun evaluateSimpleEvents(conditions: List<EventCondition>, operator: String): Boolean {

        return when (operator) {
            "OR" -> {
                conditions.any { condition ->
                    val result = evaluateEventCondition(condition)
                    val finalResult = if (condition.not) !result else result
                    if (finalResult) {
                    }
                    finalResult
                }
            }
            "AND" -> {
                conditions.all { condition ->
                    val result = evaluateEventCondition(condition)
                    val finalResult = if (condition.not) !result else result
                    if (!finalResult) {
                    }
                    finalResult
                }
            }
            else -> {
                Log.w(TAG, "Unknown operator: $operator")
                false
            }
        }
    }

    private fun evaluateEventCondition(condition: EventCondition): Boolean {
        if (condition.key.isEmpty()) {
            Log.w(TAG, "Event condition key is empty")
            return false
        }

        // Use operator for event name matching (default to "==" if not specified)
        val eventOperator = condition.operator ?: "=="

        synchronized(eventHistory) {
            var matchingEvents = eventHistory.filter { compareValue(it.name, eventOperator, condition.key) }


            // Apply time window filter if specified
            if (condition.withinLastDays != null && condition.withinLastDays > 0) {
                val cutoffTime = System.currentTimeMillis() - (condition.withinLastDays * 24 * 60 * 60 * 1000L)
                matchingEvents = matchingEvents.filter { it.timestamp >= cutoffTime }
            }

            // Check count operator if specified
            if (condition.count != null) {
                val actualCount = matchingEvents.size
                val countPassed = compareCount(actualCount, condition.count.operator, condition.count.value)
                if (!countPassed) {
                    return false
                }
            }

            // Check parameter conditions if specified
            if (condition.param != null && condition.param.isNotEmpty()) {
                val hasMatchingEvent = matchingEvents.any { event ->
                    condition.param.all { (paramKey, opValue) ->
                        val eventValue = event.parameters[paramKey]
                        eventValue != null && compareValue(eventValue, opValue.operator, opValue.value.toString())
                    }
                }
                if (!hasMatchingEvent) {
                    return false
                }
            }

            // If we have a count condition, it must have passed
            // If we have param conditions, they must have passed
            // Otherwise, we just need at least one matching event
            return matchingEvents.isNotEmpty()
        }
    }

    private fun evaluateSequenceEvents(steps: List<EventCondition>, ordering: String): Boolean {

        synchronized(eventHistory) {
            if (ordering == "direct") {
                return evaluateDirectSequence(steps)
            } else {
                return evaluateIndirectSequence(steps)
            }
        }
    }

    private fun evaluateDirectSequence(steps: List<EventCondition>): Boolean {
        eventHistory.forEachIndexed { index, event ->
        }
        steps.forEachIndexed { index, step ->
            val countStr = step.count?.let { "${it.operator} ${it.value}" } ?: "any"
            val paramStr = step.param?.let { "${it.size} params" } ?: "no params"
        }

        // Calculate minimum events needed
        var minEventsNeeded = 0
        steps.forEach { step ->
            val stepMinCount = when (step.count?.operator) {
                "==" -> step.count.value
                ">=" -> step.count.value
                ">" -> step.count.value + 1
                "<=", "<" -> 1
                else -> 1
            }
            minEventsNeeded += stepMinCount
        }

        // Try to find a consecutive run of events matching all steps
        for (startIdx in 0..(eventHistory.size - minEventsNeeded)) {
            var sequenceMatched = true
            var eventIdx = startIdx


            for (stepIdx in steps.indices) {
                val step = steps[stepIdx]
                var matchedCount = 0

                // Determine consumption strategy based on count operator
                val countOperator = step.count?.operator ?: ">="
                val countValue = step.count?.value ?: 1
                val maxEventsToConsume = when (countOperator) {
                    "==" -> countValue
                    "<=" -> countValue
                    "<" -> countValue - 1
                    else -> Int.MAX_VALUE // >=, > operators are greedy
                }


                // Try to match consecutive events for this step
                while (eventIdx < eventHistory.size) {
                    val evt = eventHistory[eventIdx]
                    val stepOperator = step.operator ?: "=="

                    if (compareValue(evt.name, stepOperator, step.key)) {
                        // Check time window first
                        var withinTimeWindow = true
                        if (step.withinLastDays != null && step.withinLastDays > 0) {
                            val cutoffTime = System.currentTimeMillis() - (step.withinLastDays * 24 * 60 * 60 * 1000L)
                            if (evt.timestamp < cutoffTime) {
                                withinTimeWindow = false
                                Log.d(TAG, "    Event at index $eventIdx outside time window")
                            }
                        }

                        // Check parameters
                        val stepMatch = checkStepMatch(evt, step)

                        if (withinTimeWindow && stepMatch) {
                            matchedCount++
                            eventIdx++

                            // Check if we've consumed enough events
                            if (matchedCount >= maxEventsToConsume) {
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
                val countSatisfied = compareCount(matchedCount, countOperator, countValue)
                if (!countSatisfied) {
                    sequenceMatched = false
                    break
                }

            }

            if (sequenceMatched) {
                return true
            } else {
            }
        }

        return false
    }

    private fun evaluateIndirectSequence(steps: List<EventCondition>): Boolean {

        var lastMatchedIndex = -1

        for (stepIndex in steps.indices) {
            val step = steps[stepIndex]

            // Check if this step has a count condition
            if (step.count != null) {
                val op = step.count.operator
                val threshold = step.count.value

                var matchedCount = 0
                var latestMatchIndex = lastMatchedIndex

                for (i in (lastMatchedIndex + 1) until eventHistory.size) {
                    val evt = eventHistory[i]
                    val stepOperator = step.operator ?: "=="

                    if (compareValue(evt.name, stepOperator, step.key) && checkStepMatch(evt, step)) {
                        // Check time window
                        if (step.withinLastDays != null && step.withinLastDays > 0) {
                            val cutoffTime = System.currentTimeMillis() - (step.withinLastDays * 24 * 60 * 60 * 1000L)
                            if (evt.timestamp < cutoffTime) {
                                continue
                            }
                        }
                        matchedCount++
                        latestMatchIndex = i
                    }
                }

                val countMatched = compareCount(matchedCount, op, threshold)

                if (!countMatched) {
                    return false
                }

                lastMatchedIndex = latestMatchIndex
            } else {
                // No count condition, just check if at least one matching event exists
                var stepMatched = false

                for (i in (lastMatchedIndex + 1) until eventHistory.size) {
                    val evt = eventHistory[i]
                    val stepOperator = step.operator ?: "=="

                    if (compareValue(evt.name, stepOperator, step.key) && checkStepMatch(evt, step)) {
                        // Check time window
                        if (step.withinLastDays != null && step.withinLastDays > 0) {
                            val cutoffTime = System.currentTimeMillis() - (step.withinLastDays * 24 * 60 * 60 * 1000L)
                            if (evt.timestamp < cutoffTime) {
                                continue
                            }
                        }

                        stepMatched = true
                        lastMatchedIndex = i
                        break
                    }
                }

                if (!stepMatched) {
                    return false
                }
            }
        }

        return true
    }

    private fun checkStepMatch(event: EventRecord, step: EventCondition): Boolean {
        if (step.param == null || step.param.isEmpty()) {
            return true
        }

        return step.param.all { (paramKey, opValue) ->
            val actualValue = event.parameters[paramKey]
            actualValue != null && compareValue(actualValue, opValue.operator, opValue.value.toString())
        }
    }

    private fun compareCount(actual: Int, operator: String, expected: Int): Boolean {
        return when (operator) {
            "==" -> actual == expected
            "!=" -> actual != expected
            ">" -> actual > expected
            "<" -> actual < expected
            ">=" -> actual >= expected
            "<=" -> actual <= expected
            else -> false
        }
    }

    private fun evaluateProperties(
        properties: List<UserOrDeviceCondition>,
        operator: String,
        propertyMap: Map<String, String>
    ): Boolean {
        if (properties.isEmpty()) return true

        return when (operator) {
            "AND" -> properties.all { evaluateProperty(it, propertyMap) }
            "OR" -> properties.any { evaluateProperty(it, propertyMap) }
            else -> false
        }
    }

    private fun evaluateProperty(condition: UserOrDeviceCondition, propertyMap: Map<String, String>): Boolean {
        val actualValue = propertyMap[condition.key]
        val expectedValue = condition.value.value
        val operator = condition.value.operator

        val result = if (actualValue != null) {
            compareValue(actualValue, operator, expectedValue)
        } else {
            Log.d(TAG, "         ⚠️ Property '${condition.key}' not found in property map")
            false
        }

        val finalResult = if (condition.not) !result else result
        Log.d(TAG, "         ${if (finalResult) "✅" else "❌"} '${condition.key}' ${operator} '${expectedValue}' | actual: '${actualValue ?: "null"}' | result: $finalResult")

        return finalResult
    }

    private fun compareValue(actual: Any, operator: String, expected: Any): Boolean {
        return try {
            val actualStr = actual.toString()
            val expectedStr = expected.toString()

            when (operator) {
                "==" -> actualStr == expectedStr
                "!=" -> actualStr != expectedStr
                "==_ci" -> actualStr.equals(expectedStr, ignoreCase = true)
                "!=_ci" -> !actualStr.equals(expectedStr, ignoreCase = true)
                ">" -> {
                    val a = actualStr.toDoubleOrNull()
                    val e = expectedStr.toDoubleOrNull()
                    if (a != null && e != null) {
                        a > e
                    } else {
                        actualStr.compareTo(expectedStr) > 0
                    }
                }
                "<" -> {
                    val a = actualStr.toDoubleOrNull()
                    val e = expectedStr.toDoubleOrNull()
                    if (a != null && e != null) {
                        a < e
                    } else {
                        actualStr.compareTo(expectedStr) < 0
                    }
                }
                ">=" -> {
                    val a = actualStr.toDoubleOrNull()
                    val e = expectedStr.toDoubleOrNull()
                    if (a != null && e != null) {
                        a >= e
                    } else {
                        actualStr.compareTo(expectedStr) >= 0
                    }
                }
                "<=" -> {
                    val a = actualStr.toDoubleOrNull()
                    val e = expectedStr.toDoubleOrNull()
                    if (a != null && e != null) {
                        a <= e
                    } else {
                        actualStr.compareTo(expectedStr) <= 0
                    }
                }
                "in" -> {
                    val expectedList = when (expected) {
                        is List<*> -> expected.map { it.toString().lowercase() }
                        else -> expectedStr.split(",").map { it.trim().lowercase() }
                    }
                    actualStr.lowercase() in expectedList
                }
                "not_in" -> {
                    val expectedList = when (expected) {
                        is List<*> -> expected.map { it.toString().lowercase() }
                        else -> expectedStr.split(",").map { it.trim().lowercase() }
                    }
                    actualStr.lowercase() !in expectedList
                }
                "contains" -> actualStr.contains(expectedStr, ignoreCase = true)
                "contains_ci" -> actualStr.contains(expectedStr, ignoreCase = true)
                "starts_with" -> actualStr.startsWith(expectedStr, ignoreCase = true)
                "starts_with_ci" -> actualStr.startsWith(expectedStr, ignoreCase = true)
                "ends_with" -> actualStr.endsWith(expectedStr, ignoreCase = true)
                "ends_with_ci" -> actualStr.endsWith(expectedStr, ignoreCase = true)
                "regex" -> {
                    try {
                        Regex(expectedStr).matches(actualStr)
                    } catch (e: Exception) {
                        Log.w(TAG, "Invalid regex pattern: $expectedStr")
                        false
                    }
                }
                else -> {
                    Log.w(TAG, "Unknown operator: $operator")
                    false
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "Error comparing values: ${e.message}")
            false
        }
    }

    // ==================================================================================
    // PRIVATE HELPERS - PERSISTENCE
    // ==================================================================================

    private fun getCacheKey(company: String, tenant: String, env: String, suffix: String): String {
        return "AppFig_Cache_${company}_${tenant}_${env}_$suffix"
    }

    private fun loadCachedRules(): Boolean {
        try {
            val rulesKey = getCacheKey(companyId, tenantId, environment, "Rules")
            val hashKey = getCacheKey(companyId, tenantId, environment, "Hash")
            val timestampKey = getCacheKey(companyId, tenantId, environment, "Timestamp")

            val rulesJson = prefs?.getString(rulesKey, null) ?: return false
            val hash = prefs?.getString(hashKey, null) ?: return false
            val timestamp = prefs?.getLong(timestampKey, 0) ?: 0

            currentRulesHash = hash
            lastFetchTime = timestamp

            parseAndApplyRules(rulesJson)

            return true

        } catch (e: Exception) {
            Log.w(TAG, "Failed to load cached rules: ${e.message}")
            return false
        }
    }

    private fun saveCachedRules(rulesJson: String, hash: String) {
        try {
            val rulesKey = getCacheKey(companyId, tenantId, environment, "Rules")
            val hashKey = getCacheKey(companyId, tenantId, environment, "Hash")
            val timestampKey = getCacheKey(companyId, tenantId, environment, "Timestamp")

            prefs?.edit()?.apply {
                putString(rulesKey, rulesJson)
                putString(hashKey, hash)
                putLong(timestampKey, System.currentTimeMillis())
                apply()
            }

            currentRulesHash = hash

        } catch (e: Exception) {
            Log.w(TAG, "Failed to save cached rules: ${e.message}")
        }
    }

    private fun saveCacheTimestamp() {
        try {
            val timestampKey = getCacheKey(companyId, tenantId, environment, "Timestamp")
            prefs?.edit()?.putLong(timestampKey, System.currentTimeMillis())?.apply()
        } catch (e: Exception) {
            Log.w(TAG, "Failed to save cache timestamp: ${e.message}")
        }
    }

    private fun getCachedHash(): String? {
        val hashKey = getCacheKey(companyId, tenantId, environment, "Hash")
        return prefs?.getString(hashKey, null)
    }

    private fun loadCachedEvents() {
        try {
            val eventsKey = getCacheKey(companyId, tenantId, environment, "Events")
            val eventsJson = prefs?.getString(eventsKey, null) ?: return

            val type = object : TypeToken<List<EventRecord>>() {}.type
            val events: List<EventRecord> = gson.fromJson(eventsJson, type)

            synchronized(eventHistory) {
                eventHistory.clear()
                eventHistory.addAll(events)

                // Rebuild event counts
                eventCounts.clear()
                events.forEach { event ->
                    eventCounts[event.name] = (eventCounts[event.name] ?: 0) + 1
                }
            }


        } catch (e: Exception) {
            Log.w(TAG, "Failed to load cached events: ${e.message}")
        }
    }

    private fun debounceSaveEvents() {
        // Cancel existing runnable
        eventSaveDebounceRunnable?.let { mainHandler.removeCallbacks(it) }

        synchronized(this) {
            eventsSavedCount++

            // Save immediately if 10 events have accumulated
            if (eventsSavedCount >= 10) {
                eventsSavedCount = 0
                saveCachedEvents()
                return
            }
        }

        // Otherwise, debounce for 5 seconds
        val runnable = Runnable {
            synchronized(this) {
                eventsSavedCount = 0
            }
            saveCachedEvents()
        }
        eventSaveDebounceRunnable = runnable
        mainHandler.postDelayed(runnable, 5000)
    }

    private fun saveCachedEvents() {
        // Validate we have company/tenant/env info (required for cache keys)
        if (companyId.isEmpty() || tenantId.isEmpty() || environment.isEmpty()) {
            Log.w(TAG, "Cannot save events: missing companyId/tenantId/environment info")
            return
        }

        try {
            val eventsKey = getCacheKey(companyId, tenantId, environment, "Events")

            synchronized(eventHistory) {
                val eventsJson = gson.toJson(eventHistory)
                prefs?.edit()?.putString(eventsKey, eventsJson)?.apply()
            }


        } catch (e: Exception) {
            Log.w(TAG, "Failed to save cached events: ${e.message}")
        }
    }

    private fun trimEventHistory() {
        synchronized(eventHistory) {
            // Remove events older than maxEventAgeDays
            val cutoffTime = System.currentTimeMillis() - (maxEventAgeDays * 24 * 60 * 60 * 1000L)
            eventHistory.removeAll { it.timestamp < cutoffTime }

            // Limit to maxEvents
            if (eventHistory.size > maxEvents) {
                val toRemove = eventHistory.size - (maxEvents * 0.8).toInt() // Trim to 80%
                val removed = eventHistory.sortedBy { it.timestamp }.take(toRemove)
                eventHistory.removeAll(removed.toSet())

                // Rebuild counts
                eventCounts.clear()
                eventHistory.forEach { event ->
                    eventCounts[event.name] = (eventCounts[event.name] ?: 0) + 1
                }
            }
        }
    }

    // ==================================================================================
    // DATA CLASSES
    // ==================================================================================

    data class EventRecord(
        val name: String,
        val timestamp: Long,
        val parameters: MutableMap<String, String> = mutableMapOf()
    )

    data class Rule(
        val feature: String,
        val value: String,
        val conditions: RuleConditions
    )

    data class RuleConditions(
        val events: EventsConfig = EventsConfig(),
        @SerializedName("user_properties") val userProperties: List<UserOrDeviceCondition>? = null,
        @SerializedName("user_properties_operator") val userPropertiesOperator: String? = "AND",
        val device: List<UserOrDeviceCondition>? = null,
        @SerializedName("device_operator") val deviceOperator: String? = "AND"
    )

    data class EventsConfig(
        val mode: String? = "simple",
        val events: List<EventCondition>? = null,
        @SerializedName("operator") val operator: String? = null,
        @SerializedName("ordering") val ordering: String? = null
    )

    data class EventCondition(
        val key: String,
        val operator: String? = null,
        val count: CountOperator? = null,
        @SerializedName("within_last_days") val withinLastDays: Int? = null,
        val param: Map<String, OperatorValue>? = null,
        val not: Boolean = false
    )

    data class CountOperator(
        val operator: String,
        val value: Int
    )

    data class OperatorValue(
        val operator: String,
        val value: Any
    )

    data class UserOrDeviceCondition(
        val key: String,
        val value: OperatorValue,
        val not: Boolean = false
    )

    private data class PointerData(
        @SerializedName("schema_version") val schemaVersion: String? = null,
        val version: String,
        val path: String? = null,
        @SerializedName("updated_at") val updatedAt: String? = null,
        @SerializedName("feature_count") val featureCount: Int? = null,
        @SerializedName("ttl_secs") val ttlSecs: Int? = null,
        @SerializedName("min_poll_interval_secs") val minPollIntervalSecs: Int? = null
    )

    private data class FeatureWrapper(
        val features: Map<String, List<RuleSet>>
    )

    private data class RuleSet(
        val value: String,
        val conditions: RuleConditions
    )

    // ==================================================================================
    // SCHEMA DISCOVERY
    // ==================================================================================

    private fun initSchemaDiscovery() {
        executor.execute {
            // Generate stable device ID
            val storedDeviceId = prefs?.getString("AppFig_DeviceId", null)
            if (storedDeviceId != null) {
                deviceId = storedDeviceId
            } else {
                deviceId = "${System.currentTimeMillis()}-${(100000..999999).random()}"
                prefs?.edit()?.putString("AppFig_DeviceId", deviceId)?.apply()
            }

            // Load cached schema and last upload time
            loadCachedSchema()
            lastSchemaUploadTime = prefs?.getLong("AppFig_LastSchemaUpload", 0) ?: 0

            // 1% deterministic sampling based on device ID hash
            val shouldSample = isInSample()
            if (!shouldSample) {
                log(AppFigLogLevel.DEBUG, "Schema discovery: device not in 1% sample")
            }
        }
    }

    private fun isInSample(): Boolean {
        // Simple hash of device ID for 1% sampling
        var hash = 0
        for (char in deviceId) {
            hash = ((hash shl 5) - hash) + char.code
        }
        return abs(hash) % 100 < 1 // 1% sample
    }

    private fun maskPII(value: Any): String {
        val str = value.toString()

        // Mask emails
        if (str.contains("@") && str.contains(".")) {
            return "[email]"
        }

        // Truncate long strings (likely IDs or sensitive data)
        if (str.length > 50) {
            return str.substring(0, 50) + "..."
        }

        return str
    }

    private fun trackEventSchema(eventName: String, parameters: Map<String, String>?) {
        if (!isInSample()) return

        executor.execute {
            synchronized(schemaEvents) {
                // Track event name
                if (!schemaEvents.contains(eventName)) {
                    schemaDiffNewEvents.add(eventName)
                    schemaEvents.add(eventName)
                }

                // Track event parameters
                if (parameters != null && parameters.isNotEmpty()) {
                    if (!schemaEventParams.containsKey(eventName)) {
                        schemaEventParams[eventName] = mutableSetOf()
                    }
                    if (!schemaEventParamValues.containsKey(eventName)) {
                        schemaEventParamValues[eventName] = mutableMapOf()
                    }

                    val cachedParams = schemaEventParams[eventName]!!
                    val cachedParamValues = schemaEventParamValues[eventName]!!

                    if (!schemaDiffNewEventParams.containsKey(eventName)) {
                        schemaDiffNewEventParams[eventName] = Pair(mutableSetOf(), mutableMapOf())
                    }

                    val bufferEntry = schemaDiffNewEventParams[eventName]!!

                    for ((paramKey, paramValue) in parameters) {
                        // Track parameter name
                        if (!cachedParams.contains(paramKey)) {
                            bufferEntry.first.add(paramKey)
                            cachedParams.add(paramKey)
                        }

                        // Track parameter value (limit to 20 unique values)
                        val maskedValue = maskPII(paramValue)
                        if (!cachedParamValues.containsKey(paramKey)) {
                            cachedParamValues[paramKey] = mutableSetOf()
                        }
                        val valueSet = cachedParamValues[paramKey]!!

                        if (!valueSet.contains(maskedValue) && valueSet.size < 20) {
                            if (!bufferEntry.second.containsKey(paramKey)) {
                                bufferEntry.second[paramKey] = mutableSetOf()
                            }
                            bufferEntry.second[paramKey]?.add(maskedValue)
                            valueSet.add(maskedValue)
                        }
                    }
                }

                debounceSchemaUpload()
            }
        }
    }

    private fun trackUserPropertySchema(props: Map<String, String>) {
        if (!isInSample()) return

        executor.execute {
            synchronized(schemaUserProperties) {
                for ((key, value) in props) {
                    val maskedValue = maskPII(value)

                    if (!schemaUserProperties.containsKey(key)) {
                        schemaUserProperties[key] = mutableSetOf()
                    }
                    val valueSet = schemaUserProperties[key]!!

                    if (!valueSet.contains(maskedValue) && valueSet.size < 20) {
                        if (!schemaDiffNewUserProperties.containsKey(key)) {
                            schemaDiffNewUserProperties[key] = mutableSetOf()
                        }
                        schemaDiffNewUserProperties[key]?.add(maskedValue)
                        valueSet.add(maskedValue)
                    }
                }

                debounceSchemaUpload()
            }
        }
    }

    private fun trackDevicePropertySchema(props: Map<String, String>) {
        if (!isInSample()) return

        executor.execute {
            synchronized(schemaDeviceProperties) {
                for ((key, value) in props) {
                    val maskedValue = maskPII(value)

                    if (!schemaDeviceProperties.containsKey(key)) {
                        schemaDeviceProperties[key] = mutableSetOf()
                    }
                    val valueSet = schemaDeviceProperties[key]!!

                    if (!valueSet.contains(maskedValue) && valueSet.size < 20) {
                        if (!schemaDiffNewDeviceProperties.containsKey(key)) {
                            schemaDiffNewDeviceProperties[key] = mutableSetOf()
                        }
                        schemaDiffNewDeviceProperties[key]?.add(maskedValue)
                        valueSet.add(maskedValue)
                    }
                }

                debounceSchemaUpload()
            }
        }
    }

    private fun debounceSchemaUpload() {
        // Skip if not in 1% sample
        if (!isInSample()) return

        // Throttle: skip if uploaded within last 12 hours
        val now = System.currentTimeMillis()
        if (now - lastSchemaUploadTime < SCHEMA_UPLOAD_THROTTLE_MS) {
            log(AppFigLogLevel.DEBUG, "Schema upload throttled (12-hour limit)")
            return
        }

        // Cancel existing runnable
        schemaUploadRunnable?.let { mainHandler.removeCallbacks(it) }

        schemaUploadCount++

        // Upload immediately if batch size reached
        if (schemaUploadCount >= SCHEMA_UPLOAD_BATCH_SIZE) {
            schemaUploadCount = 0
            uploadSchemaDiff()
            return
        }

        // Otherwise, debounce for interval
        val runnable = Runnable {
            schemaUploadCount = 0
            uploadSchemaDiff()
        }
        schemaUploadRunnable = runnable
        mainHandler.postDelayed(runnable, SCHEMA_UPLOAD_INTERVAL_MS)
    }

    private fun uploadSchemaDiff() {
        // Skip if buffer is empty
        if (schemaDiffNewEvents.isEmpty() &&
            schemaDiffNewEventParams.isEmpty() &&
            schemaDiffNewUserProperties.isEmpty() &&
            schemaDiffNewDeviceProperties.isEmpty()) {
            return
        }

        executor.execute {
            try {
                // Build payload
                val payload = mutableMapOf<String, Any>(
                    "tenant_id" to tenantId,
                    "env" to environment
                )

                if (schemaDiffNewEvents.isNotEmpty()) {
                    payload["new_events"] = schemaDiffNewEvents.toList()
                }

                if (schemaDiffNewEventParams.isNotEmpty()) {
                    val eventParams = mutableMapOf<String, Any>()
                    for ((eventName, data) in schemaDiffNewEventParams) {
                        eventParams[eventName] = mapOf(
                            "params" to data.first.toList(),
                            "param_values" to data.second.mapValues { it.value.toList() }
                        )
                    }
                    payload["new_event_params"] = eventParams
                }

                if (schemaDiffNewUserProperties.isNotEmpty()) {
                    payload["new_user_properties"] = schemaDiffNewUserProperties.mapValues { it.value.toList() }
                }

                if (schemaDiffNewDeviceProperties.isNotEmpty()) {
                    payload["new_device_properties"] = schemaDiffNewDeviceProperties.mapValues { it.value.toList() }
                }

                // Send to collectMeta endpoint
                val functionUrl = "https://us-central1-appfig-dev.cloudfunctions.net/collectMeta"
                val jsonPayload = gson.toJson(payload)

                val mediaType = "application/json; charset=utf-8".toMediaType()
                val requestBody = jsonPayload.toByteArray().toRequestBody(mediaType)

                val request = Request.Builder()
                    .url(functionUrl)
                    .post(requestBody)
                    .header("Content-Type", "application/json")
                    .header("X-API-Key", apiKey)
                    .build()

                val response = httpClient.newCall(request).execute()

                if (response.isSuccessful) {
                    log(AppFigLogLevel.DEBUG, "Schema uploaded successfully")

                    // Update last upload time
                    lastSchemaUploadTime = System.currentTimeMillis()
                    prefs?.edit()?.putLong("AppFig_LastSchemaUpload", lastSchemaUploadTime)?.apply()

                    // Clear buffer
                    synchronized(schemaEvents) {
                        schemaDiffNewEvents.clear()
                        schemaDiffNewEventParams.clear()
                        schemaDiffNewUserProperties.clear()
                        schemaDiffNewDeviceProperties.clear()
                    }

                    // Save updated schema cache
                    saveCachedSchema()
                } else {
                    log(AppFigLogLevel.WARN, "Schema upload failed: ${response.code}")
                }
            } catch (e: Exception) {
                Log.w(TAG, "Schema upload failed: ${e.message}")
            }
        }
    }

    private fun loadCachedSchema() {
        try {
            val schemaKey = "AppFig_Schema_${companyId}_${tenantId}_${environment}"
            val cached = prefs?.getString(schemaKey, null) ?: return

            val type = object : TypeToken<Map<String, Any>>() {}.type
            val data: Map<String, Any> = gson.fromJson(cached, type)

            (data["events"] as? List<*>)?.let { events ->
                schemaEvents.addAll(events.filterIsInstance<String>())
            }

            (data["eventParams"] as? List<*>)?.let { eventParams ->
                for (ep in eventParams) {
                    if (ep is Map<*, *>) {
                        val eventName = ep["eventName"] as? String ?: continue
                        val parameters = ep["parameters"] as? List<*> ?: continue
                        schemaEventParams[eventName] = parameters.filterIsInstance<String>().toMutableSet()
                    }
                }
            }
        } catch (e: Exception) {
            Log.w(TAG, "Failed to load cached schema: ${e.message}")
        }
    }

    private fun saveCachedSchema() {
        try {
            val schemaKey = "AppFig_Schema_${companyId}_${tenantId}_${environment}"

            val data = mutableMapOf<String, Any>()
            data["events"] = schemaEvents.toList()

            val eventParamsArray = mutableListOf<Map<String, Any>>()
            for ((eventName, params) in schemaEventParams) {
                eventParamsArray.add(mapOf(
                    "eventName" to eventName,
                    "parameters" to params.toList()
                ))
            }
            data["eventParams"] = eventParamsArray

            val jsonString = gson.toJson(data)
            prefs?.edit()?.putString(schemaKey, jsonString)?.apply()
        } catch (e: Exception) {
            Log.w(TAG, "Failed to save schema cache: ${e.message}")
        }
    }
}
