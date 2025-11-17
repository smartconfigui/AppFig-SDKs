/**
 * AppFig Unity SDK - Client-side feature flags and remote configuration
 *
 * Version: 2.0.0
 * Platform: Unity (C#)
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
 *   AppFig.Init(
 *       companyId: "acmegames",
 *       tenantId: "spaceshooter",
 *       env: "prod",
 *       apiKey: "your-api-key",
 *       autoRefresh: true,
 *       pollInterval: 43200000  // 12 hours
 *   );
 *
 *   // Log events
 *   AppFig.LogEvent("level_complete", new Dictionary<string, string> { {"level", "5"} });
 *
 *   // Check features
 *   bool isEnabled = AppFig.IsFeatureEnabled("double_xp");
 *   string value = AppFig.GetFeatureValue("max_lives");
 *
 * Local Mode Usage (Development/Testing):
 *   // Initialize without API key
 *   AppFig.InitLocal("appfig_rules");  // Loads from Resources folder
 *
 *   // Log events (same as cloud mode)
 *   AppFig.LogEvent("test_event");
 *
 * IMPORTANT - Local Mode Behavior:
 * - Events are persisted (same as cloud mode)
 * - Event history maintained across sessions
 * - No network calls or CDN access
 * - No companyId/tenantId required
 * - Suitable for testing and development only
 */

using UnityEngine;
using UnityEngine.Networking;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

public static class AppFig
{
    private static List<EventRecord> eventHistory = new();
    private static Dictionary<string, int> eventCounts = new();
    private static Dictionary<string, string> userProperties = new();
    private static Dictionary<string, string> deviceProperties = new();
    private static Dictionary<string, string> features = new();
    private static List<Rule> rules = new();
    private static AppFigRunner runner;
    private static string cdnBaseUrl;
    private static string pointerUrl;
    private static string companyId;
    private static string tenantId;
    private static string environment;
    private static string apiKey;
    private static bool autoRefreshEnabled = true; // Enabled by default
    private static long pollIntervalMs = 43200000; // 12 hours default (Growth plan)
    private static DateTime lastFetchTime = DateTime.MinValue;
    private static bool isFetchInProgress = false;
    private const int THREADING_THRESHOLD_RULES = 500; // Hardcoded threshold for background parsing
    private static string currentScreen = "";
    private static DateTime lastActivity = DateTime.UtcNow;
    private static bool sessionActive = false;
    private static long sessionTimeoutMs = 30 * 60 * 1000; // 30 minutes default
    private static bool debugMode = false;
    private static int maxEvents = 5000; // Default: 5000 events, Max: 100000
    private static int maxEventAgeDays = 7; // Default: 7 days, Max: 365
    private static int eventsSavedCount = 0;
    private static Coroutine eventSaveDebounceCoroutine = null;

    // Indexing system for performance optimization
    private static Dictionary<string, HashSet<string>> eventToFeaturesIndex = new();
    private static Dictionary<string, HashSet<string>> userPropertyToFeaturesIndex = new();
    private static Dictionary<string, HashSet<string>> devicePropertyToFeaturesIndex = new();
    private static Dictionary<string, List<Rule>> featureToRulesIndex = new();
    private static string currentRulesHash = "";
    private static bool isInitialized = false;

    // Callbacks and listeners
    private static Action onReadyCallback = null;
    private static Action onRulesUpdatedCallback = null;
    private static Dictionary<string, HashSet<Action<string, string>>> featureListeners = new();

    // Helper class for schema event params (replaces tuple for Unity compatibility)
    private class SchemaEventParamData
    {
        public HashSet<string> Params { get; set; }
        public Dictionary<string, HashSet<string>> ParamValues { get; set; }

        public SchemaEventParamData()
        {
            Params = new HashSet<string>();
            ParamValues = new Dictionary<string, HashSet<string>>();
        }
    }

    // Schema discovery
    private static HashSet<string> schemaEvents = new();
    private static Dictionary<string, HashSet<string>> schemaEventParams = new();
    private static Dictionary<string, Dictionary<string, HashSet<string>>> schemaEventParamValues = new();
    private static Dictionary<string, HashSet<string>> schemaUserProperties = new();
    private static Dictionary<string, HashSet<string>> schemaDeviceProperties = new();
    private static HashSet<string> schemaDiffNewEvents = new();
    private static Dictionary<string, SchemaEventParamData> schemaDiffNewEventParams = new();
    private static Dictionary<string, HashSet<string>> schemaDiffNewUserProperties = new();
    private static Dictionary<string, HashSet<string>> schemaDiffNewDeviceProperties = new();
    private static int schemaUploadCount = 0;
    private static Coroutine schemaUploadCoroutine = null;
    private static string deviceId = "";
    private static long lastSchemaUploadTime = 0;
    private const int SCHEMA_UPLOAD_BATCH_SIZE = 50;
    private const int SCHEMA_UPLOAD_INTERVAL_MS = 600000; // 10 minutes
    private const long SCHEMA_UPLOAD_THROTTLE_MS = 43200000; // 12 hours

    // Local mode configuration
    private static bool useLocalMode = false;
    private static string localRulesResourcePath = "";
    private const string DEFAULT_LOCAL_RULES_PATH = "appfig_rules";

    // Logging system
    private enum AppFigLogLevel
    {
        Info,
        Warn,
        Error,
        Debug
    }

    private static void Log(AppFigLogLevel level, string message)
    {
        // Errors and warnings always show
        if (level == AppFigLogLevel.Warn || level == AppFigLogLevel.Error)
        {
            if (level == AppFigLogLevel.Error)
                Debug.LogError($"[AppFig] {message}");
            else
                Debug.LogWarning($"[AppFig] {message}");
            return;
        }

        // Info + Debug only show when debugMode=true
        if (!debugMode) return;

        if (level == AppFigLogLevel.Info)
            Debug.Log($"[AppFig] {message}");
        else
            Debug.Log($"[AppFig:Debug] {message}");
    }

    [Serializable]
    public class ErrorResponse
    {
        public string error;
        public string message;
        public string code;
        public string upgrade_url;
        public int current_usage;
        public int included_fetches;
        public int overage_amount;
        public float estimated_cost;
        public float overage_cap;
    }

    [Serializable]
    private class PointerData
    {
        public string schema_version;
        public string version;
        public string path;
        public string updated_at;
        public int feature_count;
        public int ttl_secs;
        public int min_poll_interval_secs;
    }

    /// <summary>
    /// Initialize AppFig SDK in cloud mode (paid plans)
    /// </summary>
    /// <param name="companyId">Your company ID from Firestore (e.g., 'acmegames')</param>
    /// <param name="tenantId">Your tenant/project ID from Firestore (e.g., 'spaceshooter')</param>
    /// <param name="env">Environment: 'dev' or 'prod'</param>
    /// <param name="apiKey">Your API key from AppFig dashboard</param>
    /// <param name="autoRefresh">Enable automatic background rule updates (default: true)</param>
    /// <param name="pollInterval">Auto-refresh interval in milliseconds (default: 12 hours)</param>
    /// <param name="debugMode">Enable debug logging (default: false)</param>
    /// <param name="sessionTimeoutMs">Session timeout in milliseconds (default: 30 minutes, range: 1 min - 2 hours)</param>
    /// <param name="maxEvents">Maximum events to store (100-100000, default: 5000)</param>
    /// <param name="maxEventAgeDays">Maximum age of events in days (1-365, default: 7)</param>
    /// <param name="onReady">Optional callback fired when SDK is ready (once with cached rules, again with fresh rules)</param>
    /// <param name="onRulesUpdated">Optional callback fired when rules are updated from CDN</param>
    public static void Init(string companyId, string tenantId, string env, string apiKey, bool autoRefresh = true, long pollInterval = 43200000, bool debugMode = false, long sessionTimeoutMs = 1800000, int maxEvents = 5000, int maxEventAgeDays = 7, Action onReady = null, Action onRulesUpdated = null)
    {
        // Validate API key
        if (string.IsNullOrEmpty(apiKey))
        {
            Log(AppFigLogLevel.Error, "API key is required for remote mode. Use InitLocal() for local development without an API key.");
            return;
        }

        // Validate company ID and tenant ID
        if (string.IsNullOrEmpty(companyId))
        {
            Log(AppFigLogLevel.Error, "Company ID is required. This should be your Firestore company document ID, not the company name.");
            return;
        }

        if (string.IsNullOrEmpty(tenantId))
        {
            Log(AppFigLogLevel.Error, "Tenant ID is required. This should be your Firestore tenant document ID, not the tenant name.");
            return;
        }

        // Validate ID formats (basic validation to catch common mistakes)
        if (companyId.Contains(" "))
        {
            Log(AppFigLogLevel.Error, $"Invalid company ID '{companyId}' - IDs cannot contain spaces. Use the Firestore document ID, not the display name.");
            return;
        }

        if (tenantId.Contains(" "))
        {
            Log(AppFigLogLevel.Error, $"Invalid tenant ID '{tenantId}' - IDs cannot contain spaces. Use the Firestore document ID, not the display name.");
            return;
        }

        // Warn about suspicious ID patterns
        if (companyId.Length > 100 || tenantId.Length > 100)
        {
            Debug.LogWarning($"[AppFig] Unusually long ID detected. Company ID: {companyId.Length} chars, Tenant ID: {tenantId.Length} chars. Are you using the correct Firestore document IDs?");
        }

        // Create runner first before any operations that need it
        if (runner == null)
        {
            var go = new GameObject("AppFigRunner");
            UnityEngine.Object.DontDestroyOnLoad(go);
            runner = go.AddComponent<AppFigRunner>();
        }

        ResetSession();

        // Auto-collect device properties
        CollectDeviceProperties();

        // Log first_open event
        LogFirstOpenIfNeeded();

        // Start session tracking
        StartSessionTracking();

        useLocalMode = false;
        AppFig.apiKey = apiKey;
        pollIntervalMs = pollInterval;
        AppFig.debugMode = debugMode;
        onReadyCallback = onReady;
        onRulesUpdatedCallback = onRulesUpdated;

        // Validate and set session timeout
        AppFig.sessionTimeoutMs = sessionTimeoutMs;
        const long MIN_SESSION_TIMEOUT = 60000; // 1 minute
        const long MAX_SESSION_TIMEOUT = 7200000; // 2 hours
        if (AppFig.sessionTimeoutMs < MIN_SESSION_TIMEOUT || AppFig.sessionTimeoutMs > MAX_SESSION_TIMEOUT)
        {
            Log(AppFigLogLevel.Warn, $"Session timeout {AppFig.sessionTimeoutMs}ms out of range. Clamping to {MIN_SESSION_TIMEOUT}-{MAX_SESSION_TIMEOUT}ms");
            AppFig.sessionTimeoutMs = Math.Max(MIN_SESSION_TIMEOUT, Math.Min(MAX_SESSION_TIMEOUT, AppFig.sessionTimeoutMs));
        }
        Log(AppFigLogLevel.Info, "Initializing AppFig");

        // Validate and set event retention parameters
        AppFig.maxEvents = Mathf.Clamp(maxEvents, 100, 100000);
        AppFig.maxEventAgeDays = Mathf.Clamp(maxEventAgeDays, 1, 365);

        if (maxEvents != AppFig.maxEvents)
        {
            Debug.LogWarning($"[AppFig] maxEvents {maxEvents} out of range. Clamped to {AppFig.maxEvents}");
        }

        if (maxEventAgeDays != AppFig.maxEventAgeDays)
        {
            Debug.LogWarning($"[AppFig] maxEventAgeDays {maxEventAgeDays} out of range. Clamped to {AppFig.maxEventAgeDays}");
        }

        if (AppFig.maxEvents > 10000)
        {
            Debug.LogWarning($"[AppFig] ⚠️ Large event limit ({AppFig.maxEvents}) may impact memory and storage.");
        }


        // Load cached events from PlayerPrefs
        LoadCachedEvents(companyId, tenantId, env);

        // Store companyId/tenantId/env for later use
        AppFig.companyId = companyId;
        AppFig.tenantId = tenantId;
        environment = env;
        autoRefreshEnabled = autoRefresh;

        // Use CDN endpoint (Firebase Hosting)
        cdnBaseUrl = "https://rules-prod.appfig.com";
        pointerUrl = $"{cdnBaseUrl}/rules_versions/{companyId}/{tenantId}/{env}/current/latest.json";


        // Load cached rules and evaluate immediately
        Log(AppFigLogLevel.Info, "Rules loaded from cache");
        bool cacheLoaded = LoadCachedRules(companyId, tenantId, env);

        // If cache loaded successfully, mark as initialized immediately
        if (cacheLoaded)
        {
            Log(AppFigLogLevel.Info, "SDK ready");
            isInitialized = true;
        }
        else
        {
        }

        // Fetch pointer to check for updates
        Log(AppFigLogLevel.Info, "Fetching latest rules");
        runner.StartCoroutine(FetchRulesFromCDNWithCallback(companyId, tenantId, env, () => {
            isInitialized = true;

            // Start auto-refresh after first fetch completes
            if (autoRefreshEnabled) {
                runner.StartCoroutine(AutoRefreshCoroutine());
            }
        }));

        // Initialize schema discovery
        InitSchemaDiscovery();

        // Log initial screen view
        LogScreenView("main_menu");

    }

    public static void InitLocal(Action onReady = null, Action onRulesUpdated = null)
    {
        InitLocal(DEFAULT_LOCAL_RULES_PATH, onReady, onRulesUpdated);
    }

    public static void InitLocal(string rulesResourcePath, Action onReady = null, Action onRulesUpdated = null)
    {
        // Create runner first before any operations that need it
        if (runner == null)
        {
            var go = new GameObject("AppFigRunner");
            UnityEngine.Object.DontDestroyOnLoad(go);
            runner = go.AddComponent<AppFigRunner>();
        }

        ResetSession();

        // Auto-collect device properties
        CollectDeviceProperties();

        // Standalone country detection for local mode
        runner.StartCoroutine(DetectCountryFromCDN());

        // Log first_open event
        LogFirstOpenIfNeeded();

        // Start session tracking
        StartSessionTracking();

        useLocalMode = true;
        localRulesResourcePath = rulesResourcePath;
        onReadyCallback = onReady;
        onRulesUpdatedCallback = onRulesUpdated;

        // Set sentinel values for cache keys in local mode
        AppFig.companyId = "local";
        AppFig.tenantId = "local";
        environment = "local";


        // Load cached events from PlayerPrefs (same as cloud mode)
        LoadCachedEvents("local", "local", "local");

        LoadLocalRules();

        // Fire onReady callback after rules are loaded
        onReadyCallback?.Invoke();

        // Log initial screen view
        LogScreenView("main_menu");
    }

    public static void LogEvent(string eventName, Dictionary<string, string> parameters = null)
    {
        // Track schema
        var paramsAsObject = parameters?.ToDictionary(k => k.Key, k => (object)k.Value);
        TrackEventSchema(eventName, paramsAsObject);

        var evt = new EventRecord
        {
            name = eventName,
            timestampTicks = DateTime.UtcNow.Ticks,
            parameters = parameters ?? new()
        };
        eventHistory.Add(evt);

        // Enforce retention limits
        EnforceEventRetention();

        // Debounce saving events (batch save every 10 events or 5 seconds)
        eventsSavedCount++;
        if (eventsSavedCount >= 10)
        {
            eventsSavedCount = 0;
            if (eventSaveDebounceCoroutine != null)
            {
                runner.StopCoroutine(eventSaveDebounceCoroutine);
                eventSaveDebounceCoroutine = null;
            }
            SaveCachedEvents();
        }
        else
        {
            // Start or restart the 5-second debounce timer
            if (eventSaveDebounceCoroutine != null)
            {
                runner.StopCoroutine(eventSaveDebounceCoroutine);
            }
            eventSaveDebounceCoroutine = runner.StartCoroutine(DebounceSaveEvents());
        }

        if (!eventCounts.ContainsKey(eventName)) eventCounts[eventName] = 0;
        eventCounts[eventName] += 1;


        EvaluateAllFeatures();
    }

    public static void LogScreenView(string screenName, string previousScreen = null)
    {
        if (screenName == currentScreen) return; // Don't log same screen twice
        
        var parameters = new Dictionary<string, string>
        {
            ["screen_name"] = screenName,
            ["previous_screen"] = previousScreen ?? currentScreen ?? "none"
        };
        
        currentScreen = screenName;
        LogEvent("screen_view", parameters);
    }

    public static void SetAppVersion(string version)
    {
        SetDeviceProperty("app_version", version);
    }

    public static void SetUserProperty(string key, string value)
    {
        if (string.IsNullOrEmpty(key))
        {
            Debug.LogWarning("[AppFig] Cannot set user property with null or empty key");
            return;
        }

        if (value == null)
        {
            Debug.LogWarning($"[AppFig] Cannot set user property '{key}' to null value. Use RemoveUserProperty to remove it.");
            return;
        }

        userProperties[key] = value;
        Log(AppFigLogLevel.Debug, $"User property set: {key}");

        // Track schema
        TrackUserPropertySchema(new Dictionary<string, object> { { key, value } });

        EvaluateAllFeatures();
    }

    public static void RemoveUserProperty(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            Debug.LogWarning("[AppFig] Cannot remove user property with null or empty key");
            return;
        }

        if (userProperties.Remove(key))
        {
            EvaluateAllFeatures();
        }
    }
    public static void SetDeviceProperty(string key, string value)
    {
        if (string.IsNullOrEmpty(key))
        {
            Debug.LogWarning("[AppFig] Cannot set device property with null or empty key");
            return;
        }

        if (value == null)
        {
            Debug.LogWarning($"[AppFig] Cannot set device property '{key}' to null value. Use RemoveDeviceProperty to remove it.");
            return;
        }

        deviceProperties[key] = value;
        Log(AppFigLogLevel.Debug, $"Device property set: {key}");

        // Track schema
        TrackDevicePropertySchema(new Dictionary<string, object> { { key, value } });

        EvaluateAllFeatures();
    }

    public static void RemoveDeviceProperty(string key)
    {
        if (string.IsNullOrEmpty(key))
        {
            Debug.LogWarning("[AppFig] Cannot remove device property with null or empty key");
            return;
        }

        if (deviceProperties.Remove(key))
        {
            EvaluateAllFeatures();
        }
    }

    public static bool IsFeatureEnabled(string feature)
    {
        // Check if rules need refreshing (automatic updating)
        CheckAndTriggerAutoRefresh();

        return features.ContainsKey(feature) && features[feature] == "on";
    }

    public static string GetFeatureValue(string feature)
    {
        // Check if rules need refreshing (automatic updating)
        CheckAndTriggerAutoRefresh();

        // Return cached value if exists
        if (features.ContainsKey(feature))
        {
            return features[feature];
        }


        // Evaluate on-demand if not cached
        foreach (var rule in rules)
        {
            if (rule.feature == feature)
            {
                bool passed = EvaluateConditions(rule.conditions);
                if (passed)
                {
                    string value = rule.value ?? "on";
                    features[feature] = value;
                    return value;
                }
            }
        }

        // No matching rule found
        features[feature] = null;
        return null;
    }

    private static void CheckAndTriggerAutoRefresh()
    {
        // Only check if auto-refresh is enabled and not in local mode
        if (!autoRefreshEnabled || useLocalMode || runner == null || isFetchInProgress)
        {
            return;
        }

        // Check if poll interval has elapsed
        long timeSinceLastFetch = (long)(DateTime.UtcNow - lastFetchTime).TotalMilliseconds;
        if (timeSinceLastFetch > pollIntervalMs)
        {
            Log(AppFigLogLevel.Info, "Fetching latest rules");
            // Update lastFetchTime immediately to prevent multiple triggers
            lastFetchTime = DateTime.UtcNow;
            isFetchInProgress = true;
            // Trigger background refresh (non-blocking)
            runner.StartCoroutine(FetchRulesFromCDN(companyId, tenantId, environment));
        }
    }

    public static bool HasRulesLoaded() =>
        rules.Count > 0 || features.Count > 0;

    /// <summary>
    /// Reset a specific feature's cached value and force re-evaluation
    /// Useful for implementing recurring triggers (e.g., "show popup every 3 events")
    ///
    /// Example usage:
    /// <code>
    /// // Check if feature is enabled
    /// if (AppFig.IsFeatureEnabled("level_complete_popup")) {
    ///     ShowPopup();
    ///     // Reset the feature so it can trigger again after next 3 events
    ///     AppFig.ResetFeature("level_complete_popup");
    /// }
    /// </code>
    /// </summary>
    /// <param name="featureName">Name of the feature to reset</param>
    public static void ResetFeature(string featureName)
    {
        if (!isInitialized)
        {
            Log(AppFigLogLevel.Warn, "Cannot reset feature: AppFig not initialized");
            return;
        }

        if (rules.Count == 0)
        {
            Log(AppFigLogLevel.Warn, "Cannot reset feature: No rules loaded");
            return;
        }

        Log(AppFigLogLevel.Debug, $"Resetting feature: {featureName}");

        // Store old value for comparison
        string oldValue = features.ContainsKey(featureName) ? features[featureName] : null;

        // Clear cached value
        features.Remove(featureName);

        // Re-evaluate the feature immediately
        string newValue = EvaluateFeature(featureName);

        // Update features map with new evaluation
        features[featureName] = newValue;

        // Notify listeners if value changed
        if (oldValue != newValue)
        {
            Log(AppFigLogLevel.Debug, $"Feature value changed after reset: {featureName} = {newValue}");
            NotifyListeners(new HashSet<string> { featureName });
        }
        else
        {
            Log(AppFigLogLevel.Debug, $"Feature value unchanged after reset: {featureName} = {newValue}");
        }
    }

    /// <summary>
    /// Reset all features and force complete re-evaluation
    /// This clears all cached feature values and re-evaluates all rules
    ///
    /// Use this sparingly - typically you want to reset specific features using ResetFeature()
    /// </summary>
    public static void ResetAllFeatures()
    {
        if (!isInitialized)
        {
            Log(AppFigLogLevel.Warn, "Cannot reset features: AppFig not initialized");
            return;
        }

        if (rules.Count == 0)
        {
            Log(AppFigLogLevel.Warn, "Cannot reset features: No rules loaded");
            return;
        }

        Log(AppFigLogLevel.Debug, "Resetting all features");

        // Clear all caches
        features.Clear();

        // Re-evaluate all features
        EvaluateAllFeatures();

        Log(AppFigLogLevel.Debug, "All features reset and re-evaluated");
    }

    /// <summary>
    /// Add a listener for feature value changes
    /// Useful for reactive frameworks (Unity UI, etc.)
    /// </summary>
    /// <param name="featureName">Name of the feature to listen to</param>
    /// <param name="listener">Callback invoked when feature value changes (featureName, newValue)</param>
    public static void AddListener(string featureName, Action<string, string> listener)
    {
        if (!featureListeners.ContainsKey(featureName))
        {
            featureListeners[featureName] = new HashSet<Action<string, string>>();
        }
        featureListeners[featureName].Add(listener);
    }

    /// <summary>
    /// Remove a feature listener
    /// </summary>
    /// <param name="featureName">Name of the feature</param>
    /// <param name="listener">Listener to remove</param>
    public static void RemoveListener(string featureName, Action<string, string> listener)
    {
        if (featureListeners.ContainsKey(featureName))
        {
            featureListeners[featureName].Remove(listener);
            if (featureListeners[featureName].Count == 0)
            {
                featureListeners.Remove(featureName);
            }
        }
    }

    /// <summary>
    /// Remove all listeners for a feature
    /// </summary>
    /// <param name="featureName">Name of the feature</param>
    public static void RemoveAllListeners(string featureName)
    {
        featureListeners.Remove(featureName);
    }

    /// <summary>
    /// Remove all listeners for all features
    /// </summary>
    public static void ClearAllListeners()
    {
        featureListeners.Clear();
    }

    /// <summary>
    /// Detect and set the country code using static endpoint
    /// [DEPRECATED] Country is now automatically detected from CDN response headers during rules fetch.
    /// This fallback method is kept for backward compatibility.
    /// This is a standalone method that can be called manually.
    /// Country is automatically detected during Init(), so this is only needed if you want to refresh it.
    /// </summary>
    [System.Obsolete("Country is now automatically detected from CDN response headers during rules fetch.")]
    public static void DetectAndSetCountry()
    {
        if (runner == null)
        {
            Log(AppFigLogLevel.Error, "Cannot detect country: SDK not initialized. Call Init() first.");
            return;
        }
        runner.StartCoroutine(FetchCountryCode());
    }

    /// <summary>
    /// Reset the session tracking state
    /// Clears session-related data but preserves events and properties
    /// </summary>
    public static void ResetSession()
    {
        sessionActive = false;
        currentScreen = "";
        lastActivity = DateTime.UtcNow;
    }

    private static void CollectDeviceProperties()
    {
        
        // Platform detection
        string platform = GetPlatform();
        SetDeviceProperty("platform", platform);
        
        // OS Version
        string osVersion = GetOSVersion();
        SetDeviceProperty("os_version", osVersion);
        
        // Language
        string language = GetLanguage();
        SetDeviceProperty("language", language);
        
        // Timezone
        string timezone = GetTimezone();
        SetDeviceProperty("timezone", timezone);
        
        // Device brand and model
        string deviceBrand = GetDeviceBrand();
        string deviceModel = GetDeviceModel();
        SetDeviceProperty("device_brand", deviceBrand);
        SetDeviceProperty("device_model", deviceModel);

        // Country will be auto-detected from CDN response headers during rules fetch
        // For local mode, DetectCountryFromCDN() must be called separately

    }

    private static IEnumerator DetectCountryFromCDN()
    {
        string url = $"https://rules-dev.appfig.com/rules_versions/country/country/dev/current/latest.json?ts={DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        UnityWebRequest www = UnityWebRequest.Get(url);
        www.disposeDownloadHandlerOnDispose = true;
        www.disposeCertificateHandlerOnDispose = true;
        www.disposeUploadHandlerOnDispose = true;
        www.timeout = 5;
        yield return www.SendWebRequest();

        if (www.result == UnityWebRequest.Result.Success)
        {
            string countryHeader = www.GetResponseHeader("Country");
            if (!string.IsNullOrEmpty(countryHeader))
            {
                SetDeviceProperty("country", countryHeader);
            }
        }
    }
    
    private static string GetPlatform()
    {
        switch (Application.platform)
        {
            case RuntimePlatform.IPhonePlayer: return "iOS";
            case RuntimePlatform.Android: return "Android";
            case RuntimePlatform.OSXPlayer: return "macOS";
            case RuntimePlatform.WindowsPlayer: return "Windows";
            case RuntimePlatform.LinuxPlayer: return "Linux";
            case RuntimePlatform.WebGLPlayer: return "WebGL";
            case RuntimePlatform.WSAPlayerX86:
            case RuntimePlatform.WSAPlayerX64:
            case RuntimePlatform.WSAPlayerARM: return "UWP";
            default: return "Unknown";
        }
    }
    
    private static string GetOSVersion()
    {
        return SystemInfo.operatingSystem;
    }
    
    private static string GetLanguage()
    {
        return Application.systemLanguage.ToString();
    }
    
    private static string GetTimezone()
    {
        return TimeZoneInfo.Local.Id;
    }
    
    private static string GetDeviceBrand()
    {
        string deviceModel = SystemInfo.deviceModel.ToLower();
        
        if (deviceModel.Contains("iphone") || deviceModel.Contains("ipad")) return "Apple";
        if (deviceModel.Contains("samsung")) return "Samsung";
        if (deviceModel.Contains("huawei")) return "Huawei";
        if (deviceModel.Contains("xiaomi")) return "Xiaomi";
        if (deviceModel.Contains("oppo")) return "Oppo";
        if (deviceModel.Contains("vivo")) return "Vivo";
        if (deviceModel.Contains("oneplus")) return "OnePlus";
        if (deviceModel.Contains("lg")) return "LG";
        if (deviceModel.Contains("htc")) return "HTC";
        if (deviceModel.Contains("sony")) return "Sony";
        if (deviceModel.Contains("motorola")) return "Motorola";
        if (deviceModel.Contains("nokia")) return "Nokia";
        if (deviceModel.Contains("pixel")) return "Google";
        
        return "Unknown";
    }
    
    private static string GetDeviceModel()
    {
        return SystemInfo.deviceModel;
    }
    
    private static IEnumerator FetchCountryCode()
    {
        string url = $"https://rules-dev.appfig.com/rules_versions/country/country/dev/current/latest.json?ts={DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()}";
        UnityWebRequest www = UnityWebRequest.Get(url);
        www.disposeDownloadHandlerOnDispose = true;
        www.disposeCertificateHandlerOnDispose = true;
        www.disposeUploadHandlerOnDispose = true;
        www.timeout = 5;
        yield return www.SendWebRequest();

        if (www.result == UnityWebRequest.Result.Success)
        {
            try
            {
                string countryHeader = www.GetResponseHeader("Country");
                if (!string.IsNullOrEmpty(countryHeader))
                {
                    SetDeviceProperty("country", countryHeader);
                }
            }
            catch (Exception e)
            {
            }
        }
        else
        {
        }
    }
    
    private static void LogFirstOpenIfNeeded()
    {
        if (!PlayerPrefs.HasKey("AppFig_FirstOpen"))
        {
            LogEvent("first_open");
            PlayerPrefs.SetInt("AppFig_FirstOpen", 1);
            PlayerPrefs.Save();
        }
    }
    
    private static void StartSessionTracking()
    {
        LogEvent("session_start");
        sessionActive = true;
        lastActivity = DateTime.UtcNow;
        
        // Start session timeout coroutine
        runner.StartCoroutine(SessionTimeoutCoroutine());
        
        // Listen for application focus events
        Application.focusChanged += OnApplicationFocusChanged;
    }
    
    private static void OnApplicationFocusChanged(bool hasFocus)
    {
        if (!hasFocus && sessionActive)
        {
            EndSession();
        }
        else if (hasFocus && !sessionActive)
        {
            LogEvent("session_start");
            sessionActive = true;
            lastActivity = DateTime.UtcNow;
            runner.StartCoroutine(SessionTimeoutCoroutine());
        }
    }
    
    private static IEnumerator SessionTimeoutCoroutine()
    {
        while (sessionActive)
        {
            yield return new WaitForSeconds(60f); // Check every minute

            if (sessionActive && (DateTime.UtcNow - lastActivity).TotalMilliseconds >= sessionTimeoutMs)
            {
                EndSession();
                break;
            }
        }
    }
    
    private static void EndSession()
    {
        if (sessionActive)
        {
            LogEvent("session_end");
            sessionActive = false;
        }
    }
    

    public static void UpdateActivity()
    {
        lastActivity = DateTime.UtcNow;
    }
    /// <summary>
    /// Enable or disable auto-refresh
    /// </summary>
    /// <param name="enabled">true to enable auto-refresh, false to disable</param>
    [Obsolete("Use the autoRefresh parameter in Init() instead")]
    public static void SetAutoRefresh(bool enabled)
    {
        autoRefreshEnabled = enabled;
        if (enabled && runner != null && !string.IsNullOrEmpty(companyId))
        {
            runner.StartCoroutine(AutoRefreshCoroutine());
        }
    }

    /// <summary>
    /// Set the polling interval for auto-refresh
    /// </summary>
    /// <param name="intervalMs">Interval in milliseconds (min: 60000, default: 43200000)</param>
    [Obsolete("Use the pollInterval parameter in Init() instead")]
    public static void SetPollInterval(long intervalMs)
    {
        // Clamp between 1 minute and 24 hours
        long min = 60000; // 1 minute
        long max = 86400000; // 24 hours
        pollIntervalMs = Math.Max(min, Math.Min(max, intervalMs));
    }

    public static void RefreshRules()
    {
        if (useLocalMode)
        {
            LoadLocalRules();
        }
        else if (runner != null && !string.IsNullOrEmpty(companyId))
        {
            if (!string.IsNullOrEmpty(companyId) && !string.IsNullOrEmpty(tenantId) && !string.IsNullOrEmpty(environment))
            {
                runner.StartCoroutine(FetchRulesFromCDN(companyId, tenantId, environment));
            }
            else
            {
                Log(AppFigLogLevel.Error, "Cannot refresh: SDK not initialized");
            }
        }
    }

    private static IEnumerator AutoRefreshCoroutine()
    {
        if (useLocalMode)
        {
            yield break;
        }

        while (autoRefreshEnabled && !string.IsNullOrEmpty(companyId))
        {
            // Calculate time until next poll
            DateTime now = DateTime.UtcNow;
            TimeSpan timeSinceFetch = now - lastFetchTime;
            long msUntilNextPoll = pollIntervalMs - (long)timeSinceFetch.TotalMilliseconds;

            if (msUntilNextPoll > 0)
            {
                float waitSeconds = msUntilNextPoll / 1000f;
                yield return new WaitForSeconds(waitSeconds);
            }

            if (autoRefreshEnabled) // Check again in case it was disabled during wait
            {
                if (!string.IsNullOrEmpty(companyId) && !string.IsNullOrEmpty(tenantId) && !string.IsNullOrEmpty(environment))
                {
                    yield return FetchRulesFromCDN(companyId, tenantId, environment);
                }
            }
        }
    }

    private static void LoadLocalRules()
    {
        try
        {

            TextAsset jsonFile = Resources.Load<TextAsset>(localRulesResourcePath);

            if (jsonFile == null)
            {
                Log(AppFigLogLevel.Error, "Failed to load local rules");
                return;
            }

            string rawJson = jsonFile.text;
            ParseRules(rawJson);
        }
        catch (Exception e)
        {
            Log(AppFigLogLevel.Error, "Failed to load local rules");
        }
    }


    private static IEnumerator FetchRulesFromCDNWithCallback(string companyId, string tenantId, string env, Action onComplete)
    {
        yield return FetchRulesFromCDN(companyId, tenantId, env);
        onComplete?.Invoke();
    }

    private static IEnumerator FetchRulesFromCDN(string companyId, string tenantId, string env)
    {
        // Fetch pointer file to get current version hash
        yield return FetchPointer(
            onSuccess: (pointerJson) =>
            {
                // Extract Country header from CDN response (if available)
                // Note: Country header extraction happens in FetchPointer

                PointerData pointer = null;
                try
                {
                    pointer = JsonUtility.FromJson<PointerData>(pointerJson);
                }
                catch (Exception e)
                {
                    Log(AppFigLogLevel.Error, "JSON parse error");
                    isFetchInProgress = false;
                    return;
                }

                if (pointer == null || string.IsNullOrEmpty(pointer.version))
                {
                    Log(AppFigLogLevel.Error, "Invalid rule format");
                    isFetchInProgress = false;
                    return;
                }

                // Process pointer data
                ProcessPointerData(pointer, companyId, tenantId, env);
            },
            onFail: () =>
            {
                Log(AppFigLogLevel.Error, "Unable to load rules");

                bool hasCachedRules = rules.Count > 0 || features.Count > 0;
                if (hasCachedRules)
                {
                    Debug.LogWarning("[AppFig] ⚠️ Using cached rules due to pointer fetch error (graceful degradation)");
                    Debug.LogWarning("[AppFig] ✅ SDK is functional with cached data");
                }
                else
                {
                    Log(AppFigLogLevel.Error, "No rules available — all features disabled");
                }
                isFetchInProgress = false;
            }
        );
    }

    private static void ProcessPointerData(PointerData pointer, string companyId, string tenantId, string env)
    {

        // Enforce minimum poll interval from server (if present)
        if (pointer.min_poll_interval_secs > 0)
        {
            long minPollIntervalMs = pointer.min_poll_interval_secs * 1000L;
            if (pollIntervalMs < minPollIntervalMs)
            {
                pollIntervalMs = minPollIntervalMs;
            }
        }

        string cachedHash = GetCachedHash(companyId, tenantId, env);

        if (!string.IsNullOrEmpty(cachedHash) && cachedHash == pointer.version)
        {
            // Hash matches - cached rules are still current
            Log(AppFigLogLevel.Info, "Rules are up to date");
            lastFetchTime = DateTime.UtcNow;
            SaveCacheTimestamp(companyId, tenantId, env);

            // Rules already evaluated in LoadCachedRules, no need to re-evaluate
            int sampleCount = 0;
            foreach (var kvp in features)
            {
                if (sampleCount++ < 5)
                {
                }
            }

            // Just fire callback to signal CDN check is complete
            onRulesUpdatedCallback?.Invoke();

            isFetchInProgress = false;
            return;
        }

        Log(AppFigLogLevel.Info, "Rules updated from server");

        // Hash doesn't match - fetch immutable rules
        string immutableUrl = $"{cdnBaseUrl}/rules_versions/{companyId}/{tenantId}/{env}/current/{pointer.version}.json";
        runner.StartCoroutine(FetchImmutableRules(immutableUrl, pointer.version, companyId, tenantId, env));
    }

    private static IEnumerator FetchImmutableRules(string immutableUrl, string hash, string companyId, string tenantId, string env)
    {
        UnityWebRequest www = UnityWebRequest.Get(immutableUrl);
        yield return www.SendWebRequest();

        if (www.result != UnityWebRequest.Result.Success)
        {
            Log(AppFigLogLevel.Warn, "Failed to fetch remote rules – using cached version");

            bool hasCachedRules = rules.Count > 0 || features.Count > 0;
            if (hasCachedRules)
            {
                Debug.LogWarning("[AppFig] ⚠️ Using cached rules due to fetch error");
            }
            else
            {
                Log(AppFigLogLevel.Error, "No rules available — all features disabled");
            }
            yield break;
        }

        string rawJson = www.downloadHandler.text;

        bool useThreading = ShouldUseThreading(rawJson);

        if (useThreading)
        {
            yield return runner.StartCoroutine(ParseRulesAsync(rawJson));
        }
        else
        {
            ParseRules(rawJson);
        }


        // Build index and evaluate once
        BuildIndex();

        EvaluateAllFeatures();

        // Log sample feature values after update
        int sampleCount = 0;
        foreach (var kvp in features)
        {
            if (sampleCount++ < 5)
            {
            }
        }

        // Fire onRulesUpdated callback
        onRulesUpdatedCallback?.Invoke();

        lastFetchTime = DateTime.UtcNow;
        SaveCachedRules(companyId, tenantId, env, rawJson, hash);
    }

    private static bool ShouldUseThreading(string rawJson)
    {
        if (rawJson.Length > 500 * 1024)
        {
            return true;
        }

        int featureCount = System.Text.RegularExpressions.Regex.Matches(rawJson, "\"feature_key\"\\s*:").Count;
        return featureCount >= THREADING_THRESHOLD_RULES;
    }

    private static IEnumerator ParseRulesAsync(string rawJson)
    {
        bool parseComplete = false;
        Exception parseException = null;

        System.Threading.Thread parseThread = new System.Threading.Thread(() =>
        {
            try
            {
                ParseRules(rawJson);
                parseComplete = true;
            }
            catch (Exception e)
            {
                parseException = e;
                parseComplete = true;
            }
        });

        parseThread.Start();

        while (!parseComplete)
        {
            yield return null;
        }

        if (parseException != null)
        {
            Log(AppFigLogLevel.Error, "Rule format error");
        }
        else
        {
        }
    }

    private static void ParseRules(string rawJson)
    {
        try
        {
            features.Clear();

            // Handle v2 format with "features" wrapper
            // This format is used by both remote CDN and should be used for local mode
            if (rawJson.Contains("\"features\""))
            {

                try
                {
                    // Try to extract the features object more reliably
                    int featuresIndex = rawJson.IndexOf("\"features\"");
                    if (featuresIndex != -1)
                    {
                        // Find the colon after "features"
                        int colonIndex = rawJson.IndexOf(':', featuresIndex);
                        if (colonIndex != -1)
                        {
                            // Skip whitespace after colon
                            int startIndex = colonIndex + 1;
                            while (startIndex < rawJson.Length && char.IsWhiteSpace(rawJson[startIndex]))
                            {
                                startIndex++;
                            }

                            // Find matching closing brace
                            if (startIndex < rawJson.Length && rawJson[startIndex] == '{')
                            {
                                int braceCount = 0;
                                int endIndex = -1;
                                bool inString = false;
                                bool escaped = false;

                                for (int i = startIndex; i < rawJson.Length; i++)
                                {
                                    char c = rawJson[i];

                                    if (escaped)
                                    {
                                        escaped = false;
                                        continue;
                                    }

                                    if (c == '\\')
                                    {
                                        escaped = true;
                                        continue;
                                    }

                                    if (c == '"')
                                    {
                                        inString = !inString;
                                        continue;
                                    }

                                    if (!inString)
                                    {
                                        if (c == '{') braceCount++;
                                        else if (c == '}')
                                        {
                                            braceCount--;
                                            if (braceCount == 0)
                                            {
                                                endIndex = i;
                                                break;
                                            }
                                        }
                                    }
                                }

                                if (endIndex != -1)
                                {
                                    rawJson = rawJson.Substring(startIndex, endIndex - startIndex + 1);
                                }
                                else
                                {
                                    Log(AppFigLogLevel.Error, "Invalid rule format");
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Log(AppFigLogLevel.Warn, "Invalid rule format");
                }
            }
            else
            {
            }

            // Convert object-based JSON to array-based JSON that Unity can parse
            string convertedJson = ConvertToUnityFormat(rawJson);

            var wrapper = JsonUtility.FromJson<FeatureWrapper>(convertedJson);
            if (wrapper?.features == null)
            {
                Log(AppFigLogLevel.Error, "JSON parse error");
                return;
            }

            rules.Clear();

            foreach (var feature in wrapper.features)
            {
                foreach (var rule in feature.rules)
                {
                    rule.feature = feature.featureName;
                    rules.Add(rule);
                    
                    // Debug conditions
                    if (rule.conditions != null)
                    {
                        string eventCount = (rule.conditions.events?.events?.Count ?? 0).ToString();

                        if (rule.conditions.events != null && rule.conditions.events.events != null)
                        {
                            foreach (var evt in rule.conditions.events.events)
                            {
                            }
                        }

                        if (rule.conditions.device != null && rule.conditions.device.Count > 0)
                        {
                            foreach (var deviceCond in rule.conditions.device)
                            {
                            }
                        }

                        if (rule.conditions.user_properties != null && rule.conditions.user_properties.Count > 0)
                        {
                            foreach (var userCond in rule.conditions.user_properties)
                            {
                            }
                        }
                    }
                }
            }



            // Debug: Show all loaded rules
            foreach (var rule in rules)
            {
                string mode = rule.conditions?.events?.mode ?? "none";
            }

            // Build index for performance optimization
            BuildIndex();
            EvaluateAllFeatures();
        }
        catch (Exception e)
        {
            Log(AppFigLogLevel.Error, "Rule format error");
        }
    }

    private static string ConvertToUnityFormat(string objectJson)
    {
        try
        {
            // Remove outer braces and whitespace
            string content = objectJson.Trim();
            if (content.StartsWith("{")) content = content.Substring(1);
            if (content.EndsWith("}")) content = content.Substring(0, content.Length - 1);

            var features = new List<string>();
            int pos = 0;


            while (pos < content.Length)
            {
                // Skip whitespace and commas
                while (pos < content.Length && (char.IsWhiteSpace(content[pos]) || content[pos] == ','))
                    pos++;

                if (pos >= content.Length) break;

                // Find feature name (quoted string)
                if (content[pos] != '"')
                {
                    Debug.LogWarning($"[AppFig] Expected '\"' at position {pos}, found '{content[pos]}'. Stopping parse.");
                    break;
                }

                int nameStart = pos + 1;
                int nameEnd = content.IndexOf('"', nameStart);
                if (nameEnd == -1)
                {
                    Log(AppFigLogLevel.Error, "Invalid rule format");
                    break;
                }

                string featureName = content.Substring(nameStart, nameEnd - nameStart);
                pos = nameEnd + 1;


                // Skip whitespace and colon
                while (pos < content.Length && (char.IsWhiteSpace(content[pos]) || content[pos] == ':'))
                    pos++;

                if (pos >= content.Length)
                {
                    Debug.LogWarning($"[AppFig] Unexpected end after feature name '{featureName}'");
                    break;
                }

                // Check if value is an array (actual feature) or not (metadata field)
                if (content[pos] != '[')
                {

                    // Skip this field's value to continue parsing
                    int depth = 0;
                    bool inString = false;
                    char valueStart = content[pos];

                    if (valueStart == '"')
                    {
                        // Skip string value
                        pos++;
                        while (pos < content.Length)
                        {
                            if (content[pos] == '\\' && pos + 1 < content.Length)
                            {
                                pos += 2; // Skip escaped character
                                continue;
                            }
                            if (content[pos] == '"')
                            {
                                pos++;
                                break;
                            }
                            pos++;
                        }
                    }
                    else if (valueStart == '{' || valueStart == '[')
                    {
                        // Skip object or array value
                        char closeChar = valueStart == '{' ? '}' : ']';
                        depth = 1;
                        pos++;

                        while (pos < content.Length && depth > 0)
                        {
                            if (content[pos] == '"')
                            {
                                inString = !inString;
                            }
                            else if (!inString)
                            {
                                if (content[pos] == valueStart) depth++;
                                else if (content[pos] == closeChar) depth--;
                            }
                            pos++;
                        }
                    }
                    else
                    {
                        // Skip primitive value (number, boolean, null)
                        while (pos < content.Length && content[pos] != ',' && content[pos] != '}')
                            pos++;
                    }

                    continue;
                }

                // This is a feature with rules array

                // Find the matching closing bracket for the rules array
                int arrayStart = pos;
                int bracketCount = 0;
                int arrayEnd = -1;
                bool inRulesString = false;

                for (int i = pos; i < content.Length; i++)
                {
                    if (content[i] == '"' && (i == 0 || content[i-1] != '\\'))
                    {
                        inRulesString = !inRulesString;
                    }
                    else if (!inRulesString)
                    {
                        if (content[i] == '[') bracketCount++;
                        else if (content[i] == ']')
                        {
                            bracketCount--;
                            if (bracketCount == 0)
                            {
                                arrayEnd = i;
                                break;
                            }
                        }
                    }
                }

                if (arrayEnd == -1)
                {
                    Log(AppFigLogLevel.Error, "Invalid rule format");
                    break;
                }

                string rulesArray = content.Substring(arrayStart, arrayEnd - arrayStart + 1);
                pos = arrayEnd + 1;

                string featureObject = $"{{\"featureName\":\"{featureName}\",\"rules\":{rulesArray}}}";
                features.Add(featureObject);
            }

            string result = "{\"features\":[" + string.Join(",", features) + "]}";
            return result;
        }
        catch (Exception e)
        {
            Log(AppFigLogLevel.Error, "JSON parse error");
            return "{\"features\":[]}";
        }
    }

    private static void BuildIndex()
    {

        eventToFeaturesIndex.Clear();
        userPropertyToFeaturesIndex.Clear();
        devicePropertyToFeaturesIndex.Clear();
        featureToRulesIndex.Clear();

        int eventIndexCount = 0;
        int userPropIndexCount = 0;
        int devicePropIndexCount = 0;

        foreach (var rule in rules)
        {
            string featureName = rule.feature;

            // Build feature-to-rules index for O(1) rule lookup
            if (!featureToRulesIndex.ContainsKey(featureName))
            {
                featureToRulesIndex[featureName] = new List<Rule>();
            }
            featureToRulesIndex[featureName].Add(rule);

            if (rule.conditions != null)
            {
                if (rule.conditions.events != null && rule.conditions.events.events != null)
                {
                    foreach (var eventCond in rule.conditions.events.events)
                    {
                        if (!string.IsNullOrEmpty(eventCond.key))
                        {
                            if (!eventToFeaturesIndex.ContainsKey(eventCond.key))
                            {
                                eventToFeaturesIndex[eventCond.key] = new HashSet<string>();
                            }
                            eventToFeaturesIndex[eventCond.key].Add(featureName);
                            eventIndexCount++;
                        }
                    }
                }

                if (rule.conditions.user_properties != null)
                {
                    foreach (var userProp in rule.conditions.user_properties)
                    {
                        if (!string.IsNullOrEmpty(userProp.key))
                        {
                            if (!userPropertyToFeaturesIndex.ContainsKey(userProp.key))
                            {
                                userPropertyToFeaturesIndex[userProp.key] = new HashSet<string>();
                            }
                            userPropertyToFeaturesIndex[userProp.key].Add(featureName);
                            userPropIndexCount++;
                        }
                    }
                }

                if (rule.conditions.device != null)
                {
                    foreach (var deviceProp in rule.conditions.device)
                    {
                        if (!string.IsNullOrEmpty(deviceProp.key))
                        {
                            if (!devicePropertyToFeaturesIndex.ContainsKey(deviceProp.key))
                            {
                                devicePropertyToFeaturesIndex[deviceProp.key] = new HashSet<string>();
                            }
                            devicePropertyToFeaturesIndex[deviceProp.key].Add(featureName);
                            devicePropIndexCount++;
                        }
                    }
                }
            }
        }


        currentRulesHash = ComputeRulesHash();
        SaveIndexToPlayerPrefs();
    }

    private static string ComputeRulesHash()
    {
        // Using unchecked arithmetic to prevent overflow
        int hash = rules.Count;
        unchecked
        {
            foreach (var rule in rules)
            {
                hash = hash * 31 + (rule.feature?.GetHashCode() ?? 0);
                hash = hash * 31 + (rule.value?.GetHashCode() ?? 0);
            }
        }
        return hash.ToString();
    }

    private static void SaveIndexToPlayerPrefs()
    {
        try
        {
            PlayerPrefs.SetString("AppFig_IndexHash", currentRulesHash);

            var eventIndexJson = SerializeIndex(eventToFeaturesIndex);
            var userPropIndexJson = SerializeIndex(userPropertyToFeaturesIndex);
            var devicePropIndexJson = SerializeIndex(devicePropertyToFeaturesIndex);

            PlayerPrefs.SetString("AppFig_EventIndex", eventIndexJson);
            PlayerPrefs.SetString("AppFig_UserPropIndex", userPropIndexJson);
            PlayerPrefs.SetString("AppFig_DeviceIndex", devicePropIndexJson);
            PlayerPrefs.Save();

        }
        catch (Exception e)
        {
            Debug.LogWarning($"[AppFig] Failed to save index to PlayerPrefs: {e.Message}");
        }
    }

    private static string SerializeIndex(Dictionary<string, HashSet<string>> index)
    {
        var pairs = new List<string>();
        foreach (var kvp in index)
        {
            var features = string.Join(",", kvp.Value);
            pairs.Add($"{kvp.Key}:{features}");
        }
        return string.Join("|", pairs);
    }

    private static void LoadIndexFromPlayerPrefs()
    {
        try
        {
            string savedHash = PlayerPrefs.GetString("AppFig_IndexHash", "");

            if (string.IsNullOrEmpty(savedHash) || savedHash != currentRulesHash)
            {
                return;
            }

            var eventIndexJson = PlayerPrefs.GetString("AppFig_EventIndex", "");
            var userPropIndexJson = PlayerPrefs.GetString("AppFig_UserPropIndex", "");
            var devicePropIndexJson = PlayerPrefs.GetString("AppFig_DeviceIndex", "");

            eventToFeaturesIndex = DeserializeIndex(eventIndexJson);
            userPropertyToFeaturesIndex = DeserializeIndex(userPropIndexJson);
            devicePropertyToFeaturesIndex = DeserializeIndex(devicePropIndexJson);

        }
        catch (Exception e)
        {
            Debug.LogWarning($"[AppFig] Failed to load index from PlayerPrefs: {e.Message}");
            eventToFeaturesIndex.Clear();
            userPropertyToFeaturesIndex.Clear();
            devicePropertyToFeaturesIndex.Clear();
        }
    }

    private static Dictionary<string, HashSet<string>> DeserializeIndex(string serialized)
    {
        var result = new Dictionary<string, HashSet<string>>();

        if (string.IsNullOrEmpty(serialized))
            return result;

        var pairs = serialized.Split('|');
        foreach (var pair in pairs)
        {
            var parts = pair.Split(':');
            if (parts.Length == 2)
            {
                var key = parts[0];
                var features = parts[1].Split(',');
                result[key] = new HashSet<string>(features);
            }
        }

        return result;
    }

    private static void EvaluateAllFeatures()
    {

        if (rules.Count == 0)
        {
            return;
        }


        // Evaluate each feature using the index for O(1) rule lookup
        foreach (var kvp in featureToRulesIndex)
        {
            string feature = kvp.Key;
            List<Rule> featureRules = kvp.Value;

            string oldValue = features.ContainsKey(feature) ? features[feature] : null;
            string newValue = null;

            // Find first matching rule for this feature (no linear search needed!)
            foreach (var rule in featureRules)
            {
                if (EvaluateConditions(rule.conditions))
                {
                    newValue = rule.value ?? "on";
                    break;
                }
            }

            // Set feature value (null if no match)
            if (newValue != null)
            {
                features[feature] = newValue;
                if (oldValue != newValue)
                {
                    Log(AppFigLogLevel.Debug, $"Feature updated: {feature} = {newValue}");
                }
            }
            else
            {
                features[feature] = newValue;
                Log(AppFigLogLevel.Debug, $"Feature cleared: {feature}");
            }

            // Notify listeners only if value changed
            if (oldValue != newValue)
            {
                NotifyFeatureListeners(feature, newValue);
            }
        }

        // Remove orphaned features not in current rules
        List<string> toRemove = new List<string>();
        foreach (var key in features.Keys)
        {
            if (!featureToRulesIndex.ContainsKey(key))
            {
                toRemove.Add(key);
            }
        }
        foreach (var key in toRemove)
        {
            features.Remove(key);
        }
    }

    private static void NotifyFeatureListeners(string featureName, string newValue)
    {
        if (featureListeners.ContainsKey(featureName))
        {
            foreach (var listener in featureListeners[featureName])
            {
                try
                {
                    listener?.Invoke(featureName, newValue);
                }
                catch (System.Exception e)
                {
                    Log(AppFigLogLevel.Error, "Conditions could not be evaluated");
                }
            }
        }
    }

    private static bool EvaluateConditions(RuleConditions group)
    {
        if (group == null)
        {
            return true;
        }

        bool eventResult = MatchEventConditions(group.events);
        bool userResult = MatchUserConditions(group.user_properties, group.user_properties_operator ?? "AND");
        bool deviceResult = MatchDeviceConditions(group.device, group.device_operator ?? "AND");

        return eventResult &&
               userResult &&
               deviceResult;
    }

    private static bool MatchEventConditions(EventsConfig eventsConfig)
    {
        if (eventsConfig == null || eventsConfig.events == null || eventsConfig.events.Count == 0)
        {
            return true;
        }

        if (eventsConfig.mode == "simple")
        {
            return MatchSimpleEvents(eventsConfig.events, eventsConfig.@operator ?? "AND");
        }
        else if (eventsConfig.mode == "sequence")
        {
            return MatchSequenceEvents(eventsConfig.events, eventsConfig.ordering ?? "direct");
        }

        return true;
    }

    private static bool MatchSimpleEvents(List<EventCondition> list, string logicalOperator)
    {
        if (list == null || list.Count == 0)
        {
            return true;
        }

        if (logicalOperator == "OR")
        {
            foreach (var condition in list)
            {
                bool result = EvaluateEvent(condition);
                if (condition.not) result = !result;
                if (result)
                {
                    return true;
                }
            }
            return false;
        }
        else
        {
            foreach (var condition in list)
            {
                bool result = EvaluateEvent(condition);
                if (condition.not) result = !result;
                if (!result)
                {
                    return false;
                }
            }
            return true;
        }
    }

    private static bool MatchSequenceEvents(List<EventCondition> steps, string ordering)
    {
        if (steps == null || steps.Count == 0)
        {
            return true;
        }

        if (ordering == "direct")
        {
            return MatchDirectSequence(steps);
        }
        else
        {
            return MatchIndirectSequence(steps);
        }
    }

    private static bool MatchDirectSequence(List<EventCondition> steps)
    {

        // Calculate minimum events needed for this sequence with overflow protection
        int minEventsNeeded = 0;
        foreach (var step in steps)
        {
            int stepMinCount = 1;
            if (step.count != null && !string.IsNullOrEmpty(step.count.@operator))
            {
                // Validate count value to prevent overflow
                int safeCountValue = Math.Max(0, Math.Min(step.count.value, 100000));

                // For ==, we need exactly that many. For >=, we need at least that many. For >, we need at least value+1
                if (step.count.@operator == "==")
                {
                    stepMinCount = safeCountValue;
                }
                else if (step.count.@operator == ">=")
                {
                    stepMinCount = safeCountValue;
                }
                else if (step.count.@operator == ">")
                {
                    // Prevent overflow on addition
                    stepMinCount = safeCountValue < int.MaxValue ? safeCountValue + 1 : int.MaxValue;
                }
                else if (step.count.@operator == "<=" || step.count.@operator == "<")
                {
                    stepMinCount = 1; // For <= and <, we need at least 1
                }
            }

            // Check for overflow before adding
            if (minEventsNeeded > int.MaxValue - stepMinCount)
            {
                minEventsNeeded = int.MaxValue;
                break;
            }
            minEventsNeeded += stepMinCount;
        }


        // For direct sequence, we need to find a consecutive run of events that match all steps
        for (int startIdx = 0; startIdx <= eventHistory.Count - minEventsNeeded; startIdx++)
        {
            bool sequenceMatched = true;
            int eventIdx = startIdx;


            for (int stepIdx = 0; stepIdx < steps.Count; stepIdx++)
            {
                var step = steps[stepIdx];
                int matchedCount = 0;

                // Determine how many events we need/can consume based on the count operator
                string countOperator = step.count != null && !string.IsNullOrEmpty(step.count.@operator) ? step.count.@operator : ">=";
                int countValue = step.count != null ? step.count.value : 1;
                int maxEventsToConsume = int.MaxValue;

                // For "==" operator, we should consume exactly the required count
                // For "<=" operator, we should consume at most the required count
                // For "<" operator, we should consume at most (value - 1)
                // For ">=" and ">" operators, we consume as many as available (greedy)
                if (countOperator == "==")
                {
                    maxEventsToConsume = countValue;
                }
                else if (countOperator == "<=")
                {
                    maxEventsToConsume = countValue;
                }
                else if (countOperator == "<")
                {
                    maxEventsToConsume = countValue - 1;
                }
                else
                {
                }


                // Try to match consecutive events for this step
                while (eventIdx < eventHistory.Count)
                {
                    var evt = eventHistory[eventIdx];

                    // Use operator for event name matching (default to "==" if not specified)
                    string stepOperator = string.IsNullOrEmpty(step.@operator) ? "==" : step.@operator;
                    if (CompareValue(evt.name, stepOperator, step.key))
                    {
                        // Check time window FIRST before other checks
                        bool withinTimeWindow = true;
                        if (step.within_last_days > 0)
                        {
                            // Validate days to prevent overflow (max 365 as per config)
                            int safeDays = Math.Min(step.within_last_days, 365);
                            DateTime cutoff = DateTime.UtcNow.AddDays(-safeDays);
                            if (evt.timestamp < cutoff)
                            {
                                withinTimeWindow = false;
                            }
                        }

                        // Only check params if within time window
                        bool stepMatch = CheckStepMatch(evt, step);

                        if (withinTimeWindow && stepMatch)
                        {
                            matchedCount++;
                            eventIdx++;

                            // Check if we've consumed enough events based on the operator
                            if (matchedCount >= maxEventsToConsume)
                            {
                                break;
                            }
                        }
                        else
                        {
                            // Event name matches but either outside time window or params don't match
                            // Stop counting for this step
                            break;
                        }
                    }
                    else
                    {
                        // Different event type stops this step's matching
                        break;
                    }
                }


                // Check if the count condition is satisfied
                // Note: countOperator and countValue are already declared at the beginning of this step
                bool countSatisfied = CompareCount(matchedCount, countOperator, countValue);

                if (!countSatisfied)
                {
                    sequenceMatched = false;
                    break;
                }

            }

            if (sequenceMatched)
            {
                return true;
            }
            else
            {
            }
        }

        return false;
    }

    private static bool MatchIndirectSequence(List<EventCondition> steps)
    {

        int lastMatchedIndex = -1; // Track the index of the last matched event to enforce ordering

        for (int stepIndex = 0; stepIndex < steps.Count; stepIndex++)
        {
            var step = steps[stepIndex];

            // Count how many events of this type we need
            if (step.count != null && !string.IsNullOrEmpty(step.count.@operator))
            {
                // For indirect sequence with count, we count matching events after the last matched index
                string op = step.count.@operator;
                int threshold = step.count.value;

                int matchedCount = 0;
                int latestMatchIndex = lastMatchedIndex;

                for (int i = lastMatchedIndex + 1; i < eventHistory.Count; i++)
                {
                    var evt = eventHistory[i];
                    string stepOperator = string.IsNullOrEmpty(step.@operator) ? "==" : step.@operator;
                    if (CompareValue(evt.name, stepOperator, step.key) && CheckStepMatch(evt, step))
                    {
                        if (step.within_last_days > 0)
                        {
                            // Validate days to prevent overflow (max 365 as per config)
                            int safeDays = Math.Min(step.within_last_days, 365);
                            DateTime cutoff = DateTime.UtcNow.AddDays(-safeDays);
                            if (evt.timestamp < cutoff)
                            {
                                continue;
                            }
                        }
                        matchedCount++;
                        latestMatchIndex = i;
                    }
                }

                bool countMatched = CompareCount(matchedCount, op, threshold);

                if (!countMatched)
                {
                    return false;
                }

                lastMatchedIndex = latestMatchIndex;
            }
            else
            {
                // No count condition, just check if at least one matching event exists after the last matched index
                bool stepMatched = false;
                for (int i = lastMatchedIndex + 1; i < eventHistory.Count; i++)
                {
                    var evt = eventHistory[i];
                    string stepOperator = string.IsNullOrEmpty(step.@operator) ? "==" : step.@operator;
                    if (CompareValue(evt.name, stepOperator, step.key) && CheckStepMatch(evt, step))
                    {
                        if (step.within_last_days > 0)
                        {
                            // Validate days to prevent overflow (max 365 as per config)
                            int safeDays = Math.Min(step.within_last_days, 365);
                            DateTime cutoff = DateTime.UtcNow.AddDays(-safeDays);
                            if (evt.timestamp < cutoff)
                            {
                                continue;
                            }
                        }

                        stepMatched = true;
                        lastMatchedIndex = i;
                        break;
                    }
                }

                if (!stepMatched)
                {
                    return false;
                }
            }
        }

        return true;
    }

    private static bool CheckStepMatch(EventRecord evt, EventCondition step)
    {

        if (step.param != null)
        {
            foreach (var paramCondition in step.param)
            {
                if (!evt.parameters.TryGetValue(paramCondition.Key, out string actual) ||
                    !CompareValue(actual, paramCondition.Value.@operator, paramCondition.Value.value))
                {
                    return false;
                }
            }
        }
        else
        {
        }
        return true;
    }

    private static bool EvaluateEvent(EventCondition cond)
    {
        if (string.IsNullOrEmpty(cond.key))
        {
            return false;
        }

        // Use operator for event name matching (default to "==" if not specified)
        string eventOperator = string.IsNullOrEmpty(cond.@operator) ? "==" : cond.@operator;
        var matched = eventHistory.FindAll(e => CompareValue(e.name, eventOperator, cond.key));

        if (cond.within_last_days > 0)
        {
            // Validate days to prevent overflow (max 365 as per config)
            int safeDays = Math.Min(cond.within_last_days, 365);
            DateTime cutoff = DateTime.UtcNow.AddDays(-safeDays);
            matched = matched.FindAll(e => e.timestamp >= cutoff);
        }

        if (cond.count != null && !string.IsNullOrEmpty(cond.count.@operator))
        {
            bool countPassed = CompareCount(matched.Count, cond.count.@operator, cond.count.value);
            if (!countPassed)
            {
                return false;
            }
        }


        if (cond.param != null)
        {
            bool any = false;
            foreach (var e in matched)
            {
                bool allParamsMatch = true;
                foreach (var paramCondition in cond.param)
                {
                    if (!e.parameters.TryGetValue(paramCondition.Key, out string actual) ||
                        !CompareValue(actual, paramCondition.Value.@operator, paramCondition.Value.value))
                    {
                        allParamsMatch = false;
                        break;
                    }
                }
                if (allParamsMatch)
                {
                    any = true;
                    break;
                }
            }
            if (!any)
            {
                return false;
            }
            else
            {
            }
        }

        // If we have a count condition, it must have passed (checked above)
        // If we have param conditions, they must have passed (checked above)
        // Otherwise, we just need at least one matching event
        bool result = matched.Count > 0;
        return result;
    }

    private static bool MatchUserConditions(List<UserOrDeviceCondition> list, string logicalOperator)
    {
        if (list == null || list.Count == 0) return true;


        if (logicalOperator == "OR")
        {
            foreach (var cond in list)
            {
                bool result = EvaluateKeyVal(userProperties, cond);
                if (cond.not) result = !result;
                if (result)
                {
                    return true;
                }
            }
            return false;
        }
        else
        {
            foreach (var cond in list)
            {
                bool result = EvaluateKeyVal(userProperties, cond);
                if (cond.not) result = !result;
                if (!result)
                {
                    return false;
                }
            }
            return true;
        }
    }

    private static bool MatchDeviceConditions(List<UserOrDeviceCondition> list, string logicalOperator)
    {
        if (list == null || list.Count == 0) return true;


        if (logicalOperator == "OR")
        {
            foreach (var cond in list)
            {
                bool result = EvaluateKeyVal(deviceProperties, cond);
                if (cond.not) result = !result;
                if (result)
                {
                    return true;
                }
            }
            return false;
        }
        else
        {
            foreach (var cond in list)
            {
                bool result = EvaluateKeyVal(deviceProperties, cond);
                if (cond.not) result = !result;
                if (!result)
                {
                    return false;
                }
            }
            return true;
        }
    }

    private static bool EvaluateKeyVal(Dictionary<string, string> source, UserOrDeviceCondition cond)
    {
        if (cond == null)
        {
            return false;
        }

        if (cond.value == null)
        {
            return false;
        }

        if (string.IsNullOrEmpty(cond.value.@operator))
        {
            return false;
        }

        if (cond.value.value == null)
        {
            Log(AppFigLogLevel.Error, "Invalid condition format");
            return false;
        }

        if (!source.TryGetValue(cond.key, out string actual))
        {
            Debug.LogWarning($"[AppFig] Device/user property '{cond.key}' not set. Rule condition will fail until property is available.");
            return false;
        }

        bool result = CompareValue(actual, cond.value.@operator, cond.value.value);
        return result;
    }

    private static bool CompareCount(int a, string op, int b) => op switch
    {
        "==" => a == b,
        ">" => a > b,
        ">=" => a >= b,
        "<" => a < b,
        "<=" => a <= b,
        _ => false
    };

    private static bool CompareValue(string actual, string op, string expected)
    {
        string expectedStr = expected ?? "";

        switch (op)
        {
            case "==":
                return actual == expectedStr;
            case "!=":
                return actual != expectedStr;
            case "==_ci": // case-insensitive equals
                return string.Equals(actual, expectedStr, StringComparison.OrdinalIgnoreCase);
            case "!=_ci": // case-insensitive not equals
                return !string.Equals(actual, expectedStr, StringComparison.OrdinalIgnoreCase);
            case ">":
                if (double.TryParse(actual, out double actualNum) && double.TryParse(expectedStr, out double expectedNum))
                    return actualNum > expectedNum;
                return string.Compare(actual, expectedStr, StringComparison.Ordinal) > 0;
            case "<":
                if (double.TryParse(actual, out actualNum) && double.TryParse(expectedStr, out expectedNum))
                    return actualNum < expectedNum;
                return string.Compare(actual, expectedStr, StringComparison.Ordinal) < 0;
            case ">=":
                if (double.TryParse(actual, out actualNum) && double.TryParse(expectedStr, out expectedNum))
                    return actualNum >= expectedNum;
                return string.Compare(actual, expectedStr, StringComparison.Ordinal) >= 0;
            case "<=":
                if (double.TryParse(actual, out actualNum) && double.TryParse(expectedStr, out expectedNum))
                    return actualNum <= expectedNum;
                return string.Compare(actual, expectedStr, StringComparison.Ordinal) <= 0;
            case "in":
                return Array.Exists(expectedStr.Split(','), v => string.Equals(v.Trim(), actual, StringComparison.OrdinalIgnoreCase));
            case "not_in":
                return !Array.Exists(expectedStr.Split(','), v => string.Equals(v.Trim(), actual, StringComparison.OrdinalIgnoreCase));
            case "contains":
                return actual.IndexOf(expectedStr, StringComparison.OrdinalIgnoreCase) >= 0;
            case "contains_ci": // case-insensitive contains (legacy)
                return actual.IndexOf(expectedStr, StringComparison.OrdinalIgnoreCase) >= 0;
            case "starts_with":
                return actual.StartsWith(expectedStr, StringComparison.OrdinalIgnoreCase);
            case "starts_with_ci": // case-insensitive starts with (legacy)
                return actual.StartsWith(expectedStr, StringComparison.OrdinalIgnoreCase);
            case "ends_with":
                return actual.EndsWith(expectedStr, StringComparison.OrdinalIgnoreCase);
            case "ends_with_ci": // case-insensitive ends with (legacy)
                return actual.EndsWith(expectedStr, StringComparison.OrdinalIgnoreCase);
            case "regex":
                try
                {
                    return System.Text.RegularExpressions.Regex.IsMatch(actual, expectedStr);
                }
                catch
                {
                    Debug.LogWarning($"[AppFig] Invalid regex pattern: {expectedStr}");
                    return false;
                }
            case "version_gt": // semantic version greater than
                return CompareSemanticVersion(actual, expectedStr) > 0;
            case "version_gte": // semantic version greater than or equal
                return CompareSemanticVersion(actual, expectedStr) >= 0;
            case "version_lt": // semantic version less than
                return CompareSemanticVersion(actual, expectedStr) < 0;
            case "version_lte": // semantic version less than or equal
                return CompareSemanticVersion(actual, expectedStr) <= 0;
            case "version_eq": // semantic version equals
                return CompareSemanticVersion(actual, expectedStr) == 0;
            default:
                return false;
        }
    }

    private static int CompareSemanticVersion(string version1, string version2)
    {
        try
        {
            var v1Parts = version1.Split('.');
            var v2Parts = version2.Split('.');

            int maxLength = Math.Max(v1Parts.Length, v2Parts.Length);

            for (int i = 0; i < maxLength; i++)
            {
                int v1Part = i < v1Parts.Length && int.TryParse(v1Parts[i], out int p1) ? p1 : 0;
                int v2Part = i < v2Parts.Length && int.TryParse(v2Parts[i], out int p2) ? p2 : 0;

                if (v1Part != v2Part)
                    return v1Part.CompareTo(v2Part);
            }

            return 0;
        }
        catch
        {
            Debug.LogWarning($"[AppFig] Failed to parse semantic versions: '{version1}' vs '{version2}'");
            return string.Compare(version1, version2, StringComparison.Ordinal);
        }
    }

    // =====================================================================================
    // Session-Persistent Caching (PlayerPrefs)
    // =====================================================================================

    /// <summary>
    /// Generate cache key for PlayerPrefs storage using company ID, tenant ID, and environment
    /// </summary>
    private static string GetCacheKey(string companyId, string tenantId, string env, string suffix)
    {
        return $"AppFig_Cache_{companyId}_{tenantId}_{env}_{suffix}";
    }

    private static bool LoadCachedRules(string companyId, string tenantId, string env)
    {
        try
        {
            string rulesKey = GetCacheKey(companyId, tenantId, env, "Rules");
            string hashKey = GetCacheKey(companyId, tenantId, env, "Hash");
            string timestampKey = GetCacheKey(companyId, tenantId, env, "Timestamp");

            if (!PlayerPrefs.HasKey(rulesKey) || !PlayerPrefs.HasKey(timestampKey))
            {
                return false;
            }

            string cachedRules = PlayerPrefs.GetString(rulesKey);
            string cachedHash = PlayerPrefs.GetString(hashKey);
            string timestampStr = PlayerPrefs.GetString(timestampKey);

            if (string.IsNullOrEmpty(cachedRules) || string.IsNullOrEmpty(timestampStr))
            {
                return false;
            }

            // Parse timestamp
            if (!DateTime.TryParse(timestampStr, out DateTime cachedTime))
            {
                Debug.LogWarning("[AppFig] Failed to parse cached timestamp");
                return false;
            }

            lastFetchTime = cachedTime;

            ParseRules(cachedRules);

            // Log sample feature values
            int sampleCount = 0;
            foreach (var kvp in features)
            {
                if (sampleCount++ < 5)
                {
                }
            }

            // Fire onReady callback for cached rules
            onReadyCallback?.Invoke();

            return true;
        }
        catch (Exception e)
        {
            Debug.LogWarning($"[AppFig] Failed to load cached rules: {e.Message}");
            return false;
        }
    }

    private static void SaveCachedRules(string companyId, string tenantId, string env, string rulesJson, string hash)
    {
        try
        {
            string rulesKey = GetCacheKey(companyId, tenantId, env, "Rules");
            string hashKey = GetCacheKey(companyId, tenantId, env, "Hash");
            string timestampKey = GetCacheKey(companyId, tenantId, env, "Timestamp");

            PlayerPrefs.SetString(rulesKey, rulesJson);
            PlayerPrefs.SetString(hashKey, hash);
            PlayerPrefs.SetString(timestampKey, DateTime.UtcNow.ToString("o"));
            PlayerPrefs.Save();

        }
        catch (Exception e)
        {
            Debug.LogWarning($"[AppFig] Failed to save cached rules: {e.Message}");
        }
    }

    private static void SaveCacheTimestamp(string companyId, string tenantId, string env)
    {
        try
        {
            string timestampKey = GetCacheKey(companyId, tenantId, env, "Timestamp");
            PlayerPrefs.SetString(timestampKey, DateTime.UtcNow.ToString("o"));
            PlayerPrefs.Save();
        }
        catch (Exception e)
        {
            Debug.LogWarning($"[AppFig] Failed to save cache timestamp: {e.Message}");
        }
    }

    private static string GetCachedHash(string companyId, string tenantId, string env)
    {
        try
        {
            string hashKey = GetCacheKey(companyId, tenantId, env, "Hash");
            if (PlayerPrefs.HasKey(hashKey))
            {
                return PlayerPrefs.GetString(hashKey);
            }
        }
        catch (Exception e)
        {
            Debug.LogWarning($"[AppFig] Failed to get cached hash: {e.Message}");
        }
        return null;
    }

    /// <summary>
    /// Clear cached rules for specified company ID, tenant ID, and environment
    /// </summary>
    public static void ClearCache(string companyId, string tenantId, string env)
    {
        try
        {
            PlayerPrefs.DeleteKey(GetCacheKey(companyId, tenantId, env, "Rules"));
            PlayerPrefs.DeleteKey(GetCacheKey(companyId, tenantId, env, "Hash"));
            PlayerPrefs.DeleteKey(GetCacheKey(companyId, tenantId, env, "Timestamp"));
            PlayerPrefs.Save();
        }
        catch (Exception e)
        {
            Debug.LogWarning($"[AppFig] Failed to clear cache: {e.Message}");
        }
    }

    // =====================================================================================
    // EVENT PERSISTENCE (PlayerPrefs)
    // =====================================================================================

    private static void LoadCachedEvents(string companyId, string tenantId, string env)
    {
        try
        {
            string eventsKey = GetCacheKey(companyId, tenantId, env, "Events");

            if (!PlayerPrefs.HasKey(eventsKey))
            {
                return;
            }

            string eventsJson = PlayerPrefs.GetString(eventsKey);
            if (string.IsNullOrEmpty(eventsJson))
            {
                return;
            }

            var wrapper = JsonUtility.FromJson<EventRecordListWrapper>(eventsJson);
            if (wrapper != null && wrapper.events != null)
            {
                eventHistory = wrapper.events;

                // Log sample events
                int sampleCount = 0;
                foreach (var evt in eventHistory)
                {
                    if (sampleCount++ < 3)
                    {
                    }
                }
            }
            else
            {
            }
        }
        catch (Exception e)
        {
            Debug.LogWarning($"[AppFig] Failed to load cached events: {e.Message}\n{e.StackTrace}");
        }
    }

    private static IEnumerator DebounceSaveEvents()
    {
        yield return new WaitForSeconds(5f);
        eventsSavedCount = 0;
        eventSaveDebounceCoroutine = null;
        SaveCachedEvents();
    }

    private static void SaveCachedEvents()
    {
        try
        {
            // Use the stored companyId and tenantId
            string company = companyId;
            string tenant = tenantId;
            string env = environment;

            if (string.IsNullOrEmpty(company) || string.IsNullOrEmpty(tenant) || string.IsNullOrEmpty(env))
            {
                Debug.LogWarning("[AppFig] Cannot save events: missing companyId/tenantId/environment info");
                return;
            }

            string eventsKey = GetCacheKey(company, tenant, env, "Events");
            var wrapper = new EventRecordListWrapper { events = eventHistory };
            string eventsJson = JsonUtility.ToJson(wrapper);

            PlayerPrefs.SetString(eventsKey, eventsJson);
            PlayerPrefs.Save();


        }
        catch (Exception e)
        {
            Debug.LogWarning($"[AppFig] Failed to save cached events: {e.Message}\n{e.StackTrace}");
        }
    }

    private static void EnforceEventRetention()
    {
        // Remove events older than maxEventAgeDays
        int safeDays = Math.Min(maxEventAgeDays, 365);
        TimeSpan maxAge = TimeSpan.FromDays(safeDays);
        DateTime cutoffTime = DateTime.UtcNow - maxAge;

        int beforeCount = eventHistory.Count;
        eventHistory.RemoveAll(e => e.timestamp < cutoffTime);
        int removedByAge = beforeCount - eventHistory.Count;

        // Trim to maxEvents (keep most recent)
        if (eventHistory.Count > maxEvents)
        {
            int toRemove = eventHistory.Count - maxEvents;
            eventHistory.RemoveRange(0, toRemove);
        }

        if (removedByAge > 0)
        {
        }
    }

    public static void ClearEventHistory()
    {
        eventHistory.Clear();
        eventCounts.Clear();
        SaveCachedEvents();

        // Re-evaluate all features with empty event history
        EvaluateAllFeatures();
    }

    /// <summary>
    /// Clear all cached data including events, properties, and features
    /// This is a hard reset useful for testing scenarios
    /// Note: This is destructive and will remove all persistent data
    /// </summary>
    public static void ClearAllData()
    {
        // Clear runtime state
        eventHistory.Clear();
        eventCounts.Clear();
        features.Clear();
        userProperties.Clear();
        deviceProperties.Clear();

        // Clear persistent cache
        SaveCachedEvents();

        // Reset session
        sessionActive = false;
        currentScreen = "";
        lastActivity = DateTime.UtcNow;

        // Re-evaluate all features
        EvaluateAllFeatures();

    }

    public static Dictionary<string, object> GetEventHistoryStats()
    {
        var stats = new Dictionary<string, object>
        {
            { "count", eventHistory.Count },
            { "oldestEvent", eventHistory.Count > 0 ? eventHistory[0].timestamp : DateTime.MinValue },
            { "newestEvent", eventHistory.Count > 0 ? eventHistory[eventHistory.Count - 1].timestamp : DateTime.MinValue }
        };
        return stats;
    }

    // MonoBehaviour runner for coroutines
    private class AppFigRunner : MonoBehaviour { }

    // Wrapper class for JSON serialization of event list
    [Serializable]
    private class EventRecordListWrapper
    {
        public List<EventRecord> events;
    }

    // Data Classes
    [Serializable]
    public class EventRecord
    {
        public string name;
        public long timestampTicks;
        public Dictionary<string, string> parameters;

        // Non-serialized property for convenience
        public DateTime timestamp
        {
            get { return new DateTime(timestampTicks, DateTimeKind.Utc); }
            set { timestampTicks = value.Ticks; }
        }
    }
    [Serializable] public class Rule { public string feature; public string value; public RuleConditions conditions; }
    [Serializable] public class RuleConditions
    {
        public EventsConfig events;
        public List<UserOrDeviceCondition> user_properties;
        public string user_properties_operator;
        public List<UserOrDeviceCondition> device;
        public string device_operator;
    }

    [Serializable] public class EventsConfig
    {
        public string mode;
        public string @operator;
        public string ordering;
        public List<EventCondition> events;
    }

    [Serializable] public class EventCondition
    {
        public string key;
        public string @operator;
        public CountOperator count;
        public Dictionary<string, OperatorValue> param;
        public int within_last_days;
        public bool not;
    }

    // Schema Discovery
    private static void InitSchemaDiscovery()
    {
        // Generate stable device ID
        string storedDeviceId = PlayerPrefs.GetString("AppFig_DeviceId", "");
        if (!string.IsNullOrEmpty(storedDeviceId))
        {
            deviceId = storedDeviceId;
        }
        else
        {
            deviceId = $"{DateTime.UtcNow.Ticks}-{UnityEngine.Random.Range(100000, 999999)}";
            PlayerPrefs.SetString("AppFig_DeviceId", deviceId);
            PlayerPrefs.Save();
        }

        // Load cached schema and last upload time
        LoadCachedSchema();
        string lastUploadStr = PlayerPrefs.GetString("AppFig_LastSchemaUpload", "0");
        if (long.TryParse(lastUploadStr, out long lastUpload))
        {
            lastSchemaUploadTime = lastUpload;
        }

        // 1% deterministic sampling based on device ID hash
        bool shouldSample = IsInSample();
        if (!shouldSample)
        {
            Log(AppFigLogLevel.Debug, "Schema discovery: device not in 1% sample");
        }
    }

    private static bool IsInSample()
    {
        // Simple hash of device ID for 1% sampling
        // Using unchecked arithmetic to prevent overflow exceptions
        int hash = 5381; // DJB2 hash initial value
        unchecked
        {
            foreach (char c in deviceId)
            {
                hash = ((hash << 5) + hash) + c; // hash * 33 + c
            }
        }
        // Handle Int32.MinValue case for Math.Abs
        int absHash = hash == int.MinValue ? int.MaxValue : Math.Abs(hash);
        return absHash % 100 < 1; // 1% sample
    }

    private static string MaskPII(object value)
    {
        string str = value?.ToString() ?? "";

        // Mask emails
        if (str.Contains("@") && str.Contains("."))
        {
            return "[email]";
        }

        // Truncate long strings (likely IDs or sensitive data)
        if (str.Length > 50)
        {
            return str.Substring(0, 50) + "...";
        }

        return str;
    }

    private static void TrackEventSchema(string eventName, Dictionary<string, object> parameters)
    {
        if (!IsInSample()) return;

        // Track event name
        if (!schemaEvents.Contains(eventName))
        {
            schemaDiffNewEvents.Add(eventName);
            schemaEvents.Add(eventName);
        }

        // Track event parameters
        if (parameters != null && parameters.Count > 0)
        {
            if (!schemaEventParams.ContainsKey(eventName))
            {
                schemaEventParams[eventName] = new HashSet<string>();
            }
            if (!schemaEventParamValues.ContainsKey(eventName))
            {
                schemaEventParamValues[eventName] = new Dictionary<string, HashSet<string>>();
            }

            HashSet<string> cachedParams = schemaEventParams[eventName];
            Dictionary<string, HashSet<string>> cachedParamValues = schemaEventParamValues[eventName];

            if (!schemaDiffNewEventParams.ContainsKey(eventName))
            {
                schemaDiffNewEventParams[eventName] = new SchemaEventParamData();
            }

            var bufferEntry = schemaDiffNewEventParams[eventName];

            foreach (var param in parameters)
            {
                // Track parameter name
                if (!cachedParams.Contains(param.Key))
                {
                    bufferEntry.Params.Add(param.Key);
                    cachedParams.Add(param.Key);
                }

                // Track parameter value (limit to 20 unique values)
                string maskedValue = MaskPII(param.Value);
                if (!cachedParamValues.ContainsKey(param.Key))
                {
                    cachedParamValues[param.Key] = new HashSet<string>();
                }
                HashSet<string> valueSet = cachedParamValues[param.Key];

                if (!valueSet.Contains(maskedValue) && valueSet.Count < 20)
                {
                    if (!bufferEntry.ParamValues.ContainsKey(param.Key))
                    {
                        bufferEntry.ParamValues[param.Key] = new HashSet<string>();
                    }
                    bufferEntry.ParamValues[param.Key].Add(maskedValue);
                    valueSet.Add(maskedValue);
                }
            }

            schemaDiffNewEventParams[eventName] = bufferEntry;
        }

        DebounceSchemaUpload();
    }

    private static void TrackUserPropertySchema(Dictionary<string, object> props)
    {
        if (!IsInSample()) return;

        foreach (var prop in props)
        {
            string maskedValue = MaskPII(prop.Value);

            if (!schemaUserProperties.ContainsKey(prop.Key))
            {
                schemaUserProperties[prop.Key] = new HashSet<string>();
            }
            HashSet<string> valueSet = schemaUserProperties[prop.Key];

            if (!valueSet.Contains(maskedValue) && valueSet.Count < 20)
            {
                if (!schemaDiffNewUserProperties.ContainsKey(prop.Key))
                {
                    schemaDiffNewUserProperties[prop.Key] = new HashSet<string>();
                }
                schemaDiffNewUserProperties[prop.Key].Add(maskedValue);
                valueSet.Add(maskedValue);
            }
        }

        DebounceSchemaUpload();
    }

    private static void TrackDevicePropertySchema(Dictionary<string, object> props)
    {
        if (!IsInSample()) return;

        foreach (var prop in props)
        {
            string maskedValue = MaskPII(prop.Value);

            if (!schemaDeviceProperties.ContainsKey(prop.Key))
            {
                schemaDeviceProperties[prop.Key] = new HashSet<string>();
            }
            HashSet<string> valueSet = schemaDeviceProperties[prop.Key];

            if (!valueSet.Contains(maskedValue) && valueSet.Count < 20)
            {
                if (!schemaDiffNewDeviceProperties.ContainsKey(prop.Key))
                {
                    schemaDiffNewDeviceProperties[prop.Key] = new HashSet<string>();
                }
                schemaDiffNewDeviceProperties[prop.Key].Add(maskedValue);
                valueSet.Add(maskedValue);
            }
        }

        DebounceSchemaUpload();
    }

    private static void DebounceSchemaUpload()
    {
        // Skip if not in 1% sample
        if (!IsInSample()) return;

        // Throttle: skip if uploaded within last 12 hours
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (now - lastSchemaUploadTime < SCHEMA_UPLOAD_THROTTLE_MS)
        {
            Log(AppFigLogLevel.Debug, "Schema upload throttled (12-hour limit)");
            return;
        }

        schemaUploadCount++;

        // Upload immediately if batch size reached
        if (schemaUploadCount >= SCHEMA_UPLOAD_BATCH_SIZE)
        {
            schemaUploadCount = 0;
            if (schemaUploadCoroutine != null)
            {
                runner.StopCoroutine(schemaUploadCoroutine);
                schemaUploadCoroutine = null;
            }
            runner.StartCoroutine(UploadSchemaDiff());
            return;
        }

        // Otherwise, debounce for interval
        if (schemaUploadCoroutine != null)
        {
            runner.StopCoroutine(schemaUploadCoroutine);
        }
        schemaUploadCoroutine = runner.StartCoroutine(SchemaUploadCoroutine());
    }

    private static IEnumerator SchemaUploadCoroutine()
    {
        yield return new WaitForSeconds(SCHEMA_UPLOAD_INTERVAL_MS / 1000f);
        schemaUploadCount = 0;
        schemaUploadCoroutine = null;
        yield return UploadSchemaDiff();
    }

    private static IEnumerator UploadSchemaDiff()
    {
        // Skip if buffer is empty
        if (schemaDiffNewEvents.Count == 0 &&
            schemaDiffNewEventParams.Count == 0 &&
            schemaDiffNewUserProperties.Count == 0 &&
            schemaDiffNewDeviceProperties.Count == 0)
        {
            yield break;
        }

        // Build payload
        var payload = new Dictionary<string, object>
        {
            { "tenant_id", tenantId },
            { "env", environment }
        };

        if (schemaDiffNewEvents.Count > 0)
        {
            payload["new_events"] = new List<string>(schemaDiffNewEvents);
        }

        if (schemaDiffNewEventParams.Count > 0)
        {
            var eventParams = new Dictionary<string, object>();
            foreach (var kvp in schemaDiffNewEventParams)
            {
                eventParams[kvp.Key] = new Dictionary<string, object>
                {
                    { "params", new List<string>(kvp.Value.Params) },
                    { "param_values", kvp.Value.ParamValues.ToDictionary(
                        p => p.Key,
                        p => (object)new List<string>(p.Value)
                    )}
                };
            }
            payload["new_event_params"] = eventParams;
        }

        if (schemaDiffNewUserProperties.Count > 0)
        {
            var userProps = new Dictionary<string, object>();
            foreach (var kvp in schemaDiffNewUserProperties)
            {
                userProps[kvp.Key] = new List<string>(kvp.Value);
            }
            payload["new_user_properties"] = userProps;
        }

        if (schemaDiffNewDeviceProperties.Count > 0)
        {
            var deviceProps = new Dictionary<string, object>();
            foreach (var kvp in schemaDiffNewDeviceProperties)
            {
                deviceProps[kvp.Key] = new List<string>(kvp.Value);
            }
            payload["new_device_properties"] = deviceProps;
        }

        // Send to collectMeta endpoint
        string functionUrl = "https://us-central1-appfig-dev.cloudfunctions.net/collectMeta";
        string jsonPayload = JsonUtility.ToJson(new Wrapper { data = payload });

        UnityWebRequest www = new UnityWebRequest(functionUrl, "POST");
        byte[] bodyRaw = System.Text.Encoding.UTF8.GetBytes(jsonPayload);
        www.uploadHandler = new UploadHandlerRaw(bodyRaw);
        www.downloadHandler = new DownloadHandlerBuffer();
        www.SetRequestHeader("Content-Type", "application/json");
        www.SetRequestHeader("X-API-Key", apiKey);
        www.timeout = 30;

        yield return www.SendWebRequest();

        if (www.result == UnityWebRequest.Result.Success)
        {
            Log(AppFigLogLevel.Debug, "Schema uploaded successfully");

            // Update last upload time
            lastSchemaUploadTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            PlayerPrefs.SetString("AppFig_LastSchemaUpload", lastSchemaUploadTime.ToString());
            PlayerPrefs.Save();

            // Clear buffer
            schemaDiffNewEvents.Clear();
            schemaDiffNewEventParams.Clear();
            schemaDiffNewUserProperties.Clear();
            schemaDiffNewDeviceProperties.Clear();

            // Save updated schema cache
            SaveCachedSchema();
        }
        else
        {
            Log(AppFigLogLevel.Warn, $"Schema upload failed: {www.responseCode}");
        }
    }

    private static void LoadCachedSchema()
    {
        try
        {
            string schemaKey = $"{companyId}_{tenantId}_{environment}_Schema";
            string cached = PlayerPrefs.GetString(schemaKey, "");
            if (!string.IsNullOrEmpty(cached))
            {
                var data = JsonUtility.FromJson<SchemaCache>(cached);
                schemaEvents = new HashSet<string>(data.events != null ? data.events : new string[0]);

                schemaEventParams = new Dictionary<string, HashSet<string>>();
                if (data.eventParams != null)
                {
                    foreach (var ep in data.eventParams)
                    {
                        schemaEventParams[ep.eventName] = new HashSet<string>(ep.parameters);
                    }
                }

                // Note: EventParamValues not cached to save space
            }
        }
        catch (Exception e)
        {
        }
    }

    private static void SaveCachedSchema()
    {
        try
        {
            string schemaKey = $"{companyId}_{tenantId}_{environment}_Schema";

            var eventParamsList = new List<EventParams>();
            foreach (var kvp in schemaEventParams)
            {
                eventParamsList.Add(new EventParams
                {
                    eventName = kvp.Key,
                    parameters = new List<string>(kvp.Value)
                });
            }

            var cache = new SchemaCache
            {
                events = new List<string>(schemaEvents),
                eventParams = eventParamsList
            };

            string json = JsonUtility.ToJson(cache);
            PlayerPrefs.SetString(schemaKey, json);
            PlayerPrefs.Save();
        }
        catch (Exception e)
        {
        }
    }

    [Serializable]
    private class SchemaCache
    {
        public List<string> events;
        public List<EventParams> eventParams;
    }

    [Serializable]
    private class EventParams
    {
        public string eventName;
        public List<string> parameters;
    }

    [Serializable]
    private class Wrapper
    {
        public Dictionary<string, object> data;
    }

    [Serializable] public class CountOperator { public string @operator; public int value; }
    [Serializable] public class OperatorValue { public string @operator; public string value; }
    [Serializable] public class UserOrDeviceCondition { public string key; public OperatorValue value; public bool not; }

    // Unity-compatible wrapper classes for JSON parsing
    [Serializable]
    public class FeatureWrapper
    {
        public List<FeatureData> features;
    }

    [Serializable]
    public class FeatureData
    {
        public string featureName;
        public List<Rule> rules;
    }

    private static IEnumerator FetchPointer(Action<string> onSuccess, Action onFail)
    {

        UnityWebRequest req = UnityWebRequest.Get(pointerUrl);

        // Also disable Unity's internal caching
        req.disposeDownloadHandlerOnDispose = true;
        req.disposeCertificateHandlerOnDispose = true;
        req.disposeUploadHandlerOnDispose = true;

        yield return req.SendWebRequest();

        if (req.result != UnityWebRequest.Result.Success)
        {
            onFail?.Invoke();
            yield break;
        }

        // Check cache status from response headers
        string cfCacheStatus = req.GetResponseHeader("CF-Cache-Status");
        string age = req.GetResponseHeader("Age");
        if (!string.IsNullOrEmpty(cfCacheStatus))
        {
        }

        // Extract Country header from CDN response
        string countryHeader = req.GetResponseHeader("Country");
        if (!string.IsNullOrEmpty(countryHeader))
        {
            SetDeviceProperty("country", countryHeader);
        }

        string pointerJson = req.downloadHandler.text;

        onSuccess?.Invoke(pointerJson);
    }
}