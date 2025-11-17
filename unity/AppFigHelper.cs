using UnityEngine;
using System.Collections.Generic;

/// <summary>
/// AppFigHelper - Convenience wrapper for AppFig SDK initialization and event logging
///
/// This component provides:
/// - Inspector-based configuration (no hardcoded credentials)
/// - Singleton pattern for easy global access
/// - Automatic SDK initialization on scene start
/// - Convenient methods for logging events and managing properties
///
/// INDEPENDENCE NOTE:
/// - AppFigApplier and AppFigABTester do NOT require this component
/// - They work directly with AppFig SDK after AppFig.Init() is called
/// - Use AppFigHelper for convenient initialization and event logging
/// - Or call AppFig.Init() directly in your own scripts
///
/// USAGE:
/// 1. Attach this component to a GameObject in your first scene
/// 2. Configure Company ID, Tenant ID, and Environment in the Inspector
/// 3. Access from anywhere: AppFigHelper.Instance.LogEvent("button_clicked")
///
/// EXAMPLE:
/// // Log events
/// AppFigHelper.Instance.LogEvent("level_started", new Dictionary<string, string> {
///     {"level_id", "5"},
///     {"difficulty", "hard"}
/// });
///
/// // Manage properties
/// AppFigHelper.Instance.SetUserProperty("player_level", "10");
/// AppFigHelper.Instance.SetDeviceProperty("platform", "iOS");
/// </summary>
public class AppFigHelper : MonoBehaviour
{
    #region Singleton Pattern

    private static AppFigHelper _instance;

    /// <summary>
    /// Singleton instance for global access
    /// </summary>
    public static AppFigHelper Instance => _instance;

    #endregion

    #region Inspector Configuration

    [Header("AppFig Mode")]
    [Tooltip("Use local JSON file instead of remote server (free plan)")]
    [SerializeField] private bool useLocalMode = false;

    [Header("Remote Configuration (Paid Plan)")]
    [Tooltip("Your Firestore company document ID (e.g., 'acmegames'). This is the document ID from Firestore, NOT the display name. Cannot contain spaces.")]
    [SerializeField] private string companyId = "appfigdev";

    [Tooltip("Your Firestore tenant document ID (e.g., 'spaceshooter'). This is the document ID from Firestore, NOT the display name. Cannot contain spaces.")]
    [SerializeField] private string tenantId = "your-tenant-id";

    [Tooltip("Environment: dev, staging, or production")]
    [SerializeField] private string environment = "dev";

    [Tooltip("API key for authentication (required for remote mode)")]
    [SerializeField] private string apiKey = "";

    [Header("Advanced Remote Settings")]
    [Tooltip("Enable automatic background rule updates (default: true)")]
    [SerializeField] private bool autoRefresh = true;

    [Tooltip("Auto-refresh interval in milliseconds (default: 43200000 = 12 hours)")]
    [SerializeField] private long pollInterval = 43200000;

    [Tooltip("Session timeout in milliseconds (default: 1800000 = 30 minutes, range: 60000-7200000)")]
    [SerializeField] private long sessionTimeoutMs = 1800000;

    [Tooltip("Maximum events to store in history (default: 5000, range: 100-100000)")]
    [SerializeField] private int maxEvents = 5000;

    [Tooltip("Maximum age of events in days (default: 7, range: 1-365)")]
    [SerializeField] private int maxEventAgeDays = 7;

    [Header("Local Configuration (Free Plan)")]
    [Tooltip("Optional: Custom path to rules JSON in Resources folder (without .json extension). Leave empty to use default 'appfig_rules'. Example: 'custom_rules' for Assets/Resources/custom_rules.json")]
    [SerializeField] private string localRulesPath = "";

    [Header("General Settings")]
    [Tooltip("Enable debug logging for AppFigHelper and SDK operations")]
    [SerializeField] private string debugMode = "false";

    #endregion

    #region Private Fields

    private bool isInitialized = false;

    #endregion

    #region Inspector Buttons (Editor Only)

#if UNITY_EDITOR
    [Header("Development Tools")]
    [SerializeField] private bool showDevTools = false;

    private void OnValidate()
    {
        // This method exists to make the custom editor draw the button
    }
#endif

    #endregion

    #region Unity Lifecycle

    private void Awake()
    {
        // Enforce singleton pattern
        if (_instance != null && _instance != this)
        {
            Debug.LogWarning("[AppFigHelper] Multiple AppFigHelper instances detected. Destroying duplicate.");
            Destroy(gameObject);
            return;
        }

        _instance = this;
        DontDestroyOnLoad(gameObject);
    }

    private void Start()
    {
        InitializeAppFig();
    }

    #endregion

    #region Initialization

    /// <summary>
    /// Initializes the AppFig SDK with configured settings
    /// </summary>
    private void InitializeAppFig()
    {
        if (isInitialized)
        {
            LogDebug("AppFig already initialized");
            return;
        }

        if (useLocalMode)
        {
            // Initialize SDK in local mode
            if (string.IsNullOrEmpty(localRulesPath))
            {
                // Use default path
                AppFig.InitLocal();
                LogDebug("AppFig initialized in LOCAL MODE with default rules path: Resources/appfig_rules.json");
            }
            else
            {
                // Use custom path
                AppFig.InitLocal(localRulesPath);
                LogDebug($"AppFig initialized in LOCAL MODE with custom rules path: Resources/{localRulesPath}.json");
            }
            isInitialized = true;
        }
        else
        {
            // Validate remote configuration
            if (string.IsNullOrEmpty(companyId) || string.IsNullOrEmpty(tenantId))
            {
                Debug.LogError("[AppFigHelper] Company ID and Tenant ID must be set in the Inspector!");
                Debug.LogError("[AppFigHelper] These should be Firestore document IDs, not display names.");
                return;
            }

            // Validate ID formats
            if (companyId.Contains(" "))
            {
                Debug.LogError($"[AppFigHelper] Invalid Company ID '{companyId}' - IDs cannot contain spaces. Use the Firestore document ID (e.g., 'acmegames'), not the display name (e.g., 'Acme Games Inc.').");
                return;
            }

            if (tenantId.Contains(" "))
            {
                Debug.LogError($"[AppFigHelper] Invalid Tenant ID '{tenantId}' - IDs cannot contain spaces. Use the Firestore document ID (e.g., 'spaceshooter'), not the display name (e.g., 'Space Shooter Pro').");
                return;
            }

            if (string.IsNullOrEmpty(apiKey))
            {
                Debug.LogError("[AppFigHelper] API Key is required for remote mode!");
                Debug.LogError("[AppFigHelper] Use Local Mode for development without an API key.");
                return;
            }

            // Parse debug mode
            bool debugEnabled = false;
            if (!string.IsNullOrEmpty(debugMode))
            {
                if (debugMode.Trim().ToLower() == "true" || debugMode.Trim() == "1")
                {
                    debugEnabled = true;
                }
            }

            // Initialize SDK in remote mode with all parameters
            AppFig.Init(
                companyId: companyId,
                tenantId: tenantId,
                env: environment,
                apiKey: apiKey,
                autoRefresh: autoRefresh,
                pollInterval: pollInterval,
                debugMode: debugEnabled,
                sessionTimeoutMs: sessionTimeoutMs,
                maxEvents: maxEvents,
                maxEventAgeDays: maxEventAgeDays
            );
            isInitialized = true;

            LogDebug($"AppFig initialized in REMOTE MODE with companyId={companyId}, tenantId={tenantId}, env={environment}, autoRefresh={autoRefresh}, pollInterval={pollInterval}ms, debugMode={debugEnabled}, sessionTimeout={sessionTimeoutMs}ms, maxEvents={maxEvents}, maxEventAgeDays={maxEventAgeDays}");
        }
    }

    #endregion

    #region Event Logging

    /// <summary>
    /// Log an event to AppFig
    /// </summary>
    public void LogEvent(string eventName)
    {
        AppFig.LogEvent(eventName);
        LogDebug($"Event logged: {eventName}");
    }

    /// <summary>
    /// Log an event with parameters to AppFig
    /// </summary>
    public void LogEvent(string eventName, Dictionary<string, string> parameters)
    {
        AppFig.LogEvent(eventName, parameters);
        LogDebug($"Event logged: {eventName} with {parameters.Count} parameters");
    }

    /// <summary>
    /// Log an event with a single string parameter to AppFig.
    /// All values are stored as strings internally - the SDK automatically handles numeric comparisons.
    /// Example: LogEvent("level_complete", "level", "5") can be compared with "level > 3" in Rule Builder.
    /// </summary>
    /// <param name="eventName">Name of the event</param>
    /// <param name="paramKey">Parameter key</param>
    /// <param name="paramValue">Parameter value as string</param>
    public void LogEvent(string eventName, string paramKey, string paramValue)
    {
        var parameters = new Dictionary<string, string> { { paramKey, paramValue } };
        AppFig.LogEvent(eventName, parameters);
        LogDebug($"Event logged: {eventName} with parameter {paramKey} = {paramValue}");
    }

    /// <summary>
    /// Log an event with a single integer parameter to AppFig.
    /// The integer is automatically converted to a string for storage.
    /// The SDK's CompareValue method will parse it back to a number when using numeric operators (>, <, >=, <=).
    /// Example: LogEvent("level_complete", "level", 5) works with "level > 3" comparison in Rule Builder.
    /// </summary>
    /// <param name="eventName">Name of the event</param>
    /// <param name="paramKey">Parameter key</param>
    /// <param name="paramValue">Parameter value as integer (will be converted to string)</param>
    public void LogEvent(string eventName, string paramKey, int paramValue)
    {
        var parameters = new Dictionary<string, string> { { paramKey, paramValue.ToString() } };
        AppFig.LogEvent(eventName, parameters);
        LogDebug($"Event logged: {eventName} with parameter {paramKey} = {paramValue}");
    }

    /// <summary>
    /// Log an event with a single float parameter to AppFig.
    /// The float is automatically converted to a string for storage.
    /// The SDK's CompareValue method will parse it back to a number when using numeric operators (>, <, >=, <=).
    /// Example: LogEvent("purchase", "amount", 9.99f) works with "amount >= 5.0" comparison in Rule Builder.
    /// </summary>
    /// <param name="eventName">Name of the event</param>
    /// <param name="paramKey">Parameter key</param>
    /// <param name="paramValue">Parameter value as float (will be converted to string)</param>
    public void LogEvent(string eventName, string paramKey, float paramValue)
    {
        var parameters = new Dictionary<string, string> { { paramKey, paramValue.ToString() } };
        AppFig.LogEvent(eventName, parameters);
        LogDebug($"Event logged: {eventName} with parameter {paramKey} = {paramValue}");
    }

    /// <summary>
    /// Log an event with a single boolean parameter to AppFig.
    /// The boolean is automatically converted to a string ("True" or "False") for storage.
    /// Use string comparison operators in Rule Builder: success == "True" or success != "False"
    /// Example: LogEvent("level_complete", "success", true) works with "success == True" comparison.
    /// </summary>
    /// <param name="eventName">Name of the event</param>
    /// <param name="paramKey">Parameter key</param>
    /// <param name="paramValue">Parameter value as boolean (will be converted to string "True" or "False")</param>
    public void LogEvent(string eventName, string paramKey, bool paramValue)
    {
        var parameters = new Dictionary<string, string> { { paramKey, paramValue.ToString() } };
        AppFig.LogEvent(eventName, parameters);
        LogDebug($"Event logged: {eventName} with parameter {paramKey} = {paramValue}");
    }

    #endregion

    #region Property Management

    /// <summary>
    /// Set a user property
    /// </summary>
    public void SetUserProperty(string key, string value)
    {
        AppFig.SetUserProperty(key, value);
        LogDebug($"User property set: {key} = {value}");
    }

    /// <summary>
    /// Set a device property
    /// </summary>
    public void SetDeviceProperty(string key, string value)
    {
        AppFig.SetDeviceProperty(key, value);
        LogDebug($"Device property set: {key} = {value}");
    }

    #endregion


    #region Utility Methods

    /// <summary>
    /// Refresh rules from the server
    /// </summary>
    public void RefreshRules()
    {
        AppFig.RefreshRules();
        LogDebug("Rules refresh requested");
    }

    /// <summary>
    /// Check if AppFig SDK has been initialized
    /// </summary>
    /// <returns>True if SDK is ready to use</returns>
    public bool IsInitialized()
    {
        return isInitialized;
    }

    /// <summary>
    /// Get a feature value from AppFig SDK
    /// This is a convenience method - you can also call AppFig.GetFeatureValue() directly
    /// </summary>
    /// <param name="featureName">Name of the feature</param>
    /// <returns>Feature value as string, or null if not found</returns>
    public string GetFeatureValue(string featureName)
    {
        return AppFig.GetFeatureValue(featureName);
    }

    /// <summary>
    /// Clear all event history. Useful for testing and debugging.
    /// This will remove all logged events from memory and persistent cache.
    /// </summary>
    public void ClearEventHistory()
    {
        AppFig.ClearEventHistory();
        LogDebug("Event history cleared");
    }

    /// <summary>
    /// Clear all cached data including events, properties, and features.
    /// This is a hard reset useful for testing scenarios.
    /// WARNING: This is destructive and will remove all persistent data.
    /// </summary>
    public void ClearAllData()
    {
        AppFig.ClearAllData();
        LogDebug("All data cleared - hard reset complete");
    }

    /// <summary>
    /// Get event history statistics
    /// </summary>
    /// <returns>Dictionary with count, oldestEvent, and newestEvent</returns>
    public Dictionary<string, object> GetEventHistoryStats()
    {
        return AppFig.GetEventHistoryStats();
    }

    /// <summary>
    /// Internal debug logging
    /// </summary>
    private void LogDebug(string message)
    {
        // Parse debug mode setting
        bool debugEnabled = false;
        if (!string.IsNullOrEmpty(debugMode))
        {
            if (debugMode.Trim().ToLower() == "true" || debugMode.Trim() == "1")
            {
                debugEnabled = true;
            }
        }

        if (debugEnabled)
        {
            Debug.Log($"[AppFigHelper] {message}");
        }
    }

    #endregion

    #region Unity Editor Buttons

#if UNITY_EDITOR
    /// <summary>
    /// Unity Editor button to clear event history (for testing)
    /// </summary>
    [ContextMenu("Clear Event History")]
    private void EditorClearEventHistory()
    {
        ClearEventHistory();
        UnityEngine.Debug.Log("[AppFigHelper] Event history cleared via context menu");
    }

    /// <summary>
    /// Unity Editor button to show event history stats
    /// </summary>
    [ContextMenu("Show Event History Stats")]
    private void EditorShowEventStats()
    {
        var stats = GetEventHistoryStats();
        UnityEngine.Debug.Log("=== AppFig Event History Stats ===");
        UnityEngine.Debug.Log($"Total Events: {stats["count"]}");
        UnityEngine.Debug.Log($"Oldest Event: {stats["oldestEvent"]}");
        UnityEngine.Debug.Log($"Newest Event: {stats["newestEvent"]}");
        UnityEngine.Debug.Log("===================================");
    }

    /// <summary>
    /// Unity Editor button to clear all data (hard reset)
    /// </summary>
    [ContextMenu("Clear All Data (Hard Reset)")]
    private void EditorClearAllData()
    {
        ClearAllData();
        UnityEngine.Debug.Log("[AppFigHelper] All data cleared - hard reset complete");
    }
#endif

    #endregion
}

#if UNITY_EDITOR
/// <summary>
/// Custom Inspector editor for AppFigHelper component
/// Adds developer tools buttons for testing and debugging
/// </summary>
[UnityEditor.CustomEditor(typeof(AppFigHelper))]
public class AppFigHelperEditor : UnityEditor.Editor
{
    public override void OnInspectorGUI()
    {
        // Draw the default inspector
        DrawDefaultInspector();

        AppFigHelper helper = (AppFigHelper)target;

        // Add some space before the buttons
        UnityEditor.EditorGUILayout.Space(10);

        // Developer Tools Section
        UnityEditor.EditorGUILayout.LabelField("Developer Tools", UnityEditor.EditorStyles.boldLabel);

        // Check if in Play mode
        bool isPlaying = UnityEngine.Application.isPlaying;
        bool isInitialized = isPlaying && helper.IsInitialized();

        if (!isPlaying)
        {
            UnityEditor.EditorGUILayout.HelpBox(
                "Developer Tools are only available in Play Mode. Enter Play Mode to use these tools.",
                UnityEditor.MessageType.Warning);
        }
        else if (!isInitialized)
        {
            UnityEditor.EditorGUILayout.HelpBox(
                "AppFig SDK is not yet initialized. Wait for initialization to complete.",
                UnityEditor.MessageType.Warning);
        }
        else
        {
            UnityEditor.EditorGUILayout.HelpBox(
                "Use these tools to test and debug event-based rules during development.",
                UnityEditor.MessageType.Info);
        }

        UnityEditor.EditorGUILayout.Space(5);

        // Disable buttons if not in play mode or not initialized
        UnityEngine.GUI.enabled = isInitialized;

        // Create a horizontal layout for buttons
        UnityEditor.EditorGUILayout.BeginHorizontal();

        // Clear Event History Button
        if (UnityEngine.GUILayout.Button("Clear Event History", UnityEngine.GUILayout.Height(30)))
        {
            if (UnityEditor.EditorUtility.DisplayDialog(
                "Clear Event History",
                "This will remove all logged events from memory and persistent cache. Continue?",
                "Clear",
                "Cancel"))
            {
                try
                {
                    helper.ClearEventHistory();
                    UnityEditor.EditorUtility.DisplayDialog("Success", "Event history cleared successfully!", "OK");
                }
                catch (System.Exception e)
                {
                    UnityEditor.EditorUtility.DisplayDialog(
                        "Error",
                        $"Failed to clear event history:\n{e.Message}",
                        "OK");
                    UnityEngine.Debug.LogError($"[AppFigHelper] Clear event history failed: {e}");
                }
            }
        }

        // Show Stats Button
        if (UnityEngine.GUILayout.Button("Show Stats", UnityEngine.GUILayout.Height(30)))
        {
            try
            {
                var stats = helper.GetEventHistoryStats();
                string message = $"Total Events: {stats["count"]}\n" +
                               $"Oldest Event: {stats["oldestEvent"]}\n" +
                               $"Newest Event: {stats["newestEvent"]}";
                UnityEditor.EditorUtility.DisplayDialog("Event History Stats", message, "OK");
            }
            catch (System.Exception e)
            {
                UnityEditor.EditorUtility.DisplayDialog(
                    "Error",
                    $"Failed to retrieve event statistics:\n{e.Message}",
                    "OK");
                UnityEngine.Debug.LogError($"[AppFigHelper] Get event stats failed: {e}");
            }
        }

        UnityEditor.EditorGUILayout.EndHorizontal();

        UnityEditor.EditorGUILayout.Space(5);

        // Clear All Data Button (full width, warning color)
        UnityEngine.GUI.backgroundColor = new UnityEngine.Color(1f, 0.6f, 0.6f);
        if (UnityEngine.GUILayout.Button("Clear All Data (Hard Reset)", UnityEngine.GUILayout.Height(30)))
        {
            if (UnityEditor.EditorUtility.DisplayDialog(
                "Clear All Data - Hard Reset",
                "WARNING: This will remove ALL data including events, properties, features, and cached rules.\n\nThis is a destructive operation for testing purposes only.\n\nContinue?",
                "Clear All Data",
                "Cancel"))
            {
                try
                {
                    helper.ClearAllData();
                    UnityEditor.EditorUtility.DisplayDialog("Success", "All data cleared - hard reset complete!", "OK");
                }
                catch (System.Exception e)
                {
                    UnityEditor.EditorUtility.DisplayDialog(
                        "Error",
                        $"Failed to clear all data:\n{e.Message}",
                        "OK");
                    UnityEngine.Debug.LogError($"[AppFigHelper] Clear all data failed: {e}");
                }
            }
        }
        UnityEngine.GUI.backgroundColor = UnityEngine.Color.white;

        // Re-enable GUI
        UnityEngine.GUI.enabled = true;

        UnityEditor.EditorGUILayout.Space(5);

        // Information box
        UnityEditor.EditorGUILayout.HelpBox(
            "Clear Event History: Remove all logged events (useful for testing sequences)\n" +
            "Show Stats: Display event count and timestamps\n" +
            "Clear All Data: Hard reset - removes ALL data including properties and features (destructive)",
            UnityEditor.MessageType.None);
    }
}
#endif
