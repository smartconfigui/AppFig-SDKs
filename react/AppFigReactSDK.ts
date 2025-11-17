// AppFigReactSDK.ts

export type AppFigEvent = {
  name: string;
  params?: Record<string, any>;
  timestamp: number;
};

export type Condition = {
  key: string;
  operator?: string;
  count?: {
    operator: string;
    value: number;
  };
  repeat?: {
    operator: string;
    value: number;
  };
  param?: Record<
    string,
    {
      operator: string;
      value: any;
    }
  >;
  within_last_days?: number;
  not?: boolean;
  value?: {
    operator: string;
    value: any;
  };
};

export type EventsConfig = {
  mode?: 'simple' | 'sequence';
  operator?: 'AND' | 'OR';
  ordering?: 'direct' | 'indirect';
  events?: Condition[];
};

export type AppFigRule = {
  conditions: {
    events: Condition[] | EventsConfig;
    user_properties: Condition[];
    device: Condition[];
    user_properties_operator?: 'AND' | 'OR';
    device_operator?: 'AND' | 'OR';
  };
  sequential?: boolean;
  value: any;
};

export type AppFigInitParams = {
  companyId: string;
  tenantId: string;
  env: 'dev' | 'prod';
  apiKey: string;
  pollInterval?: number; // milliseconds (default: 12 hours for Growth, 24 hours for Starter)
  autoUpdate?: boolean; // @deprecated Use autoRefresh instead
  autoRefresh?: boolean; // enabled by default now
  debugMode?: boolean;
  sessionTimeoutMs?: number; // session timeout in milliseconds (default: 30 minutes, range: 1 min - 2 hours)
  maxEvents?: number; // max events to store (default: 5000, max: 100000)
  maxEventAgeDays?: number; // max event age in days (default: 7, max: 365)
  onReady?: () => void; // callback fired when SDK is ready (once with cached rules, again with fresh rules)
  onRulesUpdated?: () => void; // callback fired when rules are updated from CDN
  // Legacy params (will be removed)
  userId?: string;
  rulesOverride?: Record<string, AppFigRule[]>;
};

class AppFigCore {
  private rules: Record<string, AppFigRule[]> = {};
  private userId: string = '';
  private eventHistory: AppFigEvent[] = [];
  private userProperties: Record<string, any> = {};
  private deviceProperties: Record<string, any> = {};
  private lastFetched: number = 0;
  private cdnBaseUrl: string = '';
  private pointerUrl: string = '';
  private autoRefresh: boolean = true; // Enabled by default
  private pollInterval: number = 43200000; // 12 hours default (Growth plan)
  private lastFetchTime: number = 0;
  private currentHash: string = '';
  private isFetchInProgress: boolean = false;
  private readonly THREADING_THRESHOLD_KB = 500; // 500 KB threshold for web worker parsing
  private companyId: string = '';
  private tenantId: string = '';
  private env: string = 'prod';
  private apiKey: string = '';
  private refreshInterval: NodeJS.Timeout | null = null;
  private sessionTimeout: NodeJS.Timeout | null = null;
  private lastActivity: number = Date.now();
  private sessionActive: boolean = false;
  private sessionTimeoutMs: number = 30 * 60 * 1000; // 30 minutes default
  private currentScreen: string = '';
  private debugMode: boolean = false;
  private maxEvents: number = 5000; // Default: 5000 events
  private maxEventAgeDays: number = 7; // Default: 7 days
  private eventSaveDebounceTimeout: NodeJS.Timeout | null = null;
  private eventsSavedCount: number = 0;
  private timeDecayIntervalId: NodeJS.Timeout | null = null;

  // Callbacks and listeners
  private onReadyCallback?: () => void;
  private onRulesUpdatedCallback?: () => void;
  private featureListeners: Map<string, Set<(featureName: string, value: any) => void>> = new Map();
  private features: Map<string, any> = new Map();
  private eventToFeaturesIndex: Map<string, Set<string>> = new Map();
  private userPropertyToFeaturesIndex: Map<string, Set<string>> = new Map();
  private devicePropertyToFeaturesIndex: Map<string, Set<string>> = new Map();
  private featureToRulesIndex: Map<string, AppFigRule[]> = new Map();

  private log(level: 'INFO' | 'WARN' | 'ERROR' | 'DEBUG', message: string) {
    // Errors and warnings always show
    if (level === 'WARN' || level === 'ERROR') {
      if (level === 'ERROR') {
        console.error(`[AppFig] ${message}`);
      } else {
        console.warn(`[AppFig] ${message}`);
      }
      return;
    }

    // Info + Debug only show when debugMode=true
    if (!this.debugMode) return;

    if (level === 'INFO') {
      console.log(`[AppFig] ${message}`);
    } else {
      console.log(`[AppFig:Debug] ${message}`);
    }
  }

  async init(params: AppFigInitParams) {
    this.userId = params.userId;
    this.companyId = params.companyId;
    this.tenantId = params.tenantId;
    this.env = params.env;
    this.apiKey = params.apiKey;

    // Handle autoRefresh with backward compatibility for autoUpdate
    if (params.autoRefresh !== undefined) {
      this.autoRefresh = params.autoRefresh;
    } else if (params.autoUpdate !== undefined) {
      this.autoRefresh = params.autoUpdate; // Backward compatibility
      this.log('WARN', 'autoUpdate is deprecated, use autoRefresh instead');
    } else {
      this.autoRefresh = true; // Enabled by default
    }

    this.pollInterval = params.pollInterval ?? 43200000; // 12 hours default
    this.debugMode = params.debugMode ?? false;
    this.onReadyCallback = params.onReady;
    this.onRulesUpdatedCallback = params.onRulesUpdated;

    // Validate and set session timeout
    this.sessionTimeoutMs = params.sessionTimeoutMs ?? (30 * 60 * 1000); // 30 minutes default
    const MIN_SESSION_TIMEOUT = 60000; // 1 minute
    const MAX_SESSION_TIMEOUT = 7200000; // 2 hours
    if (this.sessionTimeoutMs < MIN_SESSION_TIMEOUT || this.sessionTimeoutMs > MAX_SESSION_TIMEOUT) {
      this.log('WARN', `Session timeout ${this.sessionTimeoutMs}ms out of range. Clamping to ${MIN_SESSION_TIMEOUT}-${MAX_SESSION_TIMEOUT}ms`);
      this.sessionTimeoutMs = Math.max(MIN_SESSION_TIMEOUT, Math.min(MAX_SESSION_TIMEOUT, this.sessionTimeoutMs));
    }
    // Session timeout configured

    // Setup CDN URLs
    this.cdnBaseUrl = 'https://rules-prod.appfig.com';
    this.pointerUrl = `${this.cdnBaseUrl}/rules_versions/${params.companyId}/${params.tenantId}/${params.env}/current/latest.json`;

    // Validate pollInterval (min 1 minute, max 24 hours)
    const MIN_POLL_INTERVAL = 60000; // 1 minute
    const MAX_POLL_INTERVAL = 86400000; // 24 hours
    if (this.pollInterval < MIN_POLL_INTERVAL || this.pollInterval > MAX_POLL_INTERVAL) {
      this.log('WARN', `Poll interval ${this.pollInterval}ms out of range. Clamping to ${MIN_POLL_INTERVAL}-${MAX_POLL_INTERVAL}ms`);
      this.pollInterval = Math.max(MIN_POLL_INTERVAL, Math.min(MAX_POLL_INTERVAL, this.pollInterval));
    }
    // Poll interval configured

    // Validate and set event retention parameters
    this.maxEvents = params.maxEvents ?? 5000;
    this.maxEventAgeDays = params.maxEventAgeDays ?? 7;

    const MIN_MAX_EVENTS = 100;
    const MAX_MAX_EVENTS = 100000;
    const MIN_EVENT_AGE_DAYS = 1;
    const MAX_EVENT_AGE_DAYS = 365;

    if (this.maxEvents < MIN_MAX_EVENTS || this.maxEvents > MAX_MAX_EVENTS) {
      this.log('WARN', `maxEvents ${this.maxEvents} out of range. Clamping to ${MIN_MAX_EVENTS}-${MAX_MAX_EVENTS}`);
      this.maxEvents = Math.max(MIN_MAX_EVENTS, Math.min(MAX_MAX_EVENTS, this.maxEvents));
    }

    if (this.maxEventAgeDays < MIN_EVENT_AGE_DAYS || this.maxEventAgeDays > MAX_EVENT_AGE_DAYS) {
      this.log('WARN', `maxEventAgeDays ${this.maxEventAgeDays} out of range. Clamping to ${MIN_EVENT_AGE_DAYS}-${MAX_EVENT_AGE_DAYS}`);
      this.maxEventAgeDays = Math.max(MIN_EVENT_AGE_DAYS, Math.min(MAX_EVENT_AGE_DAYS, this.maxEventAgeDays));
    }

    if (this.maxEvents > 10000) {
      this.log('WARN', `Large event limit (${this.maxEvents}) may impact memory and storage on mobile devices.`);
    }

    // Event retention configured

    // Load cached events from IndexedDB
    await this.loadCachedEvents();

    // Detect platform automatically
    const platform = this.detectPlatform();
    const language = this.detectLanguage();
    const timezone = this.detectTimezone();
    const osVersion = this.detectOSVersion();
    const deviceBrand = this.detectDeviceBrand();
    const deviceModel = this.detectDeviceModel();

    this.deviceProperties = {
      platform,
      language,
      timezone,
      os_version: osVersion,
      device_brand: deviceBrand,
      device_model: deviceModel
    };

    // Load cached rules (DO NOT evaluate yet)
    await this.loadCachedRules();

    // Fetch pointer to check for updates
    await this.pollRules();

    // Start auto-refresh after first fetch completes
    if (this.autoRefresh) {
      this.log('INFO', 'Initializing AppFig');
      this.startAutoRefresh();
    }

    // Optional: Support manual rules override for testing
    if (params.rulesOverride) {
      this.log('INFO', 'Local mode active');
      this.rules = params.rulesOverride;
      }

    // Start automatic session tracking
    this.initSessionTracking();

    // Initialize schema discovery
    this.initSchemaDiscovery();

    // Start periodic time-decay evaluation
    this.startTimeDecayLoop();

    // Final initialization summary
    this.log('INFO', 'SDK ready');
  }


  logEvent(name: string, params?: Record<string, any>) {
    const event: AppFigEvent = { name, params, timestamp: Date.now() };
    this.eventHistory.push(event);

    // Track schema discovery
    this.trackEventSchema(name, params);

    // Enforce retention limits
    this.enforceEventRetention();

    // Debounce saving events (batch save every 10 events or 5 seconds)
    this.debounceSaveEvents();

    // Re-evaluate all features since events changed
    this.evaluateAllFeatures();
  }

  /**
   * Get feature value (from pre-evaluated features Map)
   * Features are automatically evaluated on init and when events/properties change
   */
  getConfig(flagName: string): any {

    // Check if rules need refreshing (automatic updating)
    if (this.shouldPoll()) {
      this.log('INFO', 'Fetching latest rules');
      // Trigger background refresh (non-blocking)
      this.pollRules().catch(err => {
        this.log('WARN', 'Failed to fetch remote rules – using cached version');
      });
    }

    // Return pre-evaluated feature value
    // Features are evaluated by evaluateAllFeatures() which runs on:
    // - Initial load
    // - Rule updates
    // - Event logs
    // - Property changes
    if (this.features.has(flagName)) {
      return this.features.get(flagName);
    }

    // Feature not in index (doesn't exist in rules)
    return null;
  }

  setUserProperties(props: Record<string, any>) {
    this.userProperties = { ...this.userProperties, ...props };
    this.trackUserPropertySchema(props);
    this.evaluateAllFeatures();
  }

  setDeviceProperties(props: Record<string, any>) {
    this.deviceProperties = { ...this.deviceProperties, ...props };
    this.trackDevicePropertySchema(props);
    this.evaluateAllFeatures();
  }

  setAppVersion(version: string) {
    this.deviceProperties = { ...this.deviceProperties, app_version: version };
    this.evaluateAllFeatures();
  }

  /**
   * Reset a specific feature's cached value and force re-evaluation
   * Useful for implementing recurring triggers (e.g., "show popup every 3 events")
   *
   * Example usage:
   * ```typescript
   * // Check if feature is enabled
   * if (appfig.check("level_complete_popup")) {
   *   showPopup();
   *   // Reset the feature so it can trigger again after next 3 events
   *   appfig.resetFeature("level_complete_popup");
   * }
   * ```
   *
   * @param featureName - Name of the feature to reset
   */
  resetFeature(featureName: string): void {
    if (Object.keys(this.rules).length === 0) {
      this.log('WARN', 'Cannot reset feature: No rules loaded');
      return;
    }

    this.log('DEBUG', `Resetting feature: ${featureName}`);

    // Store old value for comparison
    const oldValue = this.features.get(featureName);

    // Clear cached value
    this.features.delete(featureName);

    // Re-evaluate the specific feature immediately
    const featureRules = this.featureToRulesIndex.get(featureName);
    if (!featureRules) {
      this.features.set(featureName, null);
      this.log('DEBUG', `Feature value after reset: ${featureName} = null (no rules)`);
      return;
    }

    let newValue: any = null;
    for (const rule of featureRules) {
      if (this.evaluateConditions(rule.conditions)) {
        newValue = rule.value ?? 'on';
        break;
      }
    }

    // Update features map with new evaluation
    this.features.set(featureName, newValue);

    // Notify listeners if value changed
    if (oldValue !== newValue) {
      this.log('DEBUG', `Feature value changed after reset: ${featureName} = ${newValue}`);
      this.notifyListeners(new Set([featureName]));
    } else {
      this.log('DEBUG', `Feature value unchanged after reset: ${featureName} = ${newValue}`);
    }
  }

  /**
   * Reset all features and force complete re-evaluation
   * This clears all cached feature values and re-evaluates all rules
   *
   * Use this sparingly - typically you want to reset specific features using resetFeature()
   */
  resetAllFeatures(): void {
    if (Object.keys(this.rules).length === 0) {
      this.log('WARN', 'Cannot reset features: No rules loaded');
      return;
    }

    this.log('DEBUG', 'Resetting all features');

    // Clear all caches
    this.features.clear();

    // Re-evaluate all features
    this.evaluateAllFeatures();

    this.log('DEBUG', 'All features reset and re-evaluated');
  }

  /**
   * @deprecated Country is now automatically detected from CDN response headers during rules fetch.
   * This fallback method is kept for backward compatibility.
   * Detect and set country code using static endpoint
   * This method makes a request to detect the user's country and automatically sets it as a device property
   *
   * @returns Promise that resolves with country code (or null on failure)
   */
  async detectAndSetCountry(): Promise<string | null> {
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000); // 5 second timeout

      const res = await fetch('https://rules-dev.appfig.com/rules_versions/country/country/dev/current/latest.json', {
        signal: controller.signal
      });
      clearTimeout(timeoutId);

      if (!res.ok) {
        return null;
      }

      const countryHeader = res.headers.get('Country');
      if (countryHeader) {
        this.setDeviceProperties({ country: countryHeader });
        return countryHeader;
      } else {
        return null;
      }
    } catch (e) {
      return null;
    }
  }

  getEventHistory(): AppFigEvent[] {
    return this.eventHistory;
  }

  getDebugInfo() {
    return {
      rules: this.rules,
      events: this.eventHistory,
      userProperties: this.userProperties,
      deviceProperties: this.deviceProperties,
      features: Object.fromEntries(this.features), // Convert Map to object for easier inspection
    };
  }

  private startAutoRefresh() {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
    }

    if (this.autoRefresh && this.pollInterval > 0) {
      // Add jitter to prevent thundering herd (±10%)
      const jitter = this.pollInterval * (0.9 + Math.random() * 0.2);
      // Auto-refresh scheduled

      this.refreshInterval = setInterval(() => {
        this.pollRules();
      }, jitter);
    }
  }

  stopAutoRefresh() {
    if (this.refreshInterval) {
      clearInterval(this.refreshInterval);
      this.refreshInterval = null;
    }
    if (this.timeDecayIntervalId) {
      clearInterval(this.timeDecayIntervalId);
      this.timeDecayIntervalId = null;
    }
    this.stopSessionTracking();

    // Clean up screen view tracking
    this.currentScreen = '';
  }

  private startTimeDecayLoop(): void {
    // Clear any existing interval
    if (this.timeDecayIntervalId) {
      clearInterval(this.timeDecayIntervalId);
    }

    // Run every minute to prune old events and re-evaluate
    this.timeDecayIntervalId = setInterval(() => {
      const pruned = this.pruneOldEvents();
      if (pruned > 0) {
        // Re-evaluate all features since events changed
        this.evaluateAllFeatures();
      }
    }, 60000); // 60 seconds
  }

  private pruneOldEvents(): number {
    const now = Date.now();
    const cutoffTime = now - (this.maxEventAgeDays * 24 * 60 * 60 * 1000);
    const initialLength = this.eventHistory.length;

    // Remove expired events
    this.eventHistory = this.eventHistory.filter(evt => evt.timestamp >= cutoffTime);

    const prunedCount = initialLength - this.eventHistory.length;
    if (prunedCount > 0) {
      this.log('DEBUG', 'Event retention limit reached, pruning old events');
    }

    return prunedCount;
  }

  async refreshRules() {
    await this.pollRules();
  }


  private buildIndex() {
    this.eventToFeaturesIndex.clear();
    this.userPropertyToFeaturesIndex.clear();
    this.devicePropertyToFeaturesIndex.clear();
    this.featureToRulesIndex.clear();

    let eventIndexCount = 0;
    let userPropIndexCount = 0;
    let devicePropIndexCount = 0;

    for (const [featureName, ruleSets] of Object.entries(this.rules)) {
      // Build feature-to-rules index for O(1) rule lookup during evaluation
      this.featureToRulesIndex.set(featureName, ruleSets);

      for (const ruleSet of ruleSets) {
        const conditions = ruleSet.conditions;

        // Index events
        const eventsConfig = conditions.events;
        if (Array.isArray(eventsConfig)) {
          eventsConfig.forEach(cond => {
            if (!this.eventToFeaturesIndex.has(cond.key)) {
              this.eventToFeaturesIndex.set(cond.key, new Set());
            }
            this.eventToFeaturesIndex.get(cond.key)!.add(featureName);
            eventIndexCount++;
          });
        } else if (eventsConfig?.events) {
          eventsConfig.events.forEach(cond => {
            if (!this.eventToFeaturesIndex.has(cond.key)) {
              this.eventToFeaturesIndex.set(cond.key, new Set());
            }
            this.eventToFeaturesIndex.get(cond.key)!.add(featureName);
            eventIndexCount++;
          });
        }

        // Index user properties
        if (conditions.user_properties) {
          conditions.user_properties.forEach(cond => {
            if (!this.userPropertyToFeaturesIndex.has(cond.key)) {
              this.userPropertyToFeaturesIndex.set(cond.key, new Set());
            }
            this.userPropertyToFeaturesIndex.get(cond.key)!.add(featureName);
            userPropIndexCount++;
          });
        }

        // Index device properties
        if (conditions.device) {
          conditions.device.forEach(cond => {
            if (!this.devicePropertyToFeaturesIndex.has(cond.key)) {
              this.devicePropertyToFeaturesIndex.set(cond.key, new Set());
            }
            this.devicePropertyToFeaturesIndex.get(cond.key)!.add(featureName);
            devicePropIndexCount++;
          });
        }
      }
    }

  }

  /**
   * Evaluate ALL features completely (Unity SDK approach)
   * No dirty feature tracking - always evaluate everything
   * Simpler, more reliable, and matches Unity SDK behavior
   */
  private evaluateAllFeatures() {

    if (this.featureToRulesIndex.size === 0) {
      return;
    }


    // Evaluate each feature using the index for O(1) rule lookup
    for (const [featureName, ruleSet] of this.featureToRulesIndex.entries()) {
      const oldValue = this.features.get(featureName);
      let newValue: any = null;

      // Find first matching rule for this feature
      for (const rule of ruleSet) {
        if (this.ruleMatches(rule)) {
          newValue = rule.value;
          break;
        }
      }

      // Set feature value (null if no match)
      this.features.set(featureName, newValue);

      if (oldValue !== newValue) {
        this.log('DEBUG', `Feature updated: ${featureName} = ${newValue}`);

        // Notify feature listeners
        const listeners = this.featureListeners.get(featureName);
        if (listeners) {
          listeners.forEach(callback => callback(featureName, newValue));
        }
      }
    }

    // Remove orphaned features not in current rules
    for (const featureName of this.features.keys()) {
      if (!this.featureToRulesIndex.has(featureName)) {
        this.features.delete(featureName);
      }
    }
  }

  private shouldPoll(): boolean {
    return (
      this.autoRefresh &&
      !this.isFetchInProgress &&
      Date.now() - this.lastFetchTime > this.pollInterval
    );
  }


  // =====================================================================================
  // SESSION-PERSISTENT CACHING (IndexedDB)
  // =====================================================================================

  private async pollRules(): Promise<void> {
    if (this.isFetchInProgress) {
      return;
    }

    this.isFetchInProgress = true;
    this.lastFetchTime = Date.now();

    try {
      const pointerResponse = await fetch(this.pointerUrl, {
        cache: 'no-store'
      });

      if (!pointerResponse.ok) {
        throw new Error(`Failed to fetch pointer: ${pointerResponse.status}`);
      }

      // Extract Country header from CDN response
      const countryHeader = pointerResponse.headers.get('Country');
      if (countryHeader) {
        this.deviceProperties = { ...this.deviceProperties, country: countryHeader };
        sessionStorage.setItem('appfig_country', countryHeader);
      }

      const pointer = await pointerResponse.json();
      const newHash = pointer.version;

      if (!newHash) {
        throw new Error('Invalid pointer data (missing version)');
      }

      // Enforce minimum poll interval from server (if present)
      if (pointer.min_poll_interval_secs) {
        const minPollIntervalMs = pointer.min_poll_interval_secs * 1000;
        if (this.pollInterval < minPollIntervalMs) {
          this.pollInterval = minPollIntervalMs;
        }
      }

      // Compare with cached hash
      if (this.currentHash && this.currentHash === newHash) {
        this.log('INFO', 'Rules are up to date');
        await this.saveCacheTimestamp();

        // Build index and evaluate once
        this.buildIndex();
        this.evaluateAllFeatures();

        // Fire onReady callback
        this.onReadyCallback?.();
        this.isFetchInProgress = false;
        return;
      }

      // Fetch immutable rules file
      this.log('INFO', 'Rules updated from server');
      const immutableUrl = `${this.cdnBaseUrl}/rules_versions/${this.companyId}/${this.tenantId}/${this.env}/current/${newHash}.json`;

      const rulesResponse = await fetch(immutableUrl);

      if (!rulesResponse.ok) {
        throw new Error(`Failed to fetch immutable rules: ${rulesResponse.status}`);
      }

      const rulesData = await rulesResponse.json();

      // Parse rules
      this.rules = rulesData.features;
      this.currentHash = newHash;

      await this.saveCachedRules(rulesData, newHash);

      // Build index and evaluate once
      this.buildIndex();
      this.evaluateAllFeatures();

      // Fire onRulesUpdated callback
      this.onRulesUpdatedCallback?.();
    } catch (err) {
      this.log('WARN', 'Failed to fetch remote rules – using cached version');
      if (Object.keys(this.rules).length > 0) {
      }
    } finally {
      this.isFetchInProgress = false;
    }
  }

  private async detectCountryFromCDN(): Promise<void> {
    try {
      // Check sessionStorage cache first
      const cachedCountry = sessionStorage.getItem('appfig_country');
      if (cachedCountry) {
        this.deviceProperties = { country: cachedCountry };
        return;
      }

      // Fetch from standalone country detection endpoint
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 5000);

      const res = await fetch('https://rules-dev.appfig.com/rules_versions/country/country/dev/current/latest.json', {
        signal: controller.signal,
        cache: 'no-store'
      });
      clearTimeout(timeoutId);

      if (res.ok) {
        const countryHeader = res.headers.get('Country');
        if (countryHeader) {
          this.deviceProperties = { country: countryHeader };
          sessionStorage.setItem('appfig_country', countryHeader);
            return;
        }
      }

      // No fallback - country detection is optional
    } catch (e) {
      // Country detection failed - continuing without country
    }
  }


  private async loadCachedRules(): Promise<boolean> {
    try {
      const db = await this.openIndexedDB();
      const tx = db.transaction('rules', 'readonly');
      const store = tx.objectStore('rules');

      const cacheKey = `${this.companyId}_${this.tenantId}_${this.env}`;
      const cached = await this.idbGet(store, cacheKey);

      if (!cached) {
        // No cached rules
        return false;
      }

      const cachedTime = new Date(cached.timestamp);
      this.lastFetchTime = cachedTime.getTime();
      this.currentHash = cached.hash || '';
      this.rules = cached.rules || {};

      const minutesAgo = (Date.now() - cachedTime.getTime()) / 60000;
      this.log('INFO', 'Rules loaded from cache');

      return true;
    } catch (err) {
      return false;
    }
  }

  private async saveCachedRules(rulesData: any, hash: string): Promise<void> {
    try {
      const db = await this.openIndexedDB();
      const tx = db.transaction('rules', 'readwrite');
      const store = tx.objectStore('rules');

      const cacheKey = `${this.companyId}_${this.tenantId}_${this.env}`;
      const cacheData = {
        key: cacheKey,
        rules: rulesData.features || rulesData,
        hash: hash,
        timestamp: new Date().toISOString(),
        version: rulesData.version || ''
      };

      await this.idbPut(store, cacheData);
      // Rules cached
    } catch (err) {
    }
  }

  private async saveCacheTimestamp(): Promise<void> {
    try {
      const db = await this.openIndexedDB();
      const tx = db.transaction('rules', 'readwrite');
      const store = tx.objectStore('rules');

      const cacheKey = `${this.companyId}_${this.tenantId}_${this.env}`;
      const cached = await this.idbGet(store, cacheKey);

      if (cached) {
        cached.timestamp = new Date().toISOString();
        await this.idbPut(store, cached);
        // Cache timestamp updated
      }
    } catch (err) {
    }
  }

  private async openIndexedDB(): Promise<IDBDatabase> {
    return new Promise((resolve, reject) => {
      const request = indexedDB.open('appfig-cache', 2); // Increment version for events store

      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve(request.result);

      request.onupgradeneeded = (event) => {
        const db = (event.target as IDBOpenDBRequest).result;
        if (!db.objectStoreNames.contains('rules')) {
          db.createObjectStore('rules', { keyPath: 'key' });
        }
        if (!db.objectStoreNames.contains('events')) {
          db.createObjectStore('events', { keyPath: 'key' });
        }
      };
    });
  }

  private idbGet(store: IDBObjectStore, key: string): Promise<any> {
    return new Promise((resolve, reject) => {
      const request = store.get(key);
      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve(request.result);
    });
  }

  private idbPut(store: IDBObjectStore, value: any): Promise<void> {
    return new Promise((resolve, reject) => {
      const request = store.put(value);
      request.onerror = () => reject(request.error);
      request.onsuccess = () => resolve();
    });
  }

  public async clearCache(): Promise<void> {
    try {
      const db = await this.openIndexedDB();
      const tx = db.transaction('rules', 'readwrite');
      const store = tx.objectStore('rules');

      const cacheKey = `${this.companyId}_${this.tenantId}_${this.env}`;
      await new Promise<void>((resolve, reject) => {
        const request = store.delete(cacheKey);
        request.onerror = () => reject(request.error);
        request.onsuccess = () => resolve();
      });

      // Cache cleared
    } catch (err) {
    }
  }

  // =====================================================================================
  // EVENT PERSISTENCE (IndexedDB)
  // =====================================================================================

  private async loadCachedEvents(): Promise<void> {
    try {
      const db = await this.openIndexedDB();
      const tx = db.transaction('events', 'readonly');
      const store = tx.objectStore('events');

      const cacheKey = `${this.companyId}_${this.tenantId}_${this.env}`;
      const cached = await this.idbGet(store, cacheKey);

      if (cached && cached.events) {
        this.eventHistory = cached.events;
      } else {
        // No cached events
      }
    } catch (err) {
    }
  }

  private debounceSaveEvents(): void {
    // Clear existing timeout
    if (this.eventSaveDebounceTimeout) {
      clearTimeout(this.eventSaveDebounceTimeout);
    }

    this.eventsSavedCount++;

    // Save immediately if 10 events have accumulated
    if (this.eventsSavedCount >= 10) {
      this.eventsSavedCount = 0;
      this.saveCachedEvents();
      return;
    }

    // Otherwise, debounce for 5 seconds
    this.eventSaveDebounceTimeout = setTimeout(() => {
      this.eventsSavedCount = 0;
      this.saveCachedEvents();
    }, 5000);
  }

  private async saveCachedEvents(): Promise<void> {
    try {
      const db = await this.openIndexedDB();
      const tx = db.transaction('events', 'readwrite');
      const store = tx.objectStore('events');

      const cacheKey = `${this.companyId}_${this.tenantId}_${this.env}`;
      const cacheData = {
        key: cacheKey,
        events: this.eventHistory,
        timestamp: new Date().toISOString()
      };

      await this.idbPut(store, cacheData);
      // Events saved to cache
    } catch (err) {
    }
  }

  private enforceEventRetention(): void {
    // Remove events older than maxEventAgeDays
    const maxAge = this.maxEventAgeDays * 24 * 60 * 60 * 1000; // Convert days to ms
    const cutoffTime = Date.now() - maxAge;

    const beforeCount = this.eventHistory.length;
    this.eventHistory = this.eventHistory.filter(event => event.timestamp >= cutoffTime);
    const removedByAge = beforeCount - this.eventHistory.length;

    // Trim to maxEvents (keep most recent)
    if (this.eventHistory.length > this.maxEvents) {
      const toRemove = this.eventHistory.length - this.maxEvents;
      this.eventHistory = this.eventHistory.slice(-this.maxEvents);
      }

    if (removedByAge > 0) {
      // Old events removed
    }
  }

  public async clearEventHistory(): Promise<void> {
    this.eventHistory = [];
    await this.saveCachedEvents();
    this.evaluateAllFeatures();
  }

  public getEventHistoryStats(): { count: number; oldestEvent: number | null; newestEvent: number | null; sizeMB: number } {
    const count = this.eventHistory.length;
    const oldestEvent = count > 0 ? this.eventHistory[0].timestamp : null;
    const newestEvent = count > 0 ? this.eventHistory[count - 1].timestamp : null;
    const sizeMB = new Blob([JSON.stringify(this.eventHistory)]).size / (1024 * 1024);

    return { count, oldestEvent, newestEvent, sizeMB };
  }

  private detectPlatform(): string {
    const userAgent = navigator.userAgent.toLowerCase();
    const platform = navigator.platform?.toLowerCase() || '';

    // Mobile platforms
    if (/iphone|ipod/.test(userAgent)) return 'iOS';
    if (/ipad/.test(userAgent)) return 'iPadOS';
    if (/android/.test(userAgent)) return 'Android';

    // Desktop platforms
    if (/mac/.test(platform) || /mac/.test(userAgent)) return 'macOS';
    if (/win/.test(platform) || /windows/.test(userAgent)) return 'Windows';
    if (/linux/.test(platform) || /linux/.test(userAgent)) return 'Linux';

    // Other platforms
    if (/cros/.test(userAgent)) return 'ChromeOS';
    if (/firefox/.test(userAgent) && /mobile/.test(userAgent)) return 'Firefox OS';

    // Fallback
    return 'Unknown';
  }

  private detectLanguage(): string {
    return navigator.language || navigator.languages?.[0] || 'en';
  }

  private detectTimezone(): string {
    try {
      return Intl.DateTimeFormat().resolvedOptions().timeZone;
    } catch (e) {
      return 'UTC';
    }
  }

  private detectOSVersion(): string {
    const userAgent = navigator.userAgent;
    
    // iOS version
    const iosMatch = userAgent.match(/OS (\d+_\d+_?\d*)/);
    if (iosMatch) {
      return iosMatch[1].replace(/_/g, '.');
    }
    
    // Android version
    const androidMatch = userAgent.match(/Android (\d+\.?\d*\.?\d*)/);
    if (androidMatch) {
      return androidMatch[1];
    }
    
    // Windows version
    const windowsMatch = userAgent.match(/Windows NT (\d+\.\d+)/);
    if (windowsMatch) {
      const version = windowsMatch[1];
      // Convert NT version to Windows version
      const windowsVersions: Record<string, string> = {
        '10.0': '10/11',
        '6.3': '8.1',
        '6.2': '8',
        '6.1': '7',
        '6.0': 'Vista',
        '5.1': 'XP'
      };
      return windowsVersions[version] || version;
    }
    
    // macOS version
    const macMatch = userAgent.match(/Mac OS X (\d+_\d+_?\d*)/);
    if (macMatch) {
      return macMatch[1].replace(/_/g, '.');
    }
    
    return 'Unknown';
  }

  private detectDeviceBrand(): string {
    const userAgent = navigator.userAgent.toLowerCase();
    
    // Mobile brands
    if (/iphone|ipad|ipod/.test(userAgent)) return 'Apple';
    if (/samsung/.test(userAgent)) return 'Samsung';
    if (/huawei/.test(userAgent)) return 'Huawei';
    if (/xiaomi/.test(userAgent)) return 'Xiaomi';
    if (/oppo/.test(userAgent)) return 'Oppo';
    if (/vivo/.test(userAgent)) return 'Vivo';
    if (/oneplus/.test(userAgent)) return 'OnePlus';
    if (/lg/.test(userAgent)) return 'LG';
    if (/htc/.test(userAgent)) return 'HTC';
    if (/sony/.test(userAgent)) return 'Sony';
    if (/motorola|moto/.test(userAgent)) return 'Motorola';
    if (/nokia/.test(userAgent)) return 'Nokia';
    if (/pixel/.test(userAgent)) return 'Google';
    
    // Desktop brands (harder to detect reliably)
    if (/mac/.test(userAgent)) return 'Apple';
    if (/windows/.test(userAgent)) return 'Microsoft';
    
    return 'Unknown';
  }

  private detectDeviceModel(): string {
    const userAgent = navigator.userAgent;
    
    // Desktop/Web fallback - use browser info instead of device model
    if (!(/Mobile|Android|iPhone|iPad/.test(userAgent))) {
      // For desktop, return browser + OS info as "model"
      const browserInfo = this.getBrowserInfo();
      const platform = this.detectPlatform();
      return `${browserInfo} on ${platform}`;
    }
    
    // iPhone models
    const iphoneMatch = userAgent.match(/iPhone(\d+,\d+)/);
    if (iphoneMatch) {
      const modelMap: Record<string, string> = {
        '14,7': 'iPhone 14',
        '14,8': 'iPhone 14 Plus',
        '15,2': 'iPhone 14 Pro',
        '15,3': 'iPhone 14 Pro Max',
        '14,4': 'iPhone 13 mini',
        '14,5': 'iPhone 13',
        '14,2': 'iPhone 13 Pro',
        '14,3': 'iPhone 13 Pro Max',
        '13,1': 'iPhone 12 mini',
        '13,2': 'iPhone 12',
        '13,3': 'iPhone 12 Pro',
        '13,4': 'iPhone 12 Pro Max'
      };
      return modelMap[iphoneMatch[1]] || `iPhone ${iphoneMatch[1]}`;
    }
    
    // iPad models
    const ipadMatch = userAgent.match(/iPad(\d+,\d+)/);
    if (ipadMatch) {
      return `iPad ${ipadMatch[1]}`;
    }
    
    // Android models (very limited detection)
    const androidModelMatch = userAgent.match(/;\s*([^;)]+)\s*\)/);
    if (androidModelMatch && /android/i.test(userAgent)) {
      const model = androidModelMatch[1].trim();
      if (model && !model.includes('wv') && model.length < 50) {
        return model;
      }
    }
    
    return 'Unknown';
  }

  private getBrowserInfo(): string {
    const userAgent = navigator.userAgent.toLowerCase();
    
    if (userAgent.includes('chrome') && !userAgent.includes('edg')) {
      const chromeMatch = userAgent.match(/chrome\/(\d+)/);
      return chromeMatch ? `Chrome ${chromeMatch[1]}` : 'Chrome';
    }
    if (userAgent.includes('firefox')) {
      const firefoxMatch = userAgent.match(/firefox\/(\d+)/);
      return firefoxMatch ? `Firefox ${firefoxMatch[1]}` : 'Firefox';
    }
    if (userAgent.includes('safari') && !userAgent.includes('chrome')) {
      const safariMatch = userAgent.match(/version\/(\d+)/);
      return safariMatch ? `Safari ${safariMatch[1]}` : 'Safari';
    }
    if (userAgent.includes('edg')) {
      const edgeMatch = userAgent.match(/edg\/(\d+)/);
      return edgeMatch ? `Edge ${edgeMatch[1]}` : 'Edge';
    }
    
    return 'Browser';
  }

  private initSessionTracking() {
    // Check if this is first open
    const hasOpenedBefore = localStorage.getItem('appfig_first_open');
    if (!hasOpenedBefore) {
      this.logEvent('first_open');
      localStorage.setItem('appfig_first_open', 'true');
    }

    // Log session start
    this.logEvent('session_start');
    this.sessionActive = true;
    this.lastActivity = Date.now();

    // Set up activity tracking
    this.setupActivityTracking();
    
    // Set up session timeout
    this.resetSessionTimeout();

    // Set up page visibility tracking for session_end
    this.setupVisibilityTracking();
  }

  private setupActivityTracking() {
    const activityEvents = ['mousedown', 'mousemove', 'keypress', 'scroll', 'touchstart', 'click'];
    
    const updateActivity = () => {
      this.lastActivity = Date.now();
      this.resetSessionTimeout();
    };

    activityEvents.forEach(event => {
      document.addEventListener(event, updateActivity, { passive: true });
    });
  }

  private resetSessionTimeout() {
    if (this.sessionTimeout) {
      clearTimeout(this.sessionTimeout);
    }

    this.sessionTimeout = setTimeout(() => {
      if (this.sessionActive) {
        this.endSession();
      }
    }, this.sessionTimeoutMs);
  }

  private setupVisibilityTracking() {
    const handleVisibilityChange = () => {
      if (document.hidden && this.sessionActive) {
        this.endSession();
      } else if (!document.hidden && !this.sessionActive) {
        this.log('[AppFig] 📱 App foregrounded, starting session');
        this.logEvent('session_start');
        this.sessionActive = true;
        this.lastActivity = Date.now();
        this.resetSessionTimeout();
      }
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);

    window.addEventListener('beforeunload', () => {
      if (this.sessionActive) {
        this.log('[AppFig] Session ended');
        this.endSession();
      }
    });
  }

  private endSession() {
    if (this.sessionActive) {
      this.logEvent('session_end');
      this.sessionActive = false;
      
      if (this.sessionTimeout) {
        clearTimeout(this.sessionTimeout);
        this.sessionTimeout = null;
      }
    }
  }

  private stopSessionTracking() {
    if (this.sessionTimeout) {
      clearTimeout(this.sessionTimeout);
      this.sessionTimeout = null;
    }
    
    if (this.sessionActive) {
      this.endSession();
    }
  }


  private ruleMatches(rule: AppFigRule): boolean {
    const {
      events = [],
      user_properties = [],
      device = [],
      user_properties_operator = 'AND',
      device_operator = 'AND',
    } = rule.conditions || {};

    this.log(`[AppFig] ruleMatches - Evaluating conditions:`);
    this.log(`  - events:`, JSON.stringify(events));
    this.log(`  - user_properties:`, JSON.stringify(user_properties));
    this.log(`  - device:`, JSON.stringify(device));

    // Handle new EventsConfig format
    const eventsResult = this.evaluateEventsConfig(events, rule.sequential);
    this.log(`[AppFig] Events evaluation result:`, eventsResult ? '✅ PASS' : '❌ FAIL');

    const userPropsResult = this.evaluatePropertiesWithOperator(
      user_properties,
      [this.userProperties],
      user_properties_operator
    );
    this.log(`[AppFig] User properties evaluation result:`, userPropsResult ? '✅ PASS' : '❌ FAIL');

    const deviceResult = this.evaluatePropertiesWithOperator(
      device,
      [this.deviceProperties],
      device_operator
    );
    this.log(`[AppFig] Device properties evaluation result:`, deviceResult ? '✅ PASS' : '❌ FAIL');

    const finalResult = eventsResult && userPropsResult && deviceResult;
    this.log(`[AppFig] Final rule match result:`, finalResult ? '✅ PASS' : '❌ FAIL');

    return finalResult;
  }

  private evaluateEventsConfig(eventsConfig: Condition[] | EventsConfig, legacySequential?: boolean): boolean {
    // Handle legacy format (flat Condition array)
    if (Array.isArray(eventsConfig)) {
      return this.matchConditions(eventsConfig, this.eventHistory, true, legacySequential);
    }

    // Handle new EventsConfig format
    const config = eventsConfig as EventsConfig;
    const mode = config.mode || 'simple';

    const eventsList = config.events || [];

    if (eventsList.length === 0) {
      return true;
    }

    const operator = config.operator || 'AND';
    const ordering = config.ordering || 'direct';

    if (mode === 'simple') {
      // Simple mode: AND or OR logic
      if (operator === 'AND') {
        return eventsList.every(cond =>
          this.eventHistory.some(e => this.conditionMatches(cond, e))
        );
      } else {
        return eventsList.some(cond =>
          this.eventHistory.some(e => this.conditionMatches(cond, e))
        );
      }
    } else {
      // Sequence mode: direct or indirect ordering
      return this.evaluateSequence(eventsList, ordering);
    }
  }

  private evaluateSequence(conditions: Condition[], ordering: string): boolean {
    if (ordering === 'direct') {
      // Direct: events must occur consecutively
      let lastIndex = -1;
      for (const cond of conditions) {
        let found = false;
        for (let i = lastIndex + 1; i < this.eventHistory.length; i++) {
          if (this.conditionMatches(cond, this.eventHistory[i])) {
            lastIndex = i;
            found = true;
            break;
          }
        }
        if (!found) return false;
      }
      return true;
    } else {
      // Indirect: events must occur in order but not necessarily consecutive
      let currentIndex = 0;
      for (const cond of conditions) {
        let found = false;
        for (let i = currentIndex; i < this.eventHistory.length; i++) {
          if (this.conditionMatches(cond, this.eventHistory[i])) {
            currentIndex = i + 1;
            found = true;
            break;
          }
        }
        if (!found) return false;
      }
      return true;
    }
  }

  private evaluatePropertiesWithOperator(
    conditions: Condition[],
    data: any[],
    operator: 'AND' | 'OR'
  ): boolean {
    if (conditions.length === 0) {
      return true;
    }

    if (operator === 'AND') {
      return conditions.every(cond =>
        data.some(d => this.conditionMatches(cond, d))
      );
    } else {
      return conditions.some(cond =>
        data.some(d => this.conditionMatches(cond, d))
      );
    }
  }

  private matchConditions(
    conditions: Condition[],
    data: any[],
    isEvent = false,
    sequential = false
  ): boolean {
    if (sequential && isEvent) {
      let i = 0;
      for (const cond of conditions) {
        let found = false;
        while (i < data.length) {
          const e = data[i];
          if (this.conditionMatches(cond, e)) {
            found = true;
            i++;
            break;
          }
          i++;
        }
        if (!found) return false;
      }
      return true;
    } else {
      return conditions.every((cond) =>
        data.some((d) => this.conditionMatches(cond, d))
      );
    }
  }

  private conditionMatches(
    cond: Condition,
    event: AppFigEvent | Record<string, any>
  ): boolean {
    // For events, check if the event name matches the condition key using operator
    if ('name' in event) {
      const eventOperator = cond.operator || '==';
      const nameMatch = this.compare(eventOperator, event.name, cond.key);
      if (!nameMatch) return false;
    }

    let match = true;

    // Param matching
    if (cond.param && 'params' in event && event.params) {
      // Check all param conditions must match
      for (const [paramKey, paramCondition] of Object.entries(cond.param)) {
        const paramValue = event.params[paramKey];
        if (
          !this.compare(
            paramCondition.operator,
            paramValue,
            paramCondition.value
          )
        ) {
          match = false;
          break;
        }
      }
    }

    // Count matching
    if (cond.count) {
      const eventOperator = cond.operator || '==';
      const actualCount = this.eventHistory.filter((e) => {
        return 'name' in e && this.compare(eventOperator, e.name, cond.key);
      }).length;

      match =
        match &&
        this.compare(cond.count.operator, actualCount, cond.count.value);
    }

    // Repeat matching - consecutive occurrences
    if (cond.repeat) {
      const eventOperator = cond.operator || '==';
      const maxConsecutive = this.getMaxConsecutiveOccurrences(cond.key, eventOperator);
      match =
        match &&
        this.compare(cond.repeat.operator, maxConsecutive, cond.repeat.value);
    }

    // Value matching (for user_properties and device conditions)
    if (cond.value !== undefined && !('name' in event)) {
      const actualValue = event[cond.key];
      const valueMatch = this.compare(cond.value.operator, actualValue, cond.value.value);
      match = match && valueMatch;
    }

    // Time-based match
    if (cond.within_last_days && 'timestamp' in event) {
      const days = cond.within_last_days;
      const cutoff = Date.now() - days * 86400000;
      match = match && event.timestamp >= cutoff;
    }

    // Apply NOT logic if specified
    if (cond.not) {
      match = !match;
    }

    return match;
  }

  private getMaxConsecutiveOccurrences(eventName: string, operator: string = '=='): number {
    let maxConsecutive = 0;
    let currentConsecutive = 0;

    // Sort events by timestamp to ensure chronological order
    const sortedEvents = [...this.eventHistory].sort(
      (a, b) => a.timestamp - b.timestamp
    );

    for (let i = 0; i < sortedEvents.length; i++) {
      const event = sortedEvents[i];

      if (this.compare(operator, event.name, eventName)) {
        currentConsecutive++;
        maxConsecutive = Math.max(maxConsecutive, currentConsecutive);
      } else {
        // Reset consecutive count when a different event occurs
        currentConsecutive = 0;
      }
    }

    return maxConsecutive;
  }
  private compare(op: string, actual: any, expected: any): boolean {
    switch (op) {
      case '==':
        return actual === expected;
      case '!=':
        return actual !== expected;
      case '>':
        return actual > expected;
      case '<':
        return actual < expected;
      case '>=':
        return actual >= expected;
      case '<=':
        return actual <= expected;
      case 'in':
        if (Array.isArray(expected)) {
          return expected.some(val => String(val).toLowerCase() === String(actual).toLowerCase());
        }
        return false;
      case 'not_in':
        if (Array.isArray(expected)) {
          return !expected.some(val => String(val).toLowerCase() === String(actual).toLowerCase());
        }
        return true;
      case 'contains':
        return (
          typeof actual === 'string' &&
          typeof expected === 'string' &&
          actual.toLowerCase().includes(expected.toLowerCase())
        );
      case 'starts_with':
        return (
          typeof actual === 'string' &&
          typeof expected === 'string' &&
          actual.toLowerCase().startsWith(expected.toLowerCase())
        );
      case 'ends_with':
        return (
          typeof actual === 'string' &&
          typeof expected === 'string' &&
          actual.toLowerCase().endsWith(expected.toLowerCase())
        );
      default:
        return false;
    }
  }

  // =====================================================================================
  // SCHEMA DISCOVERY
  // =====================================================================================

  private schemaCache: {
    events: Set<string>;
    eventParams: Map<string, Set<string>>;
    eventParamValues: Map<string, Map<string, Set<string>>>;
    userProperties: Map<string, Set<string>>;
    deviceProperties: Map<string, Set<string>>;
  } = {
    events: new Set(),
    eventParams: new Map(),
    eventParamValues: new Map(),
    userProperties: new Map(),
    deviceProperties: new Map()
  };

  private schemaDiffBuffer: {
    new_events: Set<string>;
    new_event_params: Map<string, { params: Set<string>, param_values: Map<string, Set<string>> }>;
    new_user_properties: Map<string, Set<string>>;
    new_device_properties: Map<string, Set<string>>;
  } = {
    new_events: new Set(),
    new_event_params: new Map(),
    new_user_properties: new Map(),
    new_device_properties: new Map()
  };

  private schemaUploadTimeout: NodeJS.Timeout | null = null;
  private schemaUploadCount: number = 0;
  private deviceId: string = '';
  private lastSchemaUploadTime: number = 0;
  private readonly SCHEMA_UPLOAD_BATCH_SIZE = 50;
  private readonly SCHEMA_UPLOAD_INTERVAL_MS = 600000; // 10 minutes
  private readonly SCHEMA_UPLOAD_THROTTLE_MS = 43200000; // 12 hours

  private initSchemaDiscovery() {
    // Generate stable device ID
    const storedDeviceId = localStorage.getItem('appfig_device_id');
    if (storedDeviceId) {
      this.deviceId = storedDeviceId;
    } else {
      this.deviceId = this.generateDeviceId();
      localStorage.setItem('appfig_device_id', this.deviceId);
    }

    // Load cached schema and last upload time
    this.loadCachedSchema();
    const lastUpload = localStorage.getItem('appfig_last_schema_upload');
    if (lastUpload) {
      this.lastSchemaUploadTime = parseInt(lastUpload, 10);
    }

    // 1% deterministic sampling based on device ID hash
    const shouldSample = this.isInSample();
    if (!shouldSample) {
      this.log('DEBUG', 'Schema discovery: device not in 1% sample');
    }
  }

  private generateDeviceId(): string {
    return `${Date.now()}-${Math.random().toString(36).substring(2, 15)}`;
  }

  private isInSample(): boolean {
    // Simple hash of device ID
    let hash = 0;
    for (let i = 0; i < this.deviceId.length; i++) {
      hash = ((hash << 5) - hash) + this.deviceId.charCodeAt(i);
      hash = hash & hash;
    }
    return Math.abs(hash) % 100 < 1; // 1% sample
  }

  private maskPII(value: any): string {
    const str = String(value);

    // Mask emails
    if (str.includes('@') && str.includes('.')) {
      return '[email]';
    }

    // Truncate long strings (likely IDs or sensitive data)
    if (str.length > 50) {
      return str.substring(0, 50) + '...';
    }

    // Truncate at 100 chars max
    if (str.length > 100) {
      return str.substring(0, 100);
    }

    return str;
  }

  private trackEventSchema(eventName: string, params?: Record<string, any>) {
    // Check if event is new
    if (!this.schemaCache.events.has(eventName)) {
      this.schemaDiffBuffer.new_events.add(eventName);
      this.schemaCache.events.add(eventName);
    }

    // Track parameters
    if (params) {
      const paramKeys = Object.keys(params);

      // Get or create param set for this event
      let cachedParams = this.schemaCache.eventParams.get(eventName);
      if (!cachedParams) {
        cachedParams = new Set();
        this.schemaCache.eventParams.set(eventName, cachedParams);
      }

      // Get or create param value map for this event
      let cachedParamValues = this.schemaCache.eventParamValues.get(eventName);
      if (!cachedParamValues) {
        cachedParamValues = new Map();
        this.schemaCache.eventParamValues.set(eventName, cachedParamValues);
      }

      // Get or create buffer entry
      let bufferEntry = this.schemaDiffBuffer.new_event_params.get(eventName);
      if (!bufferEntry) {
        bufferEntry = { params: new Set(), param_values: new Map() };
        this.schemaDiffBuffer.new_event_params.set(eventName, bufferEntry);
      }

      // Check for new params and values
      for (const paramKey of paramKeys) {
        const paramValue = params[paramKey];
        const maskedValue = this.maskPII(paramValue);

        // New param key
        if (!cachedParams.has(paramKey)) {
          bufferEntry.params.add(paramKey);
          cachedParams.add(paramKey);
        }

        // Track value
        let valueSet = cachedParamValues.get(paramKey);
        if (!valueSet) {
          valueSet = new Set();
          cachedParamValues.set(paramKey, valueSet);
        }

        // New param value (limit to 20 per param)
        if (!valueSet.has(maskedValue) && valueSet.size < 20) {
          let bufferValueSet = bufferEntry.param_values.get(paramKey);
          if (!bufferValueSet) {
            bufferValueSet = new Set();
            bufferEntry.param_values.set(paramKey, bufferValueSet);
          }
          bufferValueSet.add(maskedValue);
          valueSet.add(maskedValue);
        }
      }
    }

    this.debounceSchemaUpload();
  }

  private trackUserPropertySchema(props: Record<string, any>) {
    for (const [key, value] of Object.entries(props)) {
      const maskedValue = this.maskPII(value);

      // Get or create value set
      let valueSet = this.schemaCache.userProperties.get(key);
      if (!valueSet) {
        valueSet = new Set();
        this.schemaCache.userProperties.set(key, valueSet);
      }

      // New value (limit to 20 per property)
      if (!valueSet.has(maskedValue) && valueSet.size < 20) {
        let bufferValueSet = this.schemaDiffBuffer.new_user_properties.get(key);
        if (!bufferValueSet) {
          bufferValueSet = new Set();
          this.schemaDiffBuffer.new_user_properties.set(key, bufferValueSet);
        }
        bufferValueSet.add(maskedValue);
        valueSet.add(maskedValue);
      }
    }

    this.debounceSchemaUpload();
  }

  private trackDevicePropertySchema(props: Record<string, any>) {
    for (const [key, value] of Object.entries(props)) {
      const maskedValue = this.maskPII(value);

      // Get or create value set
      let valueSet = this.schemaCache.deviceProperties.get(key);
      if (!valueSet) {
        valueSet = new Set();
        this.schemaCache.deviceProperties.set(key, valueSet);
      }

      // New value (limit to 20 per property)
      if (!valueSet.has(maskedValue) && valueSet.size < 20) {
        let bufferValueSet = this.schemaDiffBuffer.new_device_properties.get(key);
        if (!bufferValueSet) {
          bufferValueSet = new Set();
          this.schemaDiffBuffer.new_device_properties.set(key, bufferValueSet);
        }
        bufferValueSet.add(maskedValue);
        valueSet.add(maskedValue);
      }
    }

    this.debounceSchemaUpload();
  }

  private debounceSchemaUpload() {
    // Skip if not in 1% sample
    if (!this.isInSample()) return;

    // Throttle: skip if uploaded within last 12 hours
    const now = Date.now();
    if (now - this.lastSchemaUploadTime < this.SCHEMA_UPLOAD_THROTTLE_MS) {
      this.log('DEBUG', 'Schema upload throttled (12-hour limit)');
      return;
    }

    // Clear existing timeout
    if (this.schemaUploadTimeout) {
      clearTimeout(this.schemaUploadTimeout);
    }

    this.schemaUploadCount++;

    // Upload immediately if batch size reached
    if (this.schemaUploadCount >= this.SCHEMA_UPLOAD_BATCH_SIZE) {
      this.schemaUploadCount = 0;
      this.uploadSchemaDiff();
      return;
    }

    // Otherwise, debounce for interval
    this.schemaUploadTimeout = setTimeout(() => {
      this.schemaUploadCount = 0;
      this.uploadSchemaDiff();
    }, this.SCHEMA_UPLOAD_INTERVAL_MS);
  }

  private async uploadSchemaDiff() {
    // Skip if buffer is empty
    if (
      this.schemaDiffBuffer.new_events.size === 0 &&
      this.schemaDiffBuffer.new_event_params.size === 0 &&
      this.schemaDiffBuffer.new_user_properties.size === 0 &&
      this.schemaDiffBuffer.new_device_properties.size === 0
    ) {
      return;
    }

    try {
      // Build payload
      const payload: any = {
        tenant_id: this.tenantId,
        env: this.env
      };

      if (this.schemaDiffBuffer.new_events.size > 0) {
        payload.new_events = Array.from(this.schemaDiffBuffer.new_events);
      }

      if (this.schemaDiffBuffer.new_event_params.size > 0) {
        payload.new_event_params = {};
        for (const [eventName, data] of this.schemaDiffBuffer.new_event_params.entries()) {
          payload.new_event_params[eventName] = {
            params: Array.from(data.params),
            param_values: {}
          };
          for (const [param, values] of data.param_values.entries()) {
            payload.new_event_params[eventName].param_values[param] = Array.from(values);
          }
        }
      }

      if (this.schemaDiffBuffer.new_user_properties.size > 0) {
        payload.new_user_properties = {};
        for (const [prop, values] of this.schemaDiffBuffer.new_user_properties.entries()) {
          payload.new_user_properties[prop] = Array.from(values);
        }
      }

      if (this.schemaDiffBuffer.new_device_properties.size > 0) {
        payload.new_device_properties = {};
        for (const [prop, values] of this.schemaDiffBuffer.new_device_properties.entries()) {
          payload.new_device_properties[prop] = Array.from(values);
        }
      }

      // Send to collectMeta endpoint
      const functionUrl = 'https://us-central1-appfig-dev.cloudfunctions.net/collectMeta';

      const response = await fetch(functionUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-API-Key': this.apiKey
        },
        body: JSON.stringify(payload)
      });

      if (response.ok) {
        this.log('DEBUG', 'Schema uploaded successfully');

        // Update last upload time
        this.lastSchemaUploadTime = Date.now();
        localStorage.setItem('appfig_last_schema_upload', String(this.lastSchemaUploadTime));

        // Clear buffer
        this.schemaDiffBuffer.new_events.clear();
        this.schemaDiffBuffer.new_event_params.clear();
        this.schemaDiffBuffer.new_user_properties.clear();
        this.schemaDiffBuffer.new_device_properties.clear();

        // Save updated schema cache
        this.saveCachedSchema();
      } else {
        this.log('WARN', `Schema upload failed: ${response.status}`);
      }
    } catch (err) {
      this.warn('[AppFig] Schema upload error:', err);
    }
  }

  private loadCachedSchema() {
    try {
      const cached = localStorage.getItem('appfig_schema_cache');
      if (cached) {
        const data = JSON.parse(cached);
        this.schemaCache.events = new Set(data.events || []);

        this.schemaCache.eventParams = new Map();
        for (const [event, params] of Object.entries(data.eventParams || {})) {
          this.schemaCache.eventParams.set(event, new Set(params as string[]));
        }

        this.schemaCache.eventParamValues = new Map();
        for (const [event, paramMap] of Object.entries(data.eventParamValues || {})) {
          const valueMap = new Map();
          for (const [param, values] of Object.entries(paramMap as any)) {
            valueMap.set(param, new Set(values));
          }
          this.schemaCache.eventParamValues.set(event, valueMap);
        }

        this.schemaCache.userProperties = new Map();
        for (const [prop, values] of Object.entries(data.userProperties || {})) {
          this.schemaCache.userProperties.set(prop, new Set(values as string[]));
        }

        this.schemaCache.deviceProperties = new Map();
        for (const [prop, values] of Object.entries(data.deviceProperties || {})) {
          this.schemaCache.deviceProperties.set(prop, new Set(values as string[]));
        }

        this.log('[AppFig] Loaded cached schema');
      }
    } catch (err) {
      this.warn('[AppFig] Failed to load cached schema:', err);
    }
  }

  private saveCachedSchema() {
    try {
      const data: any = {
        events: Array.from(this.schemaCache.events),
        eventParams: {},
        eventParamValues: {},
        userProperties: {},
        deviceProperties: {}
      };

      for (const [event, params] of this.schemaCache.eventParams.entries()) {
        data.eventParams[event] = Array.from(params);
      }

      for (const [event, paramMap] of this.schemaCache.eventParamValues.entries()) {
        data.eventParamValues[event] = {};
        for (const [param, values] of paramMap.entries()) {
          data.eventParamValues[event][param] = Array.from(values);
        }
      }

      for (const [prop, values] of this.schemaCache.userProperties.entries()) {
        data.userProperties[prop] = Array.from(values);
      }

      for (const [prop, values] of this.schemaCache.deviceProperties.entries()) {
        data.deviceProperties[prop] = Array.from(values);
      }

      localStorage.setItem('appfig_schema_cache', JSON.stringify(data));
      this.log('[AppFig] Saved schema cache');
    } catch (err) {
      this.warn('[AppFig] Failed to save schema cache:', err);
    }
  }
}

const AppFig = new AppFigCore();
export default AppFig;