/// AppFig for Flutter.
///
/// A thin layer over the native AppFig SDKs: rules are fetched, cached and
/// evaluated by the Android and iOS SDKs, so behaviour matches a native app.
///
/// ```dart
/// await AppFig.init(companyId: '...', tenantId: '...', env: 'prod', apiKey: '...');
/// final limit = await AppFig.getFeatureValue('max_songs');
/// AppFig.logEvent('add_song', parameters: {'source': 'import'});
/// ```
library appfig_flutter;

import 'dart:async';
import 'dart:developer' as developer;

import 'package:flutter/services.dart';

/// An A/B test variant the SDK assigned to this user.
class AppFigVariant {
  const AppFigVariant(this.experimentKey, this.variantName);
  final String experimentKey;
  final String variantName;

  @override
  String toString() => '$experimentKey:$variantName';
}

class AppFig {
  AppFig._();

  static const MethodChannel _channel = MethodChannel('com.appfig/flutter');
  static final StreamController<void> _rulesUpdated = StreamController<void>.broadcast();
  static final StreamController<AppFigVariant> _variants = StreamController<AppFigVariant>.broadcast();
  static bool _handlerSet = false;
  static FutureOr<void> Function(String name, String value)? _analyticsSetUserProperty;
  static FutureOr<void> Function(String name, Map<String, Object> parameters)? _analyticsLogEvent;
  static bool _unsupported = false;

  /// Whether this platform has the AppFig SDK. False on desktop and web: there every call does nothing and
  /// [getFeatureValue] returns null, so the app runs on the values in its code.
  static bool get isSupported => !_unsupported;

  // A failing AppFig call never stops the app. Where the platform has no AppFig SDK (desktop, web) calls do
  // nothing; an error from the native SDK is logged and the call returns null.
  static Future<T?> _invoke<T>(String method, [Object? arguments]) async {
    if (_unsupported) return null;
    try {
      return await _channel.invokeMethod<T>(method, arguments);
    } on MissingPluginException {
      _unsupported = true;
      developer.log('AppFig is not available on this platform; the app uses the values in its code.', name: 'AppFig');
      return null;
    } catch (e) {
      developer.log('AppFig $method failed: $e', name: 'AppFig');
      return null;
    }
  }

  // Analytics calls the app could not take yet (Firebase starts after AppFig, for instance) wait here and are
  // tried again, in order, every 3 seconds for two minutes.
  static final List<FutureOr<void> Function()> _pending = [];
  static Timer? _retry;
  static Future<void> _deliver(FutureOr<void> Function() send) async {
    if (_pending.isNotEmpty) { _pending.add(send); return; }
    try {
      await send();
    } catch (_) {
      _pending.add(send);
      _retryLater();
    }
  }
  static void _retryLater() {
    if (_retry != null) return;
    var tries = 0;
    _retry = Timer.periodic(const Duration(seconds: 3), (t) async {
      tries++;
      while (_pending.isNotEmpty) {
        try { await _pending.first(); _pending.removeAt(0); } catch (_) { break; }
      }
      if (_pending.isEmpty || tries >= 40) { _pending.clear(); t.cancel(); _retry = null; }
    });
  }

  static void _ensureHandler() {
    if (_handlerSet) return;
    _handlerSet = true;
    _channel.setMethodCallHandler((call) async {
      switch (call.method) {
        case 'onRulesUpdated':
          _rulesUpdated.add(null);
          break;
        case 'onVariantAssigned':
          final args = (call.arguments as Map?) ?? const {};
          _variants.add(AppFigVariant('${args['experimentKey']}', '${args['variantName']}'));
          break;
        case 'analyticsUserProperty':
          final args = (call.arguments as Map?) ?? const {};
          final set = _analyticsSetUserProperty;
          if (set != null) unawaited(_deliver(() => set('${args['key']}', '${args['value']}')));
          break;
        case 'analyticsExperimentAssigned':
          final args = (call.arguments as Map?) ?? const {};
          final log = _analyticsLogEvent;
          if (log != null) {
            final params = <String, Object>{'experiment': '${args['experimentKey']}', 'variant': '${args['variantName']}'};
            unawaited(_deliver(() => log('appfig_experiment', params)));
          }
          break;
        case 'analyticsFeatureSeen':
          final args = (call.arguments as Map?) ?? const {};
          final log = _analyticsLogEvent;
          if (log != null) {
            final params = <String, Object>{'feature': '${args['featureName']}', 'feature_value': '${args['value']}'};
            unawaited(_deliver(() => log('appfig_feature', params)));
          }
          break;
      }
    });
  }

  /// Starts the SDK. Call once, before `runApp` or early in startup.
  ///
  /// The returned future completes when rules are available (from cache or the
  /// network) or after [readyTimeout], whichever comes first, so the first
  /// [getFeatureValue] calls see real values instead of racing the first fetch.
  static Future<void> init({
    required String companyId,
    required String tenantId,
    required String env,
    required String apiKey,
    bool autoRefresh = true,
    Duration pollInterval = const Duration(hours: 12),
    bool debugMode = false,
    Duration readyTimeout = const Duration(seconds: 5),
    String? cdnBaseUrl,
  }) {
    _ensureHandler();
    return _invoke<void>('init', {
      'companyId': companyId,
      'tenantId': tenantId,
      'env': env,
      'apiKey': apiKey,
      'autoRefresh': autoRefresh,
      'pollIntervalMs': pollInterval.inMilliseconds,
      'debugMode': debugMode,
      'readyTimeoutMs': readyTimeout.inMilliseconds,
      if (cdnBaseUrl != null) 'cdnBaseUrl': cdnBaseUrl,
    });
  }

  /// The value of [feature] for this user, or null when no rule matches.
  static Future<String?> getFeatureValue(String feature) =>
      _invoke<String>('getFeatureValue', {'name': feature});

  /// Whether [feature] is on for this user.
  static Future<bool> isFeatureEnabled(String feature) async =>
      (await _invoke<bool>('isFeatureEnabled', {'name': feature})) ?? false;

  /// Whether any rules are loaded yet.
  static Future<bool> hasRulesLoaded() async => (await _invoke<bool>('hasRulesLoaded')) ?? false;

  /// Records an event that rules can target on. Parameter values are sent as text.
  static Future<void> logEvent(String name, {Map<String, Object?>? parameters}) =>
      _invoke<void>('logEvent', {'name': name, 'params': _stringify(parameters)});

  static Future<void> logScreenView(String screenName, {String? previousScreen}) =>
      _invoke<void>('logScreenView', {'screenName': screenName, 'previousScreen': previousScreen});

  static Future<void> setUserProperty(String key, Object value) =>
      _invoke<void>('setUserProperty', {'key': key, 'value': '$value'});

  static Future<void> removeUserProperty(String key) => _invoke<void>('removeUserProperty', {'key': key});

  static Future<void> setDeviceProperty(String key, Object value) =>
      _invoke<void>('setDeviceProperty', {'key': key, 'value': '$value'});

  static Future<void> removeDeviceProperty(String key) => _invoke<void>('removeDeviceProperty', {'key': key});

  static Future<void> setAppVersion(String version) => _invoke<void>('setAppVersion', {'version': version});

  /// A stable id for this user. Optional: without it, A/B tests are assigned per
  /// device. An experiment this device is already in keeps its variant when the
  /// id changes; to have a signed-in user get the same variant on every device,
  /// call this before [init]. Pass null to clear it.
  static Future<void> setUserId(String? userId) => _invoke<void>('setUserId', {'userId': userId});

  static Future<String?> getUserId() => _invoke<String>('getUserId');

  /// Fetches the latest rules now instead of waiting for the next poll.
  static Future<void> refreshRules() => _invoke<void>('refreshRules');

  static Future<void> resetSession() => _invoke<void>('resetSession');

  /// Fires each time new rules arrive. Read feature values again when it does.
  static Stream<void> get onRulesUpdated {
    _ensureHandler();
    return _rulesUpdated.stream;
  }

  /// Sends A/B test assignments and feature values to your analytics tool. Call once, at startup.
  ///
  /// - [setUserProperty] receives one property per running experiment
  ///   (`af_<experiment>` = variant; an empty value when the experiment ends).
  /// - [logEvent] receives two events, each once per device, also across app restarts:
  ///   `appfig_experiment` (`experiment`, `variant`) when the device enters an experiment,
  ///   and `appfig_feature` (`feature`, `feature_value`) for each feature the app reads, sent
  ///   again only when the value this device gets changes.
  ///
  /// ```dart
  /// AppFig.connectAnalytics(
  ///   setUserProperty: (name, value) =>
  ///       FirebaseAnalytics.instance.setUserProperty(name: name, value: value.isEmpty ? null : value),
  ///   logEvent: (name, parameters) =>
  ///       FirebaseAnalytics.instance.logEvent(name: name, parameters: parameters),
  /// );
  /// ```
  ///
  /// Works the same whether or not a user id is set. It can be called before or
  /// after [init]; assignments made earlier are delivered when it is called.
  static Future<void> connectAnalytics({
    required FutureOr<void> Function(String name, String value) setUserProperty,
    required FutureOr<void> Function(String name, Map<String, Object> parameters) logEvent,
  }) {
    _ensureHandler();
    _analyticsSetUserProperty = setUserProperty;
    _analyticsLogEvent = logEvent;
    return _invoke<void>('connectAnalytics');
  }

  /// Fires when the SDK assigns this user to an A/B test variant.
  static Stream<AppFigVariant> get onVariantAssigned {
    _ensureHandler();
    return _variants.stream;
  }

  static Map<String, String>? _stringify(Map<String, Object?>? params) {
    if (params == null) return null;
    return {
      for (final entry in params.entries)
        if (entry.value != null) entry.key: '${entry.value}',
    };
  }
}
