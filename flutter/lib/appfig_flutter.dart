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
  }) {
    _ensureHandler();
    return _channel.invokeMethod<void>('init', {
      'companyId': companyId,
      'tenantId': tenantId,
      'env': env,
      'apiKey': apiKey,
      'autoRefresh': autoRefresh,
      'pollIntervalMs': pollInterval.inMilliseconds,
      'debugMode': debugMode,
      'readyTimeoutMs': readyTimeout.inMilliseconds,
    });
  }

  /// The value of [feature] for this user, or null when no rule matches.
  static Future<String?> getFeatureValue(String feature) =>
      _channel.invokeMethod<String>('getFeatureValue', {'name': feature});

  /// Whether [feature] is on for this user.
  static Future<bool> isFeatureEnabled(String feature) async =>
      (await _channel.invokeMethod<bool>('isFeatureEnabled', {'name': feature})) ?? false;

  /// Whether any rules are loaded yet.
  static Future<bool> hasRulesLoaded() async => (await _channel.invokeMethod<bool>('hasRulesLoaded')) ?? false;

  /// Records an event that rules can target on. Parameter values are sent as text.
  static Future<void> logEvent(String name, {Map<String, Object?>? parameters}) =>
      _channel.invokeMethod<void>('logEvent', {'name': name, 'params': _stringify(parameters)});

  static Future<void> logScreenView(String screenName, {String? previousScreen}) =>
      _channel.invokeMethod<void>('logScreenView', {'screenName': screenName, 'previousScreen': previousScreen});

  static Future<void> setUserProperty(String key, Object value) =>
      _channel.invokeMethod<void>('setUserProperty', {'key': key, 'value': '$value'});

  static Future<void> removeUserProperty(String key) => _channel.invokeMethod<void>('removeUserProperty', {'key': key});

  static Future<void> setDeviceProperty(String key, Object value) =>
      _channel.invokeMethod<void>('setDeviceProperty', {'key': key, 'value': '$value'});

  static Future<void> removeDeviceProperty(String key) => _channel.invokeMethod<void>('removeDeviceProperty', {'key': key});

  static Future<void> setAppVersion(String version) => _channel.invokeMethod<void>('setAppVersion', {'version': version});

  /// A stable id for this user. A/B test assignments are per user id; changing
  /// it re-evaluates them. Pass null to clear it.
  static Future<void> setUserId(String? userId) => _channel.invokeMethod<void>('setUserId', {'userId': userId});

  static Future<String?> getUserId() => _channel.invokeMethod<String>('getUserId');

  /// Fetches the latest rules now instead of waiting for the next poll.
  static Future<void> refreshRules() => _channel.invokeMethod<void>('refreshRules');

  static Future<void> resetSession() => _channel.invokeMethod<void>('resetSession');

  /// Fires each time new rules arrive. Read feature values again when it does.
  static Stream<void> get onRulesUpdated {
    _ensureHandler();
    return _rulesUpdated.stream;
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
