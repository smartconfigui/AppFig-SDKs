# appfig_flutter

AppFig for Flutter: feature flags, remote config and A/B tests evaluated on the
device. This package is a thin layer over the native AppFig SDKs for Android and
iOS, so rules behave exactly as they do in a native app.

## Install

In `pubspec.yaml`:

```yaml
dependencies:
  appfig_flutter:
    git:
      url: https://github.com/smartconfigui/AppFig-SDKs.git
      path: flutter
      ref: v2.3.0
```

Nothing else is needed: the Android SDK is pulled from JitPack by the plugin and
the iOS framework ships inside it. Minimum versions: Android 5.0 (API 21), iOS 12.

## Use

```dart
import 'package:appfig_flutter/appfig_flutter.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await AppFig.init(companyId: 'your-company', tenantId: 'your-app', env: 'prod', apiKey: 'your-api-key');
  runApp(const MyApp());
}

// Read a value (null when no rule matches)
final maxSongs = int.tryParse(await AppFig.getFeatureValue('max_songs') ?? '') ?? 25;

// Tell AppFig what the user does and who they are; rules can target on both
AppFig.logEvent('add_song', parameters: {'source': 'import'});
AppFig.setUserProperty('subscription_tier', 'free');

// Re-read values when new rules arrive
AppFig.onRulesUpdated.listen((_) => refreshLimits());
```

`init` completes when rules are available or after `readyTimeout` (5 seconds by
default), so values read right after it are real.

## A/B tests and analytics

A/B tests work without a user id: a device without one is assigned by its
device id, and an assignment stays with the device across restarts and sign-in.
Call `setUserId` before `init` if a signed-in user should get the same variant
on every device.

To see experiments in your analytics tool, connect it once at startup:

```dart
AppFig.connectAnalytics(
  setUserProperty: (name, value) =>
      FirebaseAnalytics.instance.setUserProperty(name: name, value: value.isEmpty ? null : value),
  logEvent: (name, parameters) =>
      FirebaseAnalytics.instance.logEvent(name: name, parameters: parameters),
);
```

`setUserProperty` receives one property per running experiment
(`af_<experiment>` = variant). `logEvent` receives two events, each once per device:

| Event | Parameters | When |
|---|---|---|
| `appfig_experiment` | `experiment`, `variant` | The device enters an experiment. |
| `appfig_feature` | `feature`, `feature_value` | The app reads a feature. Sent again only when the value this device gets changes. |

A feature the app never reads sends nothing. The two callbacks can call any analytics tool.

## API

`init`, `getFeatureValue`, `isFeatureEnabled`, `hasRulesLoaded`, `logEvent`,
`logScreenView`, `setUserProperty`, `removeUserProperty`, `setDeviceProperty`,
`removeDeviceProperty`, `setAppVersion`, `setUserId`, `getUserId`,
`refreshRules`, `resetSession`, `connectAnalytics`, and the streams
`onRulesUpdated` and `onVariantAssigned`.

Documentation: https://appfig.com/docs
