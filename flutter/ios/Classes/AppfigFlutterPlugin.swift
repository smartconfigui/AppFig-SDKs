import AppFigSDK
import Flutter
import UIKit

/// Forwards what the native SDK reports about experiments to Dart, where the app
/// hands it to its analytics tool (see AppFig.connectAnalytics in Dart).
final class AppfigAnalyticsBridge: AppFigAnalyticsProvider {
  private let channel: FlutterMethodChannel

  init(channel: FlutterMethodChannel) {
    self.channel = channel
  }

  func setUserProperty(key: String, value: String) {
    let channel = self.channel
    DispatchQueue.main.async {
      channel.invokeMethod("analyticsUserProperty", arguments: ["key": key, "value": value])
    }
  }

  func trackExperimentAssigned(experimentKey: String, variantName: String) {
    let channel = self.channel
    DispatchQueue.main.async {
      channel.invokeMethod("analyticsExperimentAssigned", arguments: ["experimentKey": experimentKey, "variantName": variantName])
    }
  }

  func trackFeatureSeen(featureName: String, value: String) {
    let channel = self.channel
    DispatchQueue.main.async {
      channel.invokeMethod("analyticsFeatureSeen", arguments: ["featureName": featureName, "value": value])
    }
  }
}

/// Bridges the Dart API (lib/appfig_flutter.dart) to the native AppFig iOS SDK.
public class AppfigFlutterPlugin: NSObject, FlutterPlugin {
  private let channel: FlutterMethodChannel
  private var analyticsBridge: AppfigAnalyticsBridge?

  init(channel: FlutterMethodChannel) {
    self.channel = channel
    super.init()
  }

  public static func register(with registrar: FlutterPluginRegistrar) {
    let channel = FlutterMethodChannel(name: "com.appfig/flutter", binaryMessenger: registrar.messenger())
    registrar.addMethodCallDelegate(AppfigFlutterPlugin(channel: channel), channel: channel)
  }

  public func handle(_ call: FlutterMethodCall, result: @escaping FlutterResult) {
    let args = call.arguments as? [String: Any] ?? [:]
    func required(_ key: String) -> String? {
      if let value = args[key] as? String { return value }
      result(FlutterError(code: "BAD_ARGS", message: "\(key) is required", details: nil))
      return nil
    }

    switch call.method {
    case "init":
      initialize(args, result: result)
    case "getFeatureValue":
      if let name = required("name") { result(AppFig.getFeatureValue(name)) }
    case "isFeatureEnabled":
      if let name = required("name") { result(AppFig.isFeatureEnabled(name)) }
    case "hasRulesLoaded":
      result(AppFig.hasRulesLoaded())
    case "logEvent":
      if let name = required("name") {
        AppFig.logEvent(name: name, parameters: args["params"] as? [String: String])
        result(nil)
      }
    case "logScreenView":
      if let screenName = required("screenName") {
        AppFig.logScreenView(screenName: screenName, previousScreen: args["previousScreen"] as? String)
        result(nil)
      }
    case "setUserProperty":
      if let key = required("key"), let value = required("value") {
        AppFig.setUserProperty(key: key, value: value)
        result(nil)
      }
    case "removeUserProperty":
      if let key = required("key") { AppFig.removeUserProperty(key: key); result(nil) }
    case "setDeviceProperty":
      if let key = required("key"), let value = required("value") {
        AppFig.setDeviceProperty(key: key, value: value)
        result(nil)
      }
    case "removeDeviceProperty":
      if let key = required("key") { AppFig.removeDeviceProperty(key: key); result(nil) }
    case "setAppVersion":
      if let version = required("version") { AppFig.setAppVersion(version); result(nil) }
    case "setUserId":
      AppFig.setUserId(args["userId"] as? String)
      result(nil)
    case "getUserId":
      result(AppFig.getUserId())
    case "refreshRules":
      AppFig.refreshRules()
      result(nil)
    case "resetSession":
      AppFig.resetSession()
      result(nil)
    case "connectAnalytics":
      if analyticsBridge == nil {
        let bridge = AppfigAnalyticsBridge(channel: channel)
        analyticsBridge = bridge
        AppFig.registerAnalyticsProvider(bridge)
      }
      result(nil)
    default:
      result(FlutterMethodNotImplemented)
    }
  }

  private func initialize(_ args: [String: Any], result: @escaping FlutterResult) {
    guard let companyId = args["companyId"] as? String, let tenantId = args["tenantId"] as? String,
          let env = args["env"] as? String, let apiKey = args["apiKey"] as? String else {
      result(FlutterError(code: "BAD_ARGS", message: "companyId, tenantId, env and apiKey are required", details: nil))
      return
    }
    // Answer Dart once: when rules are ready, or after the timeout, whichever is first.
    // Without this the first getFeatureValue can run before the first rules arrive.
    var answered = false
    let answer: () -> Void = {
      DispatchQueue.main.async {
        if !answered {
          answered = true
          result(nil)
        }
      }
    }
    let channel = self.channel
    AppFig.setOnVariantAssigned { experimentKey, variantName in
      DispatchQueue.main.async {
        channel.invokeMethod("onVariantAssigned", arguments: ["experimentKey": experimentKey, "variantName": variantName])
      }
    }
    AppFig.initialize(
      companyId: companyId,
      tenantId: tenantId,
      env: env,
      apiKey: apiKey,
      autoRefresh: args["autoRefresh"] as? Bool ?? true,
      pollInterval: (args["pollIntervalMs"] as? NSNumber)?.doubleValue ?? 43_200_000,
      debugMode: args["debugMode"] as? Bool ?? false,
      onReady: { answer() },
      onRulesUpdated: {
        DispatchQueue.main.async { channel.invokeMethod("onRulesUpdated", arguments: nil) }
        answer()
      }
    )
    let timeoutMs = (args["readyTimeoutMs"] as? NSNumber)?.doubleValue ?? 5000
    DispatchQueue.main.asyncAfter(deadline: .now() + timeoutMs / 1000) { answer() }
  }
}
