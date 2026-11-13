package com.appfig.flutter

import android.content.Context
import android.os.Handler
import android.os.Looper
import com.appfig.sdk.AppFig
import io.flutter.embedding.engine.plugins.FlutterPlugin
import io.flutter.plugin.common.MethodCall
import io.flutter.plugin.common.MethodChannel

/** Bridges the Dart API (lib/appfig_flutter.dart) to the native AppFig Android SDK. */
class AppfigFlutterPlugin : FlutterPlugin, MethodChannel.MethodCallHandler {
    private lateinit var channel: MethodChannel
    private lateinit var context: Context
    private val main = Handler(Looper.getMainLooper())

    override fun onAttachedToEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        context = binding.applicationContext
        channel = MethodChannel(binding.binaryMessenger, "com.appfig/flutter")
        channel.setMethodCallHandler(this)
    }

    override fun onDetachedFromEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        channel.setMethodCallHandler(null)
    }

    override fun onMethodCall(call: MethodCall, result: MethodChannel.Result) {
        when (call.method) {
            "init" -> init(call, result)
            "getFeatureValue" -> withName(call, result) { result.success(AppFig.getFeatureValue(it)) }
            "isFeatureEnabled" -> withName(call, result) { result.success(AppFig.isFeatureEnabled(it)) }
            "hasRulesLoaded" -> result.success(AppFig.hasRulesLoaded())
            "logEvent" -> withArg(call, result, "name") {
                AppFig.logEvent(it, call.argument<Map<String, String>>("params"))
                result.success(null)
            }
            "logScreenView" -> withArg(call, result, "screenName") {
                AppFig.logScreenView(it, call.argument<String>("previousScreen"))
                result.success(null)
            }
            "setUserProperty" -> withKeyValue(call, result) { k, v -> AppFig.setUserProperty(k, v) }
            "removeUserProperty" -> withArg(call, result, "key") { AppFig.removeUserProperty(it); result.success(null) }
            "setDeviceProperty" -> withKeyValue(call, result) { k, v -> AppFig.setDeviceProperty(k, v) }
            "removeDeviceProperty" -> withArg(call, result, "key") { AppFig.removeDeviceProperty(it); result.success(null) }
            "setAppVersion" -> withArg(call, result, "version") { AppFig.setAppVersion(it); result.success(null) }
            "setUserId" -> { AppFig.setUserId(call.argument<String>("userId")); result.success(null) }
            "getUserId" -> result.success(AppFig.getUserId())
            "refreshRules" -> { AppFig.refreshRules(); result.success(null) }
            "resetSession" -> { AppFig.resetSession(); result.success(null) }
            else -> result.notImplemented()
        }
    }

    private fun init(call: MethodCall, result: MethodChannel.Result) {
        val companyId = call.argument<String>("companyId")
        val tenantId = call.argument<String>("tenantId")
        val env = call.argument<String>("env")
        val apiKey = call.argument<String>("apiKey")
        if (companyId == null || tenantId == null || env == null || apiKey == null) {
            result.error("BAD_ARGS", "companyId, tenantId, env and apiKey are required", null)
            return
        }
        // Answer Dart once: when rules are ready, or after the timeout, whichever is first.
        // Without this the first getFeatureValue can run before the first rules arrive.
        var answered = false
        val answer = Runnable {
            if (!answered) {
                answered = true
                result.success(null)
            }
        }
        AppFig.setOnVariantAssigned { experimentKey, variantName ->
            main.post { channel.invokeMethod("onVariantAssigned", mapOf("experimentKey" to experimentKey, "variantName" to variantName)) }
        }
        AppFig.init(
            context = context,
            companyId = companyId,
            tenantId = tenantId,
            env = env,
            apiKey = apiKey,
            autoRefresh = call.argument<Boolean>("autoRefresh") ?: true,
            pollInterval = (call.argument<Number>("pollIntervalMs") ?: 43200000).toLong(),
            debugMode = call.argument<Boolean>("debugMode") ?: false,
            onReady = { main.post(answer) },
            onRulesUpdated = {
                main.post {
                    channel.invokeMethod("onRulesUpdated", null)
                    answer.run()
                }
            }
        )
        main.postDelayed(answer, (call.argument<Number>("readyTimeoutMs") ?: 5000).toLong())
    }

    private inline fun withArg(call: MethodCall, result: MethodChannel.Result, key: String, block: (String) -> Unit) {
        val value = call.argument<String>(key)
        if (value == null) result.error("BAD_ARGS", "$key is required", null) else block(value)
    }

    private inline fun withName(call: MethodCall, result: MethodChannel.Result, block: (String) -> Unit) =
        withArg(call, result, "name", block)

    private inline fun withKeyValue(call: MethodCall, result: MethodChannel.Result, block: (String, String) -> Unit) {
        val key = call.argument<String>("key")
        val value = call.argument<String>("value")
        if (key == null || value == null) {
            result.error("BAD_ARGS", "key and value are required", null)
        } else {
            block(key, value)
            result.success(null)
        }
    }
}
