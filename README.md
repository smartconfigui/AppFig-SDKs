# AppFig SDKs

Pre-built AppFig SDKs for **iOS**, **Android**, **Unity**, and **React Web**.
Install with your package manager, or download the SDK for your platform from the folders above.

## Install

**iOS, Swift Package Manager:** in Xcode, File > Add Package Dependencies, then enter
`https://github.com/smartconfigui/AppFig-SDKs`

**iOS, CocoaPods:**

    pod 'AppFigSDK', :git => 'https://github.com/smartconfigui/AppFig-SDKs.git', :tag => 'v2.5.0'

**Android, Gradle:** add `maven { url 'https://jitpack.io' }` to your repositories, then

    implementation 'com.github.smartconfigui:AppFig-SDKs:v2.5.0'

**Flutter:** see [`flutter/`](flutter/README.md).

**Unity, Package Manager:** Window > Package Manager > + > Add package from git URL, then enter
`https://github.com/smartconfigui/AppFig-SDKs.git?path=/unity#v2.5.0`

In Swift: `import AppFigSDK`. In Kotlin: `import com.appfig.sdk.AppFig`.

## What is AppFig?

AppFig is a **client-side feature engine** for apps and games — instant feature flags, audience targeting, and real-time rule evaluation with zero latency.

## Documentation

Full guides for each platform are available at:

https://appfig.com/docs

## Support & Feedback

Questions or suggestions?
Email us anytime: **hello@appfig.com**
