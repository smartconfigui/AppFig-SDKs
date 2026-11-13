// swift-tools-version:5.5
// Swift Package Manager manifest for the pre-built AppFig iOS SDK.
// Xcode: File > Add Package Dependencies > https://github.com/smartconfigui/AppFig-SDKs
import PackageDescription

let package = Package(
    name: "AppFigSDK",
    platforms: [.iOS(.v12)],
    products: [
        .library(name: "AppFigSDK", targets: ["AppFigSDK"])
    ],
    targets: [
        // The framework and its Swift module are both named AppFigSDK: `import AppFigSDK`.
        .binaryTarget(name: "AppFigSDK", path: "ios/AppFigSDK.xcframework")
    ]
)
