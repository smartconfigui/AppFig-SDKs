# CocoaPods spec for the pre-built AppFig iOS SDK. 2.4.0 is filled in at release.
#   pod 'AppFigSDK', :git => 'https://github.com/smartconfigui/AppFig-SDKs.git', :tag => 'v2.4.0'
Pod::Spec.new do |s|
  s.name             = 'AppFigSDK'
  s.version          = '2.4.0'
  s.summary          = 'AppFig iOS SDK: client-side feature flags and remote configuration'
  s.description      = 'AppFig evaluates feature rules on the device: event, user and device targeting, A/B tests, caching and background sync.'
  s.homepage         = 'https://appfig.com'
  s.license          = { :type => 'Proprietary', :text => 'Copyright (c) AppFig. All rights reserved.' }
  s.author           = { 'AppFig' => 'hello@appfig.com' }
  s.source           = { :git => 'https://github.com/smartconfigui/AppFig-SDKs.git', :tag => "v#{s.version}" }
  s.platform         = :ios, '12.0'
  s.swift_version    = '5.0'
  s.vendored_frameworks = 'ios/AppFigSDK.xcframework'
  s.documentation_url = 'https://appfig.com/docs'
end
