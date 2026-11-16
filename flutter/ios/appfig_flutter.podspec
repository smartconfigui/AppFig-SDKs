Pod::Spec.new do |s|
  s.name             = 'appfig_flutter'
  s.version          = '2.2.0'
  s.summary          = 'AppFig for Flutter (iOS side).'
  s.description      = 'Bridges the appfig_flutter Dart API to the native AppFig iOS SDK.'
  s.homepage         = 'https://appfig.com'
  s.license          = { :type => 'Proprietary', :text => 'Copyright (c) AppFig. All rights reserved.' }
  s.author           = { 'AppFig' => 'hello@appfig.com' }
  s.source           = { :path => '.' }
  s.source_files     = 'Classes/**/*'
  s.dependency 'Flutter'
  s.platform         = :ios, '12.0'
  s.swift_version    = '5.0'
  # The native SDK ships inside the plugin, so apps need no extra pod or search path.
  s.vendored_frameworks = 'Frameworks/AppFigSDK.xcframework'
  s.pod_target_xcconfig = { 'DEFINES_MODULE' => 'YES', 'EXCLUDED_ARCHS[sdk=iphonesimulator*]' => 'i386' }
end
