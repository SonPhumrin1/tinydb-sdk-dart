import 'dart:io';
import 'dart:convert';

/// Runtime version reader using package config
class RuntimeVersionReader {
  /// Gets version from package config at runtime
  /// This works when the package is installed as a dependency
  static Future<String?> getPackageVersion(String packageName) async {
    try {
      String? packageConfigPath = Platform.packageConfig;
      if (packageConfigPath == null) return null;
      
      Uri packageConfigUri = Uri.parse(packageConfigPath);
      File configFile = File.fromUri(packageConfigUri);
      
      if (!configFile.existsSync()) return null;
      
      String content = await configFile.readAsString();
      Map<String, dynamic> config = json.decode(content);
      
      List<dynamic> packages = config['packages'] ?? [];
      
      for (var package in packages) {
        if (package['name'] == packageName) {
          String? rootUri = package['rootUri'];
          if (rootUri != null) {
            Uri packageRoot = packageConfigUri.resolve(rootUri);
            File pubspecFile = File.fromUri(packageRoot.resolve('pubspec.yaml'));
            
            if (pubspecFile.existsSync()) {
              String pubspecContent = await pubspecFile.readAsString();
              return _parseVersionFromYaml(pubspecContent);
            }
          }
        }
      }
      
      return null;
    } catch (e) {
      return null;
    }
  }
  
  /// Simple YAML parser for version field only
  static String? _parseVersionFromYaml(String yamlContent) {
    List<String> lines = yamlContent.split('\n');
    
    for (String line in lines) {
      String trimmed = line.trim();
      
      if (trimmed.startsWith('version:')) {
        String versionPart = trimmed.substring(8).trim();
        
        // Remove quotes if present
        if (versionPart.startsWith('"') && versionPart.endsWith('"')) {
          versionPart = versionPart.substring(1, versionPart.length - 1);
        } else if (versionPart.startsWith("'") && versionPart.endsWith("'")) {
          versionPart = versionPart.substring(1, versionPart.length - 1);
        }
        
        return versionPart.isNotEmpty ? versionPart : null;
      }
    }
    
    return null;
  }
}