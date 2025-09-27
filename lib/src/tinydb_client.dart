import 'dart:async';
import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:meta/meta.dart';

typedef JsonMap = Map<String, dynamic>;

enum FieldType {
  string,
  number,
  boolean,
  uuid,
  date,
  datetime,
  object,
  array,
}

extension FieldTypeValue on FieldType {
  String get value => name;
}

enum PrimaryKeyType { uuid, number, string }

extension PrimaryKeyTypeValue on PrimaryKeyType {
  String get value => name;
}

@immutable
class FieldDefinition {
  final FieldType type;
  final bool required;
  final bool allowNull;
  final String? description;
  final List<String>? enumeration;
  final FieldDefinition? items;

  const FieldDefinition({
    required this.type,
    this.required = false,
    this.allowNull = false,
    this.description,
    this.enumeration,
    this.items,
  });

  const FieldDefinition._internal({
    required this.type,
    this.required = false,
    this.allowNull = false,
    this.description,
    this.enumeration,
    this.items,
  });

  factory FieldDefinition.string({
    bool required = false,
    bool allowNull = false,
    String? description,
    List<String>? values,
  }) => FieldDefinition._internal(
        type: FieldType.string,
        required: required,
        allowNull: allowNull,
        description: description,
        enumeration: values,
      );

  factory FieldDefinition.number({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) => FieldDefinition._internal(
        type: FieldType.number,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.boolean({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) => FieldDefinition._internal(
        type: FieldType.boolean,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.uuid({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) => FieldDefinition._internal(
        type: FieldType.uuid,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.date({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) => FieldDefinition._internal(
        type: FieldType.date,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.datetime({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) => FieldDefinition._internal(
        type: FieldType.datetime,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.object({
    bool required = false,
    bool allowNull = false,
    String? description,
  }) => FieldDefinition._internal(
        type: FieldType.object,
        required: required,
        allowNull: allowNull,
        description: description,
      );

  factory FieldDefinition.array({
    bool required = false,
    bool allowNull = false,
    String? description,
    FieldDefinition? items,
  }) => FieldDefinition._internal(
        type: FieldType.array,
        required: required,
        allowNull: allowNull,
        description: description,
        items: items,
      );

  JsonMap toJson() {
    final map = <String, dynamic>{
      'type': type.value,
    };
    if (required) map['required'] = true;
    if (allowNull) map['allowNull'] = true;
    if (description != null) map['description'] = description;
    if (enumeration != null) map['enum'] = enumeration;
    if (items != null) map['items'] = items!.toJson();
    return map;
  }
}

@immutable
class CollectionSchemaDefinition {
  final Map<String, FieldDefinition> fields;
  final String? description;

  const CollectionSchemaDefinition({
    required this.fields,
    this.description,
  });

  JsonMap toJson() => {
        'fields': fields.map((key, value) => MapEntry(key, value.toJson())),
        if (description != null) 'description': description,
      };
}

@immutable
class PrimaryKeyConfig {
  final String? field;
  final PrimaryKeyType? type;
  final bool? auto;

  const PrimaryKeyConfig({this.field, this.type, this.auto});

  JsonMap toJson() => {
        if (field != null) 'field': field,
        if (type != null) 'type': type!.value,
        if (auto != null) 'auto': auto,
      };
}

@immutable
class Pagination {
  final int? limit;
  final int? offset;
  final int? count;
  final String? nextCursor;
  final bool? hasMore;

  const Pagination({
    this.limit,
    this.offset,
    this.count,
    this.nextCursor,
    this.hasMore,
  });

  factory Pagination.fromJson(dynamic json) {
    if (json is! Map<String, dynamic>) {
      return const Pagination();
    }
    return Pagination(
      limit: json['limit'] as int?,
      offset: json['offset'] as int?,
      count: json['count'] as int?,
      nextCursor: json['next_cursor'] as String?,
      hasMore: json['has_more'] as bool?,
    );
  }
}

@immutable
class ListResult<T extends Map<String, dynamic>> {
  final List<DocumentRecord<T>> items;
  final Pagination pagination;

  const ListResult({required this.items, required this.pagination});
}

@immutable
class QueryResult<T extends Map<String, dynamic>> {
  final List<DocumentRecord<T>> items;
  final Pagination pagination;

  const QueryResult({required this.items, required this.pagination});
}

@immutable
class SyncParams {
  final DateTime? since;
  final String? cursor;
  final int? limit;
  final bool? includeDeleted;

  const SyncParams({this.since, this.cursor, this.limit, this.includeDeleted});

  Map<String, String> toQuery() {
    final map = <String, String>{};
    if (since != null) map['since'] = since!.toUtc().toIso8601String();
    if (cursor != null) map['cursor'] = cursor!;
    if (limit != null) map['limit'] = '$limit';
    if (includeDeleted != null) {
      map['include_deleted'] = includeDeleted! ? 'true' : 'false';
    }
    return map;
  }
}

@immutable
class SyncChange<T extends Map<String, dynamic>> {
  final String changeType;
  final DocumentRecord<T> document;

  const SyncChange({required this.changeType, required this.document});
}

@immutable
class SyncResult<T extends Map<String, dynamic>> {
  final List<SyncChange<T>> items;
  final Pagination pagination;
  final String? since;

  const SyncResult({
    required this.items,
    required this.pagination,
    this.since,
  });
}

@immutable
class CollectionDetails {
  final String id;
  final String tenantId;
  final String name;
  final String? appId;
  final Map<String, dynamic>? schema;
  final String? primaryKeyField;
  final String? primaryKeyType;
  final bool? primaryKeyAuto;
  final String createdAt;
  final String updatedAt;
  final String? deletedAt;

  const CollectionDetails({
    required this.id,
    required this.tenantId,
    required this.name,
    this.appId,
    this.schema,
    this.primaryKeyField,
    this.primaryKeyType,
    this.primaryKeyAuto,
    required this.createdAt,
    required this.updatedAt,
    this.deletedAt,
  });

  factory CollectionDetails.fromJson(Map<String, dynamic> json) {
    dynamic schemaValue;
    final schemaRaw = json['schema_json'];
    if (schemaRaw is String && schemaRaw.isNotEmpty) {
      try {
        schemaValue = jsonDecode(schemaRaw);
      } catch (_) {
        schemaValue = {'_raw': schemaRaw};
      }
    } else if (json['schema'] is Map<String, dynamic>) {
      schemaValue = json['schema'] as Map<String, dynamic>;
    }
    return CollectionDetails(
      id: json['id'] as String,
      tenantId: json['tenant_id'] as String,
      name: json['name'] as String,
      appId: json['app_id'] as String?,
      schema: schemaValue is Map<String, dynamic>
          ? Map<String, dynamic>.from(schemaValue)
          : null,
      primaryKeyField: json['primary_key_field'] as String?,
      primaryKeyType: json['primary_key_type'] as String?,
      primaryKeyAuto: json['primary_key_auto'] as bool?,
      createdAt: json['created_at'] as String,
      updatedAt: json['updated_at'] as String,
      deletedAt: json['deleted_at'] as String?,
    );
  }
}

@immutable
class DocumentRecord<T extends Map<String, dynamic>> {
  final String id;
  final String tenantId;
  final String collectionId;
  final String key;
  final num? keyNumeric;
  final T data;
  final String createdAt;
  final String updatedAt;
  final String? deletedAt;

  const DocumentRecord({
    required this.id,
    required this.tenantId,
    required this.collectionId,
    required this.key,
    required this.data,
    required this.createdAt,
    required this.updatedAt,
    this.keyNumeric,
    this.deletedAt,
  });
}

class TinyDBException implements Exception {
  final String message;
  final int status;
  final String? code;
  final dynamic details;

  TinyDBException(this.message, this.status, {this.code, this.details});

  @override
  String toString() =>
      'TinyDBException(status: $status, code: $code, message: $message)';
}

class TinyDBClient {
  final String _endpoint;
  final String _apiKey;
  final String? _appId;
  final http.Client _httpClient;
  final bool _ownsClient;

  TinyDBClient({
    required String endpoint,
    required String apiKey,
    String? appId,
    http.Client? httpClient,
  })  : _endpoint = _normalizeEndpoint(endpoint),
        _apiKey = apiKey,
        _appId = appId,
        _httpClient = httpClient ?? http.Client(),
        _ownsClient = httpClient == null;

  static String _normalizeEndpoint(String endpoint) {
    final trimmed = endpoint.trim();
    if (trimmed.endsWith('/')) {
      return trimmed.substring(0, trimmed.length - 1);
    }
    return trimmed;
  }

  CollectionBuilder<T> collection<T extends Map<String, dynamic>>(
    String name,
  ) {
    if (name.trim().isEmpty) {
      throw ArgumentError('collection name is required');
    }
    return CollectionBuilder<T>(this, name.trim());
  }

  Future<List<CollectionDetails>> collections() async {
    final response = await _request<List<dynamic>>(
      method: 'GET',
      path: '/api/collections',
    );
    return response
        .cast<Map<String, dynamic>>()
        .map(CollectionDetails.fromJson)
        .toList(growable: false);
  }

  Future<CollectionDetails> describeCollection(String name) async {
    final all = await collections();
    final lowered = name.toLowerCase();
    final match =
        all.firstWhere((c) => c.name.toLowerCase() == lowered, orElse: () {
      throw TinyDBException(
        'Collection $name not found',
        404,
        code: 'collection_not_found',
      );
    });
    return match;
  }

  Future<CollectionDetails> ensureCollection({
    required String name,
    CollectionSchemaDefinition? schema,
    PrimaryKeyConfig? primaryKey,
  }) async {
    final body = <String, dynamic>{'name': name};
    final schemaJson = schema != null ? jsonEncode(schema.toJson()) : null;
    if (schemaJson != null) {
      body['schema'] = schemaJson;
    }
    if (_appId != null) {
      body['app_id'] = _appId;
    }
    if (primaryKey != null) {
      final pkJson = primaryKey.toJson();
      if (pkJson.isNotEmpty) {
        body['primary_key'] = pkJson;
      }
    }

    try {
      final created = await _request<Map<String, dynamic>>(
        method: 'POST',
        path: '/api/collections',
        body: body,
      );
      return CollectionDetails.fromJson(created);
    } on TinyDBException catch (error) {
      if (error.status == 409) {
        if (schemaJson != null) {
          final updated = await _request<Map<String, dynamic>>(
            method: 'PUT',
            path: '/api/collections/${Uri.encodeComponent(name)}',
            body: {'schema': schemaJson},
          );
          return CollectionDetails.fromJson(updated);
        }
        return describeCollection(name);
      }
      rethrow;
    }
  }

  Future<void> close() async {
    if (_ownsClient) {
      _httpClient.close();
    }
  }

  Future<R> _request<R>({
    required String method,
    required String path,
    Map<String, String?>? query,
    Object? body,
    R Function(dynamic data)? transform,
  }) async {
    final uri = _buildUri(path, query);
    final request = http.Request(method, uri);
    request.headers['Accept'] = 'application/json';
    request.headers['X-API-Key'] = _apiKey;
    if (_appId != null) {
      request.headers['X-App-ID'] = _appId!;
    }
    if (body != null) {
      if (body is String) {
        request.headers['Content-Type'] =
            request.headers['Content-Type'] ?? 'application/json';
        request.body = body;
      } else if (body is List<int>) {
        request.bodyBytes = body;
      } else {
        request.headers['Content-Type'] = 'application/json';
        request.body = jsonEncode(body);
      }
    }

    http.Response response;
    try {
      final streamed = await _httpClient.send(request);
      response = await http.Response.fromStream(streamed);
    } catch (error) {
      throw TinyDBException('Network error: $error', 0);
    }

    if (response.statusCode >= 400) {
      throw _parseError(response);
    }

    if (R == void || response.body.isEmpty) {
      return (null as R);
    }

    dynamic decoded;
    try {
      decoded = jsonDecode(response.body);
    } catch (_) {
      decoded = response.body;
    }

    if (transform != null) {
      return transform(decoded);
    }

    return decoded as R;
  }

  Uri _buildUri(String path, Map<String, String?>? query) {
    final normalized = path.startsWith('/') ? path : '/$path';
    final uri = Uri.parse('$_endpoint$normalized');
    if (query == null || query.isEmpty) {
      return uri;
    }
    final filtered = <String, String>{};
    for (final entry in query.entries) {
      final value = entry.value;
      if (value != null) {
        filtered[entry.key] = value;
      }
    }
    return uri.replace(queryParameters: {...uri.queryParameters, ...filtered});
  }

  TinyDBException _parseError(http.Response response) {
    dynamic payload;
    if (response.body.isNotEmpty) {
      try {
        payload = jsonDecode(response.body);
      } catch (_) {
        payload = null;
      }
    }
    final message = payload is Map<String, dynamic>
        ? (payload['message'] ??
            payload['error_description'] ??
            payload['error'] ??
            response.reasonPhrase ??
            'Request failed')
        : (response.reasonPhrase ?? 'Request failed');
    final code = payload is Map<String, dynamic> ? payload['code'] : null;
    return TinyDBException(
      message.toString(),
      response.statusCode,
      code: code?.toString(),
      details: payload,
    );
  }
}

class CollectionBuilder<T extends Map<String, dynamic>>
    implements Future<CollectionClient<T>> {
  final TinyDBClient _client;
  final String _name;
  CollectionSchemaDefinition? _schema;
  PrimaryKeyConfig? _primaryKey;

  CollectionBuilder(this._client, this._name);

  CollectionBuilder<T> schema(CollectionSchemaDefinition definition) {
    _schema = definition;
    return this;
  }

  CollectionBuilder<T> primaryKey(PrimaryKeyConfig config) {
    _primaryKey = config;
    return this;
  }

  Future<CollectionClient<T>> sync() async {
    final meta = await _client.ensureCollection(
      name: _name,
      schema: _schema,
      primaryKey: _primaryKey,
    );
    return CollectionClient<T>(_client, _name, meta);
  }

  Future<CollectionClient<T>> _resolve() async {
    if (_schema != null || _primaryKey != null) {
      return sync();
    }
    final meta = await _client.ensureCollection(name: _name);
    return CollectionClient<T>(_client, _name, meta);
  }

  @override
  Stream<CollectionClient<T>> asStream() => _resolve().asStream();

  @override
  Future<CollectionClient<T>> catchError(Function onError,
          {bool Function(Object error)? test}) =>
      _resolve().catchError(onError, test: test);

  @override
  Future<R> then<R>(FutureOr<R> Function(CollectionClient<T> value) onValue,
          {Function? onError}) =>
      _resolve().then(onValue, onError: onError);

  @override
  Future<CollectionClient<T>> timeout(Duration timeLimit,
          {FutureOr<CollectionClient<T>> Function()? onTimeout}) =>
      _resolve().timeout(timeLimit, onTimeout: onTimeout);

  @override
  Future<CollectionClient<T>> whenComplete(FutureOr<void> Function() action) =>
      _resolve().whenComplete(action);
}

class ListOptions {
  final int? limit;
  final int? offset;
  final bool includeDeleted;
  final List<String>? select;
  final Map<String, dynamic>? filters;

  const ListOptions({
    this.limit,
    this.offset,
    this.includeDeleted = false,
    this.select,
    this.filters,
  });

  Map<String, String> toQuery() {
    final map = <String, String>{};
    if (limit != null) map['limit'] = '$limit';
    if (offset != null) map['offset'] = '$offset';
    if (includeDeleted) map['include_deleted'] = 'true';
    if (select != null && select!.isNotEmpty) {
      map['select'] = select!.join(',');
    }
    if (filters != null) {
      filters!.forEach((key, value) {
        if (value != null) {
          map['f.$key'] = value.toString();
        }
      });
    }
    return map;
  }
}

class CollectionClient<T extends Map<String, dynamic>> {
  final TinyDBClient _client;
  final String name;
  CollectionDetails _metadata;

  CollectionClient(this._client, this.name, this._metadata);

  CollectionDetails get details => _metadata;

  CollectionBuilder<T> schema(CollectionSchemaDefinition definition) {
    final builder = CollectionBuilder<T>(_client, name).schema(definition);
    final existingType = _metadata.primaryKeyType != null
        ? _primaryKeyTypeFromString(_metadata.primaryKeyType!)
        : null;
    if (_metadata.primaryKeyField != null ||
        existingType != null ||
        _metadata.primaryKeyAuto != null) {
      builder.primaryKey(PrimaryKeyConfig(
        field: _metadata.primaryKeyField,
        type: existingType,
        auto: _metadata.primaryKeyAuto,
      ));
    }
    return builder;
  }

  CollectionBuilder<T> primaryKey(PrimaryKeyConfig config) =>
      CollectionBuilder<T>(_client, name).primaryKey(config);

  Future<CollectionDetails> refresh() async {
    _metadata = await _client.describeCollection(name);
    return _metadata;
  }

  Future<ListResult<T>> list({ListOptions options = const ListOptions()}) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'GET',
      path: '/api/collections/${Uri.encodeComponent(name)}/documents',
      query: options.toQuery(),
    );
    final items = (response['items'] as List<dynamic>? ?? [])
        .map((item) => _parseDocument<T>(item as Map<String, dynamic>))
        .toList(growable: false);
    return ListResult<T>(
      items: items,
      pagination: Pagination.fromJson(response['pagination']),
    );
  }

  Future<DocumentRecord<T>> get(String id, {bool pk = false}) => pk
      ? getByPrimaryKey(id)
      : _fetchDocument(
          '/api/collections/${Uri.encodeComponent(name)}/documents/${Uri.encodeComponent(id)}',
        );

  Future<DocumentRecord<T>> getByPrimaryKey(String key) => _fetchDocument(
        '/api/collections/${Uri.encodeComponent(name)}/documents/primary/${Uri.encodeComponent(key)}',
      );

  Future<DocumentRecord<T>> create(Map<String, dynamic> doc) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'POST',
      path: '/api/collections/${Uri.encodeComponent(name)}/documents',
      body: doc,
    );
    return _parseDocument<T>(response);
  }

  Future<List<DocumentRecord<T>>> createMany(
    List<Map<String, dynamic>> docs,
  ) async {
    if (docs.isEmpty) return const [];
    final response = await _client._request<Map<String, dynamic>>(
      method: 'POST',
      path: '/api/collections/${Uri.encodeComponent(name)}/documents/bulk',
      body: docs,
    );
    return (response['items'] as List<dynamic>? ?? [])
        .map((item) => _parseDocument<T>(item as Map<String, dynamic>))
        .toList(growable: false);
  }

  Future<DocumentRecord<T>> update(
    String id,
    Map<String, dynamic> doc,
  ) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'PUT',
      path: '/api/collections/${Uri.encodeComponent(name)}/documents/${Uri.encodeComponent(id)}',
      body: doc,
    );
    return _parseDocument<T>(response);
  }

  Future<DocumentRecord<T>> patch(
    String id,
    Map<String, dynamic> doc,
  ) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'PATCH',
      path: '/api/collections/${Uri.encodeComponent(name)}/documents/${Uri.encodeComponent(id)}',
      body: doc,
    );
    return _parseDocument<T>(response);
  }

  Future<void> delete(dynamic id) async {
    if (id is Iterable) {
      for (final docId in id) {
        await delete(docId);
      }
      return;
    }
    await _client._request<void>(
      method: 'DELETE',
      path: '/api/collections/${Uri.encodeComponent(name)}/documents/${Uri.encodeComponent(id.toString())}',
    );
  }

  Future<void> purge(dynamic id) async {
    if (id is Iterable) {
      for (final docId in id) {
        await purge(docId);
      }
      return;
    }
    await _client._request<void>(
      method: 'DELETE',
      path: '/api/collections/${Uri.encodeComponent(name)}/documents/${Uri.encodeComponent(id.toString())}/purge',
      query: {'confirm': 'true'},
    );
  }

  Future<QueryResult<T>> query(Map<String, dynamic> request) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'POST',
      path: '/api/collections/${Uri.encodeComponent(name)}/query',
      body: request,
    );
    final items = (response['items'] as List<dynamic>? ?? [])
        .map((item) => _parseDocument<T>(item as Map<String, dynamic>))
        .toList(growable: false);
    return QueryResult<T>(
      items: items,
      pagination: Pagination.fromJson(response['pagination']),
    );
  }

  Future<SyncResult<T>> sync([SyncParams params = const SyncParams()]) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'GET',
      path: '/api/collections/${Uri.encodeComponent(name)}/sync',
      query: params.toQuery(),
    );
    final items = (response['items'] as List<dynamic>? ?? [])
        .map((item) => item as Map<String, dynamic>)
        .map((item) => SyncChange<T>(
              changeType: item['change_type'] as String,
              document: _parseDocument<T>(
                  item['document'] as Map<String, dynamic>),
            ))
        .toList(growable: false);
    return SyncResult<T>(
      items: items,
      pagination: Pagination.fromJson(response['pagination']),
      since: response['since'] as String?,
    );
  }

  Future<dynamic> schemaJson() async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'GET',
      path: '/api/collections/${Uri.encodeComponent(name)}/schema',
    );
    return response['schema'];
  }

  Future<DocumentRecord<T>> _fetchDocument(String path) async {
    final response = await _client._request<Map<String, dynamic>>(
      method: 'GET',
      path: path,
    );
    return _parseDocument<T>(response);
  }
}

PrimaryKeyType? _primaryKeyTypeFromString(String value) {
  for (final type in PrimaryKeyType.values) {
    if (type.value == value) {
      return type;
    }
  }
  return null;
}

DocumentRecord<T> _parseDocument<T extends Map<String, dynamic>>(
  Map<String, dynamic> payload,
) {
  Map<String, dynamic> data = {};
  final rawData = payload['data'];
  if (rawData is String && rawData.isNotEmpty) {
    try {
      final decoded = jsonDecode(rawData);
      if (decoded is Map<String, dynamic>) {
        data = Map<String, dynamic>.from(decoded);
      }
    } catch (_) {
      data = {'_raw': rawData};
    }
  } else if (rawData is Map<String, dynamic>) {
    data = Map<String, dynamic>.from(rawData);
  }

  final docId = (data['_doc_id'] ?? payload['id'])?.toString();
  if (docId != null) {
    data['_doc_id'] = docId;
  }

  return DocumentRecord<T>(
    id: payload['id'] as String,
    tenantId: payload['tenant_id'] as String,
    collectionId: payload['collection_id'] as String,
    key: payload['key'] as String,
    keyNumeric: payload['key_numeric'] is num
        ? payload['key_numeric'] as num
        : null,
    data: Map<String, dynamic>.from(data) as T,
    createdAt: payload['created_at'] as String,
    updatedAt: payload['updated_at'] as String,
    deletedAt: payload['deleted_at'] as String?,
  );
}
