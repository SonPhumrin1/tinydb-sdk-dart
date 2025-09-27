import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:test/test.dart';
import 'package:tinydb_client/tinydb_client.dart';

void main() {
  const baseCollectionResponse = {
    'id': 'users',
    'tenant_id': 'tenant-1',
    'name': 'users',
    'app_id': null,
    'schema_json':
        '{"fields":{"name":{"type":"string","required":true}}}',
    'primary_key_field': 'uid',
    'primary_key_type': 'string',
    'primary_key_auto': false,
    'created_at': '2024-01-01T00:00:00Z',
    'updated_at': '2024-01-01T00:00:00Z',
    'deleted_at': null
  };

  test('sync creates or updates collection schema', () async {
    final requests = <http.Request>[];
    final mock = MockClient((request) async {
      requests.add(request);
      if (request.method == 'POST' &&
          request.url.path == '/api/collections') {
        final body = jsonDecode(request.body) as Map<String, dynamic>;
        expect(body['name'], 'users');
        expect(body['schema'], isNotNull);
        return http.Response(
          jsonEncode(baseCollectionResponse),
          200,
          headers: {'content-type': 'application/json'},
        );
      }
      return http.Response('not found', 404);
    });

    final client = TinyDBClient(
      endpoint: 'https://db.example.com',
      apiKey: 'demo-key',
      httpClient: mock,
    );

    final collection = await client.collection<JsonMap>('users').schema(
      CollectionSchemaDefinition(fields: {
        'name': FieldDefinition.string(required: true),
      }),
    ).primaryKey(
      const PrimaryKeyConfig(field: 'uid', type: PrimaryKeyType.string),
    ).sync();

    expect(collection.details.name, 'users');
    expect(collection.details.primaryKeyField, 'uid');
    expect(requests, hasLength(1));
    expect(requests.first.headers['X-API-Key'], 'demo-key');
  });

  test('create document returns hydrated record', () async {
    var callIndex = 0;
    final mock = MockClient((request) async {
      callIndex += 1;
  if (callIndex == 1 &&
      request.method == 'POST' &&
      request.url.path == '/api/collections') {
        return http.Response(
          jsonEncode(baseCollectionResponse),
          200,
          headers: {'content-type': 'application/json'},
        );
      }

      if (request.method == 'POST' &&
          request.url.path == '/api/collections/users/documents') {
        final body = jsonDecode(request.body) as Map<String, dynamic>;
        expect(body['name'], 'Alice');
        return http.Response(
          jsonEncode({
            'id': 'doc-1',
            'tenant_id': 'tenant-1',
            'collection_id': 'users',
            'key': 'doc-1',
            'key_numeric': null,
            'data': {
              '_doc_id': 'doc-1',
              'name': 'Alice',
            },
            'created_at': '2024-01-01T00:00:00Z',
            'updated_at': '2024-01-01T00:00:00Z',
            'deleted_at': null,
          }),
          200,
          headers: {'content-type': 'application/json'},
        );
      }

      return http.Response('not found', 404);
    });

    final client = TinyDBClient(
      endpoint: 'https://db.example.com',
      apiKey: 'demo-key',
      httpClient: mock,
    );

    final collection = await client.collection<JsonMap>('users').sync();

    final created = await collection.create({'name': 'Alice'});

    expect(created.id, 'doc-1');
    expect(created.data['name'], 'Alice');
    expect(created.data['_doc_id'], 'doc-1');
  });
}
