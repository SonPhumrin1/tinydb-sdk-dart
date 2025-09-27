import 'package:tinydb_client/tinydb_client.dart';

Future<void> main() async {
  final client = TinyDBClient(
    endpoint: 'http://localhost:8080',
    apiKey: 'demo-key',
  );

  final collection = await client.collection('users').schema(
    CollectionSchemaDefinition(fields: {
      'name': FieldDefinition.string(required: true),
      'role': FieldDefinition.string(),
      'age': FieldDefinition.number(),
    }),
  ).sync();

  final user = await collection.create({
    'name': 'Sambo',
    'role': 'Developer',
    'age': 29,
  });

  print('Created user: ${user.data}');
}
