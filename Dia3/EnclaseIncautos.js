use EnclaseIncautos;

db.createCollection("departamentos");
db.createCollection("municipios");
db.createCollection("unidades");
db.createCollection("incautaciones");

// La indexación crea una llave guia perteneciente al campo de todos los datos en una columna
db.departamentos.createIndex({ codDepto: 1 }, { unique: true });
db.municipios.createIndex({ codMunicipio: 1 }, { unique: true });
db.unidades.createIndex({ unidad: 1 }, { unique: true });
db.incautaciones.createIndex({ fechaHecho: 1 });
db.incautaciones.createIndex({ codMunicipio: 1 });

