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

db.Importe.aggregate([
    {
        $project: {
            _id: 0,
            codDepto: "$COD_DEPTO",
            depto: "$DEPARTAMENTO"
        }
    },
    {
        $merge: {
            into: "departamentos",
            on: "codDepto",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.departamentos.find();

db.Importe.aggregate([
    {
        $project: {
            _id: 0,
            codDepto: "$COD_DEPTO",
            codMunicipio: "$COD_MUNI",
            municipio: "$MUNICIPIO"
        }
    },
    {
        $merge: {
            into: "municipios",
            on: "codMunicipio",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.municipios.find();

db.Importe.aggregate([
    {
        $group: {
            _id: "$UNIDAD"
        }
    },
    {
        $project: {
            _id: 0,
            unidad: "$_id",
        }
    },
    {
        $merge: {
            into: "unidades",
            on: "unidad",
            whenMatched: "merge",
            whenNotMatched: "insert"
        }
    }
]);

db.unidades.find()

db.Importe.aggregate([
    {
        $addFields: {
            fechaHecho: {
                $dateFromString: {
                    dateString: "$FECHA HECHO",
                    format: "%d/%m/%Y",
                    timezone: "UTC",
                }
            }
        }
    },
    {
        $lookup: {
            from: "unidades",
            localField: "UNIDAD",
            foreignField: "unidad",
            as: "unidadInfo"
        }
        /* Desde la colección de unidades bajo la llave unidad
        llamados los campos UNIDAD ahora tendrán el nombre de unidadInfo */
    },

    {
        $set: {
            idUnidad: {
                $arrayElemAt: ["$unidadInfo._id", 0]
            }
        }
    },

    {
        $project: {
            fecha_hecho: 1,
            codMunicipio: "$COD_MUNI",
            cantidad: "$CANTIDAD",
            idUnidad: 1,
        }
    },
    {
        $merge: {
            into: "incautaciones"
        }
    }
]);

db.incautaciones.find();

// Ejercicios propuestos
// 1.

db.incautaciones.aggregate([
    {$lookup: {from: "municipios", localField: "codMunicipio", foreignField: "codMunicipio", as: "codigoMunicipio" }},
    {$unwind: "$codigoMunicipio"},
    {$match: {"codigoMunicipio.municipio": {$regex: /^La/i}}}, {$group: {_id:"$codigoMunicipio.municipio", cantidadTotal: {$sum:"$cantidad"}}}]);

// 2.
db.incautaciones.aggregate([
    {$lookup: {from: "municipios", localField: "codMunicipio", foreignField: "codMunicipio", as: "codigoMunicipio" }},
    {$unwind: "$codigoMunicipio"},
    {$match: {"codigoMunicipio.municipio": {$regex: /al$/i}}}, {$group: {_id:"$codigoMunicipio.municipio", cantidadTotal: {$sum:"$cantidad"}}}]);