# Tag Propagation — mapa del flujo
- Piensa como un ingeniero de software
- Contesta en español a no ser que te pida lo contrario
- Antes de implementar codigo o cambios, explicame ideas y opciones de arquitectura con pros y contras




Este documento describe el flujo de **propagación de governed tags (Unity Catalog) de Silver a Gold** y todos los ficheros que intervienen, para servir de referencia antes de modificar este sistema.

## Resumen del flujo (Gold Tag Propagation)

Cada vez que `HypatiaUseCaseExecution` escribe una tabla Gold, se dispara automáticamente — sin flag, sin opt-in — una propagación de tags desde las columnas Silver de las que proviene (vía `system.access.column_lineage`) hacia la columna Gold recién creada, aplicando `ALTER TABLE ... SET TAGS` en real (no dry-run).

```
HypatiaUseCaseExecution.propagateToGold()              [impl/HypatiaUseCaseExecution.scala:20]
  └─ createGoldTable(entity)                            [impl/HypatiaUseCaseExecution.scala:25]
       └─ writeDataToDeltaTable(...)                     [HypatiaDataset.scala:94]
            └─ HypatiaDatasetManager.writeDataToDeltaTable(...)   [HypatiaDatasetManager.scala:100]
                 └─ (tras escritura OK) propagateTagsToTable(databaseName, datasetName)  [HypatiaDatasetManager.scala:132]
                      └─ new TagPropagationService(spark)
                      └─ tagService.getTagPropagationPlan(goldCatalog, goldSchema, goldTable,
                                                            silverLayer = "semantic"+ENV_SOURCE, daysBack = 30)
                      └─ tagService.executeTagPropagation(plan, dryRun = false)   ⚠️ aplica tags de verdad
```

### Comportamiento crítico a tener en cuenta
- **`dryRun = true` es el default de `executeTagPropagation`**, pero el único call site real en producción (`HypatiaDatasetManager.propagateTagsToTable`) lo invoca explícitamente con `dryRun = false`. No asumir que el flujo es "seguro por defecto" en producción.
- Toda `propagateTagsToTable` está envuelta en un `Try` que **traga cualquier excepción** y solo loggea un warning — un fallo en la propagación de tags nunca hace fallar el job de escritura de la tabla Gold.
- El linaje tiene ventana temporal (`daysBack = 30`, hardcoded en la llamada): si la tabla Gold no se ha procesado/leído recientemente en `system.access.column_lineage`, no se encontrará linaje y no se propaga nada.
- `silverLayer` se construye como `"semantic" + ENV_SOURCE` (p.ej. `semantic_dev`, `semantic_pre`, `semantic` en pro) — el filtro de fuentes Silver es `catalog.startsWith(silverLayer)` (`getColumnLineageToLayer`), no coincidencia exacta.
- Solo se propagan tags Silver cuya `key` **no exista ya** en la columna Gold (`existingTagKeys`) — no se sobreescriben tags Gold existentes, solo se añaden los que faltan.
- El catalog/schema Gold se parsean de `databaseName.split("\\.")` (formato `"catalog.schema"`); si no tiene al menos 2 partes, se loggea warning y no se propaga nada.

## Ficheros que intervienen (todos en `hypatia-lib-scala`, módulo `hyptmanager` salvo el DAO)

### Núcleo del servicio — `hyptmanager/src/main/scala/com/gestamp/hypatia/utils/tag_propagation/`
- **`Column.scala`** — modelos de dominio: `Tag(key, value)`, `ColumnInfo(catalog, schema, tableName, columnName, tags)`, `ColumnLineageWithTags`, `TagPropagationPlan(targetColumn, tagsToPropagate, sourceColumns)`.
- **`SystemTableAccessor.scala`** — trait `SystemTableAccessor` (abstrae el acceso a system tables para poder testear) + `DatabricksSystemTableAccessor`, implementación real que hace `spark.sql(...)` contra:
  - `system.information_schema.column_tags` (tags por columna)
  - `system.information_schema.columns` (listar columnas de una tabla)
  - `system.access.column_lineage` (linaje directo y recursivo vía CTE con `WHERE ... NOT ARRAY_CONTAINS(path, ...)` para evitar ciclos, hasta `maxDepth` niveles)
- **`TagPropagationService.scala`** — orquestador, recibe un `SystemTableAccessor` opcional (para inyectar el mock en tests; si es `null` usa `DatabricksSystemTableAccessor`):
  - `getColumnTags` / `getAllColumnTags` / `getTableColumns` — wrappers sobre el accessor con `try/catch` que devuelven `Seq.empty`/`Map.empty` si falla (loggea con `println`, no logger).
  - `getColumnLineageToLayer` — linaje **directo** (1 nivel) filtrado por `targetLayer` (default `"semantic"`), enriquecido con tags de cada fuente.
  - `getRecursiveColumnLineageToLayer` — linaje **recursivo** (default `targetLayerPattern = "silver"`, `maxDepth = 5`), usa `getRecursiveColumnLineage` del accessor. **No se usa actualmente desde `HypatiaDatasetManager`** (que solo usa la versión no recursiva vía `getTagPropagationPlan`).
  - `getTagPropagationPlan` — por cada columna Gold, calcula tags nuevas a propagar (Silver tags que no estén ya en Gold) y construye un `TagPropagationPlan`.
  - `generateTagPropagationSQL` — genera `ALTER TABLE <gold> ALTER COLUMN <col> SET TAGS ('<key>' = '<value>');` por cada tag a propagar.
  - `generatePropagationReport` — `DataFrame` resumen (para inspección manual/notebook).
  - `executeTagPropagation(plan, dryRun = true)` — ejecuta (o simula) los `ALTER TABLE`.

### Punto de entrada / disparador — `hyptmanager/src/main/scala/com/gestamp/hypatia/`
- **`HypatiaDatasetManager.scala`**
  - `writeDataToDeltaTable` (línea ~100) — tras escribir con éxito, llama a `propagateTagsToTable`.
  - `propagateTagsToTable` (línea ~132) — **único call site real** que instancia `TagPropagationService` y ejecuta la propagación con `dryRun = false`. Envuelve todo en `Try` para no romper el flujo de escritura si falla.
- **`HypatiaDataset.scala`** — clase abstracta base; `writeDataToDeltaTable` (línea 94) delega en `HypatiaDatasetManager.writeDataToDeltaTable`, pasando `catalog = jobParams.use_case.area + ENV_SOURCE` y `schema = jobParams.use_case.name`.
- **`impl/HypatiaUseCaseExecution.scala`** — implementación concreta de `HypatiaDataset`; `propagateToGold()` → `createGoldTable(entity)` es quien arranca toda la cadena, una vez por cada `Entity` del `UseCaseProcess` (en paralelo o secuencial según `process.parallelProcessing`).

### Constantes de entorno — `commons/src/main/scala/com/gestamp/hypatia/utils/CommonsConstants.scala`
- `ENV_SOURCE` — sufijo de entorno (`_dev`, `_pre`, `_lab`, o `""` en `pro`), usado para componer `silverLayer = "semantic" + ENV_SOURCE` y el catalog Gold.
- `SEMANTIC` — equivalente resuelto a `semantic_dev/pre/pro`, usado en otras partes del repo (Flujo 1, ver abajo) pero no directamente en `TagPropagationService`.

### Tests — `hyptmanager/src/test/scala/com/gestamp/hypatia/`
- **`MockSystemTableAccessor.scala`** — implementación de `SystemTableAccessor` que lee de temp views Spark (`mock_column_tags`, `mock_columns`, `mock_column_lineage`) en lugar de `system.*`.
- **`HypatiaTagPropagationTest.scala`** — `FunSuite` con `SparkSession local[*]`, inyecta el mock en `TagPropagationService`, cubre: `getColumnTags`, `getAllColumnTags`, `getTableColumns`, `getColumnLineageToLayer`, `getTagPropagationPlan`, `generateTagPropagationSQL`, `generatePropagationReport`, `executeTagPropagation` (dry run), `ColumnInfo.fullTableName/fullColumnName`.
  - ⚠️ No hay test de `HypatiaDatasetManager.propagateTagsToTable` (el call site real) ni de `getRecursiveColumnLineageToLayer`.

## Flujo relacionado — Silver Tag Assignment (Flujo 1, NO es "propagation")

Es el flujo que **asigna** tags a Silver por primera vez (a partir de metadatos en PostgreSQL), antes de que el Flujo 2 los propague a Gold. Vive solo parcialmente en este repo:

- `commons/.../postgresql/model/ColumnTag.scala` — `case class ColumnTag(id, column_name, tag_name)`
- `commons/.../postgresql/idao/iColumnTagDao.scala` — trait con `getAllColumnTags`
- `commons/.../postgresql/dao/ColumnTagDao.scala` — `SELECT id, column_name, tag_name FROM metadata.column_tag` (PostgreSQL)
- El **`SilverTagAssigner`** que consume este DAO y hace el `ALTER TABLE ... SET TAGS` sobre Silver **vive en el repo `hypatia-semantic`**, no aquí. Existió una versión en este repo en la rama `feature/silver_tag_asignation` (módulo `integration`, commit `c5a825eb`) pero fue eliminada (commit `71649c2d "only dao"`); se decidió dejar solo el DAO aquí.

## Lo que NO gestiona este repo
- Los governed tags de Databricks en sí (alta/baja) → `hypatia-terraform-modules`.
- Grupos ABAC ni sus membresías → BD `hypatiasecurity` + Airflow DAG.
- El `SilverTagAssigner` (Flujo 1) → `hypatia-semantic`.