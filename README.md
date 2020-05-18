# ligature-rocksdb

A library for storing Ligature's data model in the RocksDB database.

## Implementation Notes

Currently, ligature-xodus stores a single collection in a single RocksDB store.
Each store contains a series of entries with different prefixes.
Below is a reference of those prefixes.

### EntityIdCounter

### PredicateIdCounter

### LiteralIdCounter

### SPOC

### SOPC

### PSOC

### POSC

### OSPC

### OPSC

### CSPO

### PredicateToId

### LangLiteralToId

### StringLiteralToId

### BooleanLiteralToId

### LongLiteralToId

### DoubleLiteralToId
