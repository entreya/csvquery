# Architecture Overview

## High-Level System Architecture

`csvquery` operates as a hybrid system leveraging PHP for the developer API and Go for performance-critical operations.

```mermaid
graph TD
    User([User Application]) -->|Fluent API| PHP["PHP Library (CsvQuery)"]
    PHP -->|CLI Arguments / Socket| Go["Go Binary (csvquery)"]
    Go -->|File I/O| CSV["CSV File"]
    Go -->|File I/O| Index["Index Files (.cidx, .bloom)"]
    Go -->|Stream Offsets| PHP
    PHP -->|Seek & Read| CSV
    PHP -->|Hydrated Rows| User
```

## Communication Flow

### Query Execution
1. **PHP**: Builds a query using `ActiveQuery`.
2. **PHP**: Invokes `GoBridge` to execute the Go binary.
3. **Go**: Parses arguments, loads indexes, and scans for matches.
4. **Go**: Streams matching **file offsets** (byte positions) back to PHP.
   - *Optimization*: Only offsets are transferred, not full rows.
5. **PHP**: Receives offsets, seeks the file pointer to the specific location, and reads the row.

```mermaid
sequenceDiagram
    participant App
    participant PHP as CsvQuery (PHP)
    participant Bridge as GoBridge
    participant Go as Go Process
    participant FS as File System

    App->>PHP: find()->where(...)->all()
    PHP->>Bridge: query(csv, indexDir, where)
    Bridge->>Go: spawn process (args)
    Go->>FS: Load Indexes
    Go->>FS: Scan CSV (Simd)
    Go-->>Bridge: Stream Offsets (stdout)
    loop For Each Offset
        Bridge-->>PHP: Yield Offset
        PHP->>FS: fseek(offset)
        PHP->>FS: fgets()
        PHP-->>App: Hydrated Row
    end
```
