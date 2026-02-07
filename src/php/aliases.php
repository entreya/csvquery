<?php
/**
 * CsvQuery - Backward Compatibility Facade
 *
 * This file provides backward compatibility for users importing from the
 * root CsvQuery namespace. It creates class aliases pointing to the new
 * modular namespace structure.
 *
 * Usage remains unchanged:
 * ```php
 * use CsvQuery\CsvQuery;
 * use CsvQuery\ActiveQuery;
 * use CsvQuery\GoBridge;
 * ```
 *
 * New modular imports (optional):
 * ```php
 * use CsvQuery\Core\CsvQuery;
 * use CsvQuery\Query\ActiveQuery;
 * use CsvQuery\Bridge\GoBridge;
 * ```
 *
 * @package CsvQuery
 */

declare(strict_types=1);

namespace CsvQuery;

// Core module
class_alias(\CsvQuery\Core\CsvQuery::class, 'CsvQuery\CsvQuery');

// Query module
class_alias(\CsvQuery\Query\ActiveQuery::class, 'CsvQuery\ActiveQuery');
class_alias(\CsvQuery\Query\Command::class, 'CsvQuery\Command');

// Bridge module
class_alias(\CsvQuery\Bridge\GoBridge::class, 'CsvQuery\GoBridge');
class_alias(\CsvQuery\Bridge\SocketClient::class, 'CsvQuery\SocketClient');
