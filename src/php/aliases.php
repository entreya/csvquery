<?php
/**
 * Entreya CsvQuery - Backward Compatibility Facade
 *
 * This file provides backward compatibility for users importing from the
 * root Entreya\CsvQuery namespace. It creates class aliases pointing to the new
 * modular namespace structure.
 *
 * Usage remains unchanged:
 * ```php
 * use Entreya\CsvQuery\CsvQuery;
 * use Entreya\CsvQuery\ActiveQuery;
 * use Entreya\CsvQuery\GoBridge;
 * ```
 *
 * New modular imports (optional):
 * ```php
 * use Entreya\CsvQuery\Core\CsvQuery;
 * use Entreya\CsvQuery\Query\ActiveQuery;
 * use Entreya\CsvQuery\Bridge\GoBridge;
 * ```
 *
 * @package Entreya\CsvQuery
 */

declare(strict_types=1);

namespace Entreya\CsvQuery;

// Core module
class_alias(\Entreya\CsvQuery\Core\CsvQuery::class, 'Entreya\CsvQuery\CsvQuery');

// Query module
class_alias(\Entreya\CsvQuery\Query\ActiveQuery::class, 'Entreya\CsvQuery\ActiveQuery');
class_alias(\Entreya\CsvQuery\Query\Command::class, 'Entreya\CsvQuery\Command');

// Bridge module
class_alias(\Entreya\CsvQuery\Bridge\GoBridge::class, 'Entreya\CsvQuery\GoBridge');
class_alias(\Entreya\CsvQuery\Bridge\SocketClient::class, 'Entreya\CsvQuery\SocketClient');
