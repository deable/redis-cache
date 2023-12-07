<?php

declare(strict_types=1);

namespace Deable\RedisCache;

use DateInterval;
use Deable\Redis\Exceptions\RedisThrowable;
use Deable\Redis\RedisClient;
use Deable\RedisCache\Exceptions\InvalidKeyException;
use Deable\RedisCache\Exceptions\RedisCacheException;
use Deable\Serializer\DefaultSerializer;
use Deable\Serializer\Serializer;
use Psr\SimpleCache\CacheInterface;
use Throwable;

/**
 * Class RedisCache
 *
 * @package Deable\RedisCache
 */
final class RedisCache implements CacheInterface
{
	private const LIMIT_BATCH = 512;

	private RedisClient $client;

	private int $defaultTtl;

	private string $prefix;

	private Serializer $serializer;

	public function __construct(RedisClient $client, int $defaultTtl = 604800, string $prefix = 'cache', ?Serializer $serializer = null)
	{
		$this->client = $client;
		$this->defaultTtl = $defaultTtl;
		$this->prefix = $prefix;
		$this->serializer = $serializer ?: new DefaultSerializer();
	}

	/**
	 * Fetches a value from the cache.
	 * @param string $key The unique key of this item in the cache.
	 * @param mixed $default Default value to return if the key does not exist.
	 * @return mixed The value of the item from the cache, or $default in case of cache miss.
	 * @throws InvalidKeyException
	 * @throws RedisCacheException
	 */
	public function get($key, $default = null)
	{
		$cacheKey = $this->formatKey($key);
		try {
			return $this->result($this->client->get($cacheKey), $default);
		} catch (RedisThrowable $e) {
			throw new RedisCacheException("Cannot load cache item under key '$cacheKey'.", 0, $e);
		}
	}

	/**
	 * Persists data in the cache, uniquely referenced by a key with an optional expiration TTL time.
	 * @param string $key The key of the item to store.
	 * @param mixed $value The value of the item to store, must be serializable.
	 * @param null|int|\DateInterval $ttl Optional. The TTL value of this item.
	 * @return bool True on success and false on failure.
	 * @throws InvalidKeyException
	 * @throws RedisCacheException
	 */
	public function set($key, $value, $ttl = null): bool
	{
		$cacheKey = $this->formatKey($key);
		try {
			return $this->client->setEX($cacheKey, $ttl ?? $this->defaultTtl, $this->serializer->encode($value));
		} catch (RedisThrowable $e) {
			throw new RedisCacheException("Cannot save cache item under key '$cacheKey'.", 0, $e);
		}
	}

	/**
	 * Delete an item from the cache by its unique key.
	 * @param string $key The unique cache key of the item to delete.
	 * @return bool True if the item was successfully removed. False if there was an error.
	 * @throws InvalidKeyException
	 * @throws RedisCacheException
	 */
	public function delete($key): bool
	{
		$cacheKey = $this->formatKey($key);
		try {
			$this->client->unlink($cacheKey);

			return true;
		} catch (RedisThrowable $e) {
			throw new RedisCacheException("Cannot delete cache item under key '$cacheKey'.", 0, $e);
		}
	}

	/**
	 * Wipes clean the entire cache's keys.
	 * @return bool True on success and false on failure.
	 * @throws InvalidKeyException
	 * @throws RedisCacheException
	 */
	public function clear(): bool
	{
		$cacheKey = $this->formatKey('*');
		try {
			do {
				$keys = $this->client->scan($cursor, $cacheKey, self::LIMIT_BATCH);
				if (!empty($keys)) {
					$this->client->unlink(...$keys);
				}
			} while ($cursor);

			return true;
		} catch (RedisThrowable $e) {
			throw new RedisCacheException("Cannot clear cache with key pattern '$cacheKey'.", 0, $e);
		}
	}

	/**
	 * Obtains multiple cache items by their unique keys.
	 * @param iterable $keys A list of keys that can obtained in a single operation.
	 * @param mixed $default Default value to return for keys that do not exist.
	 * @return iterable A list of key => value pairs. Cache keys that do not exist or are stale will have $default as value.
	 * @throws InvalidKeyException
	 */
	public function getMultiple($keys, $default = null): iterable
	{
		$items = [];
		$chunks = array_chunk((array)$keys, self::LIMIT_BATCH);
		foreach ($chunks as $chunk) {
			$cacheKeys = array_map(fn ($key) => $this->formatKey($key), $chunk);
			$results = $this->client->mGet($cacheKeys);
			$results = array_map(fn (string $data) => $this->result($data, $default), $results);
			$items = array_merge($items, array_combine($chunk, $results));
		}

		return $items;
	}

	/**
	 * Persists a set of key => value pairs in the cache, with an optional TTL.
	 * @param iterable $values A list of key => value pairs for a multiple-set operation.
	 * @param null|int|DateInterval $ttl Optional. The TTL value of this item.
	 * @return bool True on success and false on failure.
	 * @throws InvalidKeyException
	 */
	public function setMultiple($values, $ttl = null): bool
	{
		$result = true;
		foreach ($values as $key => $value) {
			$result = $result && $this->set($key, $value, $ttl);
		}

		return $result;
	}

	/**
	 * Deletes multiple cache items in a single operation.
	 * @param iterable $keys A list of string-based keys to be deleted.
	 * @return bool True if the items were successfully removed. False if there was an error.
	 * @throws InvalidKeyException
	 * @throws RedisCacheException
	 */
	public function deleteMultiple($keys): bool
	{
		try {
			$chunks = array_chunk((array)$keys, self::LIMIT_BATCH);
			foreach ($chunks as $chunk) {
				$cacheKeys = array_map(fn ($key) => $this->formatKey($key), $chunk);
				$this->client->unlink(...$cacheKeys);
			}

			return true;
		} catch (RedisThrowable $e) {
			throw new RedisCacheException("Cannot delete multiple cache items.", 0, $e);
		}
	}

	/**
	 * Determines whether an item is present in the cache.
	 * @param string $key The cache item key.
	 * @return bool
	 * @throws InvalidKeyException
	 * @throws RedisCacheException
	 */
	public function has($key): bool
	{
		$key = $this->formatKey($key);
		try {
			return $this->client->exists($key) == 1;
		} catch (RedisThrowable $e) {
			throw new RedisCacheException("Cannot check if cache item under key '$key' exists.", 0, $e);
		}
	}

	private function formatKey($key): string
	{
		if (is_string($key)) {
			return sprintf('%s:%s', $this->prefix, $key);
		}
		try {
			return sprintf('%s:%s', $this->prefix, (string) $key);
		} catch (Throwable $e) {
			$class = get_class($key);
			throw new InvalidKeyException("Invalid cache key typed as '$class', cannot be converted to string.", 0, $e);
		}
	}

	/**
	 * @param mixed $data
	 * @param mixed $default
	 * @return mixed|null
	 */
	private function result($data, $default = null)
	{
		return empty($data) ? $default : $this->serializer->decode($data);
	}
}
