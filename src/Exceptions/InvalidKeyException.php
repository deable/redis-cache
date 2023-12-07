<?php

declare(strict_types=1);

namespace Deable\RedisCache\Exceptions;

use Psr\SimpleCache\InvalidArgumentException;

/**
 * Class InvalidKeyException
 *
 * @package Deable\RedisCache\Exceptions
 */
final class InvalidKeyException extends RedisCacheException implements RedisCacheThrowable, InvalidArgumentException
{

}
