<?php
declare(strict_types = 1);

namespace Courier\Console\Traits;

use Courier\Contracts\Providers\ProviderInterface;
use Courier\Contracts\Inflectors\InflectorInterface;

trait ResolveQueueListTrait {
  /**
   * @param string[] $options
   *
   * @return string[]
   */
  private static function resolveQueueList(
    ProviderInterface $provider,
    InflectorInterface $inflector,
    array $options = []
  ): array {
    $queueList = [];
    foreach ($provider as $class) {
      $queueList[] = $inflector->resolve($class);
    }

    if (in_array('all', $options, true) === true) {
      return $queueList;
    }

    $resolvedList = [];
    foreach ($options as $option) {
      $prefix = false;
      if (str_ends_with($option, '*') === true) {
        $prefix = true;
        $option = substr($option, 0, -1);
      }

      $suffix = false;
      if (str_starts_with($option, '*') === true) {
        $suffix = true;
        $option = substr($option, 1);
      }

      $resolvedList = array_merge(
        $resolvedList,
        array_filter(
          $queueList,
          static function (string $queueName) use ($prefix, $suffix, $option): bool {
            $queueName = strtolower($queueName);
            $option = strtolower($option);
            return (
              ($prefix === true && str_starts_with($queueName, $option)) ||
              ($suffix === true && str_ends_with($queueName, $option)) ||
              ($prefix === true && $suffix === true && str_contains($queueName, $option)) ||
              $queueName === $option
            );
          }
        )
      );
    }

    return array_values(array_unique($resolvedList));
  }
}
