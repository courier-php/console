<?php
declare(strict_types = 1);

namespace Courier\Console\Commands;

use Courier\Console\Traits\ResolveQueueListTrait;
use Courier\Contracts\Clients\CollectorInterface;
use Courier\Contracts\Inflectors\InflectorInterface;
use Courier\Contracts\Messages\MessageInterface;
use Courier\Contracts\Providers\ProviderInterface;
use Courier\Clients\Collector;
use Courier\Clients\ObservableCollector;
use Courier\Exceptions\ClientException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Command\SignalableCommandInterface;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand('courier:consume', 'Consume queued messages from one or more queues')]
final class ConsumeCommand extends Command implements SignalableCommandInterface {
  use ResolveQueueListTrait;

  private CollectorInterface $collector;
  private ProviderInterface $provider;
  private InflectorInterface $inflector;
  private bool $manualStop = false;

  protected function configure(): void {
    $this
      ->addOption(
        'config',
        'c',
        InputOption::VALUE_REQUIRED,
        'The configuration file to load'
      )
      ->addOption(
        'queue',
        null,
        InputOption::VALUE_REQUIRED | InputOption::VALUE_IS_ARRAY,
        'The names of the queues to process messages from (all = consume from all queues)',
        ['all']
      )
      ->addOption(
        'max-attempts',
        null,
        InputOption::VALUE_REQUIRED,
        'Number of times to attempt a message before marking message as failed (0 = unlimited retry)',
        0
      )
      ->addOption(
        'retry-until',
        null,
        InputOption::VALUE_REQUIRED,
        'The number of seconds to retry a message before marking it as failed (0 = retry indefinitely)',
        0
      )
      ->addOption(
        'timeout',
        null,
        InputOption::VALUE_REQUIRED,
        'The number of seconds a child process can run (0 = no time limit)'
      )
      ->addOption(
        'once',
        null,
        InputOption::VALUE_NONE,
        'Only process the next message on the queue'
      )
      ->addOption(
        'max-messages',
        null,
        InputOption::VALUE_REQUIRED,
        'The number of messages to process before stopping (0 = unlimited messages processing)',
        0
      )
      ->addOption(
        'stop-when-empty',
        null,
        InputOption::VALUE_NONE,
        'Stop when the queue is empty'
      )
      ->addOption(
        'max-time',
        null,
        InputOption::VALUE_REQUIRED,
        'The maximum number of seconds the worker should run (0 = run until manually stopped)',
        0
      )
      ->addOption(
        'sleep',
        null,
        InputOption::VALUE_REQUIRED,
        'Number of seconds to sleep when no message is available (0 = no sleep)',
        0
      )
      ->addOption(
        'backoff',
        null,
        InputOption::VALUE_REQUIRED,
        'The number of seconds to wait before retrying a failed message (0 = immediate requeue)',
        0
      )
      ->addOption(
        'stats',
        null,
        InputOption::VALUE_NONE,
        'Display statistics about message consumption'
      );
  }

  protected function execute(InputInterface $input, OutputInterface $output): int {
    // this is a simple collector which can't handle this command's options
    if ($this->collector instanceof Collector) {
      $output->writeln('Warning: To control collector behavior, use ObservableCollector.');
      $output->writeln('Ignored options: max-attempts, retry-until, timeout?, once, max-messages, stop-when-empty, max-time, sleep, backoff, stats');
    }

    $autoStop = false;
    $accepted = 0;
    $requeued = 0;
    $rejected = 0;
    if ($this->collector instanceof ObservableCollector) {
      $maxAttempts = (int)$input->getOption('max-attempts');
      if ($maxAttempts > 0) {
        $this->collector->addObserver(
          'received',
          function (string $queueName, ?MessageInterface $message = null) use ($maxAttempts): void {
            static $cache = [];
            if (
              $message->hasProperty('isRedelivery') === false ||
              (bool)$message->getProperty('isRedelivery') === false ||
              $message->hasProperty('id') === false
            ) {
              return;
            }

            if (isset($cache[$message->getProperty('id')]) === false) {
              $cache[$message->getProperty('id')] = 0;
            }

            $cache[$message->getProperty('id')]++;
            if ($cache[$message->getProperty('id')] >= $maxAttempts) {
              unset($cache[$message->getProperty('id')]);

              throw new ClientException('Message rejected due to max attempts limit');
            }
          }
        );
      }

      $retryUntil = (int)$input->getOption('retry-until');
      if ($retryUntil > 0) {
        $this->collector->addObserver(
          'received',
          function (string $queueName, ?MessageInterface $message = null) use ($retryUntil): void {
            if (
              $message->hasProperty('isRedelivery') === false ||
              (bool)$message->getProperty('isRedelivery') === false ||
              $message->hasProperty('timestamp') === false
            ) {
              return;
            }

            if ((int)$message->getProperty('timestamp') + $retryUntil >= time()) {
              throw new ClientException('Message rejected due to timeout');
            }
          }
        );
      }

      // $timeout = (int)$input->getOption('timeout');
      // if ($timeout > 0) {
      //   $function = function (string $queueName, ?MessageInterface $message = null) use ($timeout): void {
      //     static $cache = [];
      //     if ($message->hasProperty('id') === false) {
      //       return;
      //     }

      //     if (isset($cache[$message->getProperty('id')]) === false) {
      //       $cache[$message->getProperty('id')] = time();

      //       return;
      //     }


      //   };
      //   $this->collector->addObserver('start', $function);
      //   $this->collector->addObserver('done', $function);
      // }

      if ((bool)$input->getOption('once') === true) {
        $this->collector->addObserver(
          'done',
          function (string $queueName, ?MessageInterface $message = null): void {
            $this->stop();
          }
        );
      }

      $maxMessages = (int)$input->getOption('max-messages');
      if ($maxMessages > 0) {
        $this->collector->addObserver(
          'done',
          function (string $queueName, ?MessageInterface $message = null) use ($maxMessages): void {
            static $messageCount = 0;
            $messageCount++;
            if ($messageCount >= $maxMessages) {
              $this->stop();
            }
          }
        );
      }

      if ((bool)$input->getOption('stop-when-empty') === true) {
        $this->collector->addObserver(
          'empty-queue',
          function (string $queueName, ?MessageInterface $message = null): void {
            $this->stop();
          }
        );
      }

      $maxTime = (int)$input->getOption('max-time');
      if ($maxTime > 0) {
        $startTime = time();
        $this->collector->addObserver(
          'start',
          function (string $queueName, ?MessageInterface $message = null) use ($maxTime, $startTime): void {
            if ((time() - $startTime) >= $maxTime) {
              $this->stop();
            }
          }
        );
      }

      $sleep = (int)$input->getOption('sleep');
      if ($sleep > 0) {
        $this->collector->addObserver(
          'empty-queue',
          function (string $queueName, ?MessageInterface $message = null) use ($sleep): void {
            sleep($sleep);
          }
        );
      }

      $backoff = (int)$input->getOption('backoff');
      if ($backoff > 0) {
        $this->collector->addObserver(
          'received',
          function (string $queueName, ?MessageInterface $message = null) use ($backoff): void {
            if (
              $message->hasProperty('isRedelivery') === false ||
              (bool)$message->getProperty('isRedelivery') === false
            ) {
              return;
            }

            sleep($backoff);
          }
        );
      }

      if ((bool)$input->getOption('stats') === true) {
        $this->collector->addObserver(
          'accepted',
          function (string $queueName, ?MessageInterface $message = null) use (&$accepted): void {
            $accepted++;
          }
        );
        $this->collector->addObserver(
          'requeued',
          function (string $queueName, ?MessageInterface $message = null) use (&$requeued): void {
            $requeued++;
          }
        );
        $this->collector->addObserver(
          'rejected',
          function (string $queueName, ?MessageInterface $message = null) use (&$rejected): void {
            $rejected++;
          }
        );
      }

      $this->collector->addObserver(
        'stop',
        function (string $queueName, ?MessageInterface $message = null) use (&$autoStop): void {
          $autoStop = true;
        }
      );
    }

    $queues = self::resolveQueueList(
      $this->provider,
      $this->inflector,
      $input->getOption('queue')
    );

    while ($autoStop === false && $this->manualStop === false) {
      $this->collector->collect(...$queues);
    }

    if ((bool)$input->getOption('stats') === true) {
      echo 'Worker Statistics', PHP_EOL;
      echo '=================', PHP_EOL;
      echo 'Accepted Jobs: ', $accepted, PHP_EOL;
      echo 'Requeued Jobs: ', $requeued, PHP_EOL;
      echo 'Rejected Jobs: ', $rejected, PHP_EOL;
    }

    return Command::SUCCESS;
  }

  public function __construct(
    CollectorInterface $collector,
    ProviderInterface $provider,
    InflectorInterface $inflector
  ) {
    parent::__construct();

    $this->collector = $collector;
    $this->provider = $provider;
    $this->inflector = $inflector;
  }

  public function getSubscribedSignals(): array {
    return [SIGINT, SIGTERM];
  }

  public function handleSignal(int $signal, int|false $previousExitCode = 0): int|false {
    $this->manualStop = true;
    $this->collector->stop();

    return false;
  }
}
