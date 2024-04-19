<?php
declare(strict_types = 1);

namespace Courier\Console\Commands;

use Courier\Console\Traits\ResolveQueueListTrait;
use Courier\Contracts\Inflectors\InflectorInterface;
use Courier\Contracts\Providers\ProviderInterface;
use Courier\Contracts\Transports\TransportInterface;
use DateTimeInterface;
use InvalidArgumentException;
use LogicException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Command\SignalableCommandInterface;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\ConsoleOutputInterface;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand('courier:monitor', 'Monitors the number of queued messages on one or more available queues')]
final class MonitorCommand extends Command implements SignalableCommandInterface {
  use ResolveQueueListTrait;

  private ProviderInterface $provider;
  private InflectorInterface $inflector;
  private TransportInterface $transport;
  private bool $shouldStop = false;

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
        'The names of the queues to monitor (all = monitor all queues)',
        ['all']
      )
      ->addOption(
        'interval',
        null,
        InputOption::VALUE_REQUIRED,
        'Interval between output updates (in seconds)',
        5
      )
      ->addOption(
        'max-messages',
        null,
        InputOption::VALUE_REQUIRED,
        'The maximum number of messages that can be on the queue before an alert is triggered',
        1000
      );
  }

  protected function execute(InputInterface $input, OutputInterface $output): int {
    if (($output instanceof ConsoleOutputInterface) === false) {
      throw new LogicException('This command accepts only an instance of "ConsoleOutputInterface"');
    }

    $queueList = self::resolveQueueList(
      $this->provider,
      $this->inflector,
      $input->getOption('queue')
    );
    sort($queueList);

    if ($queueList === []) {
      $output->writeln('Select at least one queue to monitor');

      return Command::FAILURE;
    }

    $interval = (int)$input->getOption('interval');
    if ($interval < 1) {
      throw new InvalidArgumentException('Interval option must be a non zero positive integer');
    }

    $maxMessages = (int)$input->getOption('max-messages');
    if ($maxMessages < 1) {
      throw new InvalidArgumentException('Max messages option must be a non zero positive integer');
    }

    $section = $output->section();
    while ($this->shouldStop === false) {
      $section->clear();
      $section->writeln(date(DateTimeInterface::RFC1123));
      $table = new Table($section);
      $table->setHeaders(
        [
          'Queue',
          'Messages',
          'Status'
        ]
      );

      $alerts = [];
      foreach ($queueList as $queueName) {
        $pending = $this->transport->pending($queueName);
        $status = $pending < $maxMessages ? 'OK' : 'ALERT';
        $table->addRow(
          [
            $queueName,
            number_format(
              $pending,
              0,
              '.',
              ' '
            ),
            $status
          ]
        );

        if ($status === 'ALERT') {
          $alerts[] = [$queueName, $pending];
        }
      }

      $table->render();

      if (count($alerts) > 0) {

      }

      sleep($interval);
    }

    return Command::SUCCESS;
  }

  public function __construct(
    ProviderInterface $provider,
    InflectorInterface $inflector,
    TransportInterface $transport
  ) {
    parent::__construct();

    $this->provider = $provider;
    $this->inflector = $inflector;
    $this->transport = $transport;
  }

  public function getSubscribedSignals(): array {
    return [SIGINT, SIGTERM];
  }

  public function handleSignal(int $signal, int|false $previousExitCode = 0): int|false {
    $this->shouldStop = true;

    return false;
  }
}
