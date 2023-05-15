<?php
declare(strict_types = 1);

namespace Courier\Console\Commands;

use Courier\Console\Traits\ResolveQueueListTrait;
use Courier\Contracts\Inflectors\InflectorInterface;
use Courier\Contracts\Providers\ProviderInterface;
use Courier\Contracts\Transports\TransportInterface;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Helper\Table;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

#[AsCommand('courier:purge', 'Purges all queued messages from one or more available queues')]
final class PurgeCommand extends Command {
  use ResolveQueueListTrait;

  private ProviderInterface $provider;
  private InflectorInterface $inflector;
  private TransportInterface $transport;

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
        'The names of the queues to purge messages from (all = purge from all queues)',
        []
      );
  }

  protected function execute(InputInterface $input, OutputInterface $output): int {
    $queueList = self::resolveQueueList(
      $this->provider,
      $this->inflector,
      $input->getOption('queue')
    );
    sort($queueList);

    if ($queueList === []) {
      $output->writeln('Select at least one queue to purge messages from');

      return Command::FAILURE;
    }

    $table = new Table($output);
    $table->setHeaders(
      [
        'Queue',
        'Messages'
      ]
    );
    foreach ($queueList as $queueName) {
      $table->addRow(
        [
          $queueName,
          number_format(
            $this->transport->pending($queueName),
            0,
            '.',
            ' '
          )
        ]
      );
      $this->transport->purge($queueName);
    }

    $table->render();

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
}
