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

#[AsCommand('courier:inspect', 'Inspect available queues, listing names and number of queued messages')]
final class InspectCommand extends Command {
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
      );
  }

  protected function execute(InputInterface $input, OutputInterface $output): int {
    $queueList = self::resolveQueueList(
      $this->provider,
      $this->inflector,
      ['all']
    );
    sort($queueList);

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
